//! SK하이닉스 본주 체결 틱으로 양방향 진입 조건을 판정하는 순수 OCO 상태 머신.
//!
//! 이 모듈은 네트워크·시계·주문 실행을 소유하지 않는다. 호출자가 단조 증가 시간과
//! 체결 순번을 주입하고, `TriggeredScenario`를 받은 뒤 주문 actor에 전달한다.

use std::time::Duration;

use crate::types::{ModelScenario, ProductKind, ScenarioStatus};

/// 기준가 바깥에서 유지해야 하는 최소 시간.
pub const CONFIRMATION_DURATION: Duration = Duration::from_secs(3);
/// 확정 틱 전에 이미 관측되어야 하는 서로 다른 체결 틱 수.
pub const REQUIRED_CONFIRMING_TICKS: u8 = 3;

pub const MIN_TARGET_RETURN_PCT: f64 = 0.2;
pub const MAX_TARGET_RETURN_PCT: f64 = 2.0;
pub const TARGET_RETURN_STEP_PCT: f64 = 0.1;
const TARGET_STEP_SCALE: f64 = 1.0 / TARGET_RETURN_STEP_PCT;
const FLOAT_EPSILON: f64 = 1e-8;

/// 모델 결정 전체가 무효인 경우.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionValidationError {
    /// 판단 기준인 본주 현재가가 유효하지 않다.
    InvalidSnapshotPrice,
    /// strict schema를 통과했더라도 프로그램 경계에서 최대 2개를 다시 보장한다.
    TooManyScenarios { count: usize },
    /// 같은 상품이 둘 이상이면 어느 조건을 선택할지 임의로 정하지 않고 전체 폐기한다.
    DuplicateProduct(ProductKind),
}

/// 개별 시나리오를 제외한 이유.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioInvalidReason {
    InvalidTriggerPrice,
    WrongDirection,
    InvalidTargetReturn,
}

/// 의미 검증을 통과한 주문 독립적 시나리오.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedScenario {
    pub product: ProductKind,
    /// 본주 호가단위로 보수적으로 정규화한 가격.
    pub trigger_price: u64,
    /// 선택 ETF의 실제 체결평단에 직접 적용할 목표수익률.
    pub target_return_pct: f64,
}

/// 전체 결정은 유효하지만 제외된 개별 시나리오.
#[derive(Debug, Clone, PartialEq)]
pub struct RejectedScenario {
    pub product: ProductKind,
    pub trigger_price: u64,
    pub target_return_pct: f64,
    pub status: ScenarioStatus,
    pub reason: ScenarioInvalidReason,
}

/// 검증 결과. `scenarios`가 비어 있으면 SKIP과 같은 의미다.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ValidatedDecision {
    pub scenarios: Vec<ValidatedScenario>,
    pub rejected: Vec<RejectedScenario>,
}

/// 모델 결정을 의미 검증하고 본주 호가단위에 맞춘다.
///
/// 중복 상품과 3개 이상 출력은 전체 오류다. 방향·목표수익률이 잘못된 개별
/// 시나리오는 제외하고, 나머지가 있으면 그대로 사용할 수 있다.
pub fn validate_decision(
    snapshot_price: u64,
    scenarios: &[ModelScenario],
) -> Result<ValidatedDecision, DecisionValidationError> {
    if snapshot_price == 0 {
        return Err(DecisionValidationError::InvalidSnapshotPrice);
    }
    if scenarios.len() > 2 {
        return Err(DecisionValidationError::TooManyScenarios {
            count: scenarios.len(),
        });
    }

    for (index, scenario) in scenarios.iter().enumerate() {
        if scenarios[..index]
            .iter()
            .any(|previous| previous.product == scenario.product)
        {
            return Err(DecisionValidationError::DuplicateProduct(scenario.product));
        }
    }

    let mut result = ValidatedDecision::default();
    for scenario in scenarios {
        let invalid_reason = if scenario.trigger_price == 0 {
            Some(ScenarioInvalidReason::InvalidTriggerPrice)
        } else if !has_correct_direction(scenario.product, snapshot_price, scenario.trigger_price) {
            Some(ScenarioInvalidReason::WrongDirection)
        } else if !is_valid_target_return(scenario.target_return_pct) {
            Some(ScenarioInvalidReason::InvalidTargetReturn)
        } else {
            None
        };

        if let Some(reason) = invalid_reason {
            result.rejected.push(rejected(scenario, reason));
            continue;
        }

        let Some(trigger_price) = normalize_trigger_price(scenario.trigger_price, scenario.product)
        else {
            result.rejected.push(rejected(
                scenario,
                ScenarioInvalidReason::InvalidTriggerPrice,
            ));
            continue;
        };

        // 검증 오차를 장부와 목표가 계산으로 전파하지 않도록 정확히 한 자리로 만든다.
        let target_return_pct =
            (scenario.target_return_pct * TARGET_STEP_SCALE).round() / TARGET_STEP_SCALE;
        result.scenarios.push(ValidatedScenario {
            product: scenario.product,
            trigger_price,
            target_return_pct,
        });
    }

    Ok(result)
}

fn rejected(scenario: &ModelScenario, reason: ScenarioInvalidReason) -> RejectedScenario {
    RejectedScenario {
        product: scenario.product,
        trigger_price: scenario.trigger_price,
        target_return_pct: scenario.target_return_pct,
        status: ScenarioStatus::Invalid,
        reason,
    }
}

fn has_correct_direction(product: ProductKind, snapshot_price: u64, trigger_price: u64) -> bool {
    match product {
        ProductKind::Leverage => trigger_price > snapshot_price,
        ProductKind::Inverse => trigger_price < snapshot_price,
    }
}

fn is_valid_target_return(target: f64) -> bool {
    if !target.is_finite() || !(MIN_TARGET_RETURN_PCT..=MAX_TARGET_RETURN_PCT).contains(&target) {
        return false;
    }
    let scaled = target * TARGET_STEP_SCALE;
    (scaled - scaled.round()).abs() <= FLOAT_EPSILON
}

/// 현재 KRX 본주 가격대별 호가단위.
pub fn underlying_tick_size(price: u64) -> u64 {
    match price {
        0..=1_999 => 1,
        2_000..=4_999 => 5,
        5_000..=19_999 => 10,
        20_000..=49_999 => 50,
        50_000..=199_999 => 100,
        200_000..=499_999 => 500,
        _ => 1_000,
    }
}

/// 상향 조건은 올림, 하향 조건은 내림하여 모델 가격보다 진입을 쉽게 만들지 않는다.
fn normalize_trigger_price(price: u64, product: ProductKind) -> Option<u64> {
    let tick = underlying_tick_size(price);
    let remainder = price % tick;
    match product {
        ProductKind::Leverage if remainder != 0 => price.checked_add(tick - remainder),
        ProductKind::Leverage => Some(price),
        ProductKind::Inverse => Some(price - remainder),
    }
}

/// 서로 다른 실시간 체결을 나타낸다.
///
/// `sequence`는 같은 연결에서 단조 증가해야 한다. 재연결 시
/// `OcoGroup::reset_for_reconnect`가 순번 기준을 초기화한다.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeTick {
    pub sequence: u64,
    pub price: u64,
    pub volume: u64,
    /// 피드 수신 직후 찍은 프로세스 내부 단조 시계 기준 시간.
    /// 큐에서 꺼낼 때 새로 찍으면 재연결 전 잔여 틱을 구별할 수 없다.
    pub at: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScenarioState {
    pub scenario: ValidatedScenario,
    pub status: ScenarioStatus,
    pub confirming_since: Option<Duration>,
    pub confirming_ticks: u8,
}

impl ScenarioState {
    fn armed(scenario: ValidatedScenario) -> Self {
        Self {
            scenario,
            status: ScenarioStatus::Armed,
            confirming_since: None,
            confirming_ticks: 0,
        }
    }

    fn is_condition_met(&self, price: u64) -> bool {
        match self.scenario.product {
            ProductKind::Leverage => price >= self.scenario.trigger_price,
            ProductKind::Inverse => price <= self.scenario.trigger_price,
        }
    }

    fn reset_confirmation(&mut self) -> bool {
        let changed = self.status == ScenarioStatus::Confirming
            || self.confirming_since.is_some()
            || self.confirming_ticks != 0;
        if matches!(
            self.status,
            ScenarioStatus::Armed | ScenarioStatus::Confirming
        ) {
            self.status = ScenarioStatus::Armed;
            self.confirming_since = None;
            self.confirming_ticks = 0;
        }
        changed
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArmError {
    NoScenarios,
    TooManyScenarios { count: usize },
    InvalidExpiry,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TriggeredScenario {
    pub group_id: i64,
    pub revision: u64,
    pub scenario: ValidatedScenario,
}

/// 틱 처리 결과. 상세 UI 상태는 `OcoGroup::scenarios`에서 읽는다.
#[derive(Debug, Clone, PartialEq)]
pub enum TickOutcome {
    Ignored,
    Updated,
    Triggered(TriggeredScenario),
    Expired,
}

/// 한 번에 최대 두 방향 중 오직 하나만 승리할 수 있는 OCO 그룹.
#[derive(Debug, Clone)]
pub struct OcoGroup {
    group_id: i64,
    revision: u64,
    expires_at: Duration,
    /// 재연결 직전에 큐에 있던 틱이 확인에 섞이지 않도록 하는 수신 장벽.
    accept_ticks_after: Duration,
    scenarios: Vec<ScenarioState>,
    last_sequence: Option<u64>,
    winner: Option<TriggeredScenario>,
}

impl OcoGroup {
    pub fn arm(
        group_id: i64,
        revision: u64,
        decision: &ValidatedDecision,
        armed_at: Duration,
        expires_at: Duration,
    ) -> Result<Self, ArmError> {
        if decision.scenarios.is_empty() {
            return Err(ArmError::NoScenarios);
        }
        if decision.scenarios.len() > 2 {
            return Err(ArmError::TooManyScenarios {
                count: decision.scenarios.len(),
            });
        }
        if expires_at <= armed_at {
            return Err(ArmError::InvalidExpiry);
        }

        Ok(Self {
            group_id,
            revision,
            expires_at,
            accept_ticks_after: armed_at,
            scenarios: decision
                .scenarios
                .iter()
                .cloned()
                .map(ScenarioState::armed)
                .collect(),
            last_sequence: None,
            winner: None,
        })
    }

    pub fn group_id(&self) -> i64 {
        self.group_id
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn scenarios(&self) -> &[ScenarioState] {
        &self.scenarios
    }

    pub fn scenario(&self, product: ProductKind) -> Option<&ScenarioState> {
        self.scenarios
            .iter()
            .find(|state| state.scenario.product == product)
    }

    pub fn winner(&self) -> Option<&TriggeredScenario> {
        self.winner.as_ref()
    }

    /// 현재 확인 경과시간. 3초를 넘어도 확정 틱 전에는 3초로 고정해 표시한다.
    pub fn confirming_elapsed(&self, product: ProductKind, now: Duration) -> Duration {
        self.scenario(product)
            .and_then(|state| state.confirming_since)
            .map(|since| now.saturating_sub(since).min(CONFIRMATION_DURATION))
            .unwrap_or_default()
    }

    /// 만료 타이머에서 호출한다. 만료 경계 시각의 틱은 진입에 사용하지 않는다.
    pub fn expire(&mut self, now: Duration) -> bool {
        if self.winner.is_some() || now < self.expires_at {
            return false;
        }

        let mut changed = false;
        for state in &mut self.scenarios {
            if matches!(
                state.status,
                ScenarioStatus::Armed | ScenarioStatus::Confirming
            ) {
                state.status = ScenarioStatus::Expired;
                state.confirming_since = None;
                state.confirming_ticks = 0;
                changed = true;
            }
        }
        changed
    }

    /// 새 LLM 결정으로 교체하기 전에 기존 그룹을 종결한다.
    pub fn replace(&mut self) -> bool {
        if self.winner.is_some() {
            return false;
        }

        let mut changed = false;
        for state in &mut self.scenarios {
            if matches!(
                state.status,
                ScenarioStatus::Armed | ScenarioStatus::Confirming
            ) {
                state.status = ScenarioStatus::Replaced;
                state.confirming_since = None;
                state.confirming_ticks = 0;
                changed = true;
            }
        }
        changed
    }

    /// WebSocket 재연결 또는 시세 공백 때 진행 중 확인과 체결 순번을 초기화한다.
    /// `reset_at` 이전에 생성된 큐 잔여 틱은 이후 도착해도 무시된다.
    pub fn reset_for_reconnect(&mut self, reset_at: Duration) -> usize {
        if self.expire(reset_at) || self.winner.is_some() {
            return 0;
        }

        self.accept_ticks_after = reset_at;
        self.last_sequence = None;
        let mut reset_count = 0;
        for state in &mut self.scenarios {
            if state.reset_confirmation() {
                reset_count += 1;
            }
        }
        reset_count
    }

    /// 실시간 본주 체결 하나를 반영한다.
    pub fn on_trade_tick(&mut self, tick: TradeTick) -> TickOutcome {
        if self.expire(tick.at) {
            return TickOutcome::Expired;
        }
        if self.winner.is_some()
            || tick.volume == 0
            || tick.price == 0
            || tick.at <= self.accept_ticks_after
            || self
                .last_sequence
                .is_some_and(|last_sequence| tick.sequence <= last_sequence)
        {
            return TickOutcome::Ignored;
        }
        self.last_sequence = Some(tick.sequence);

        let mut changed = false;
        let mut winning_index = None;

        for (index, state) in self.scenarios.iter_mut().enumerate() {
            if !matches!(
                state.status,
                ScenarioStatus::Armed | ScenarioStatus::Confirming
            ) {
                continue;
            }

            if !state.is_condition_met(tick.price) {
                changed |= state.reset_confirmation();
                continue;
            }

            match state.confirming_since {
                None => {
                    state.status = ScenarioStatus::Confirming;
                    state.confirming_since = Some(tick.at);
                    state.confirming_ticks = 1;
                    changed = true;
                }
                Some(started_at) => {
                    // 세 틱과 3초가 이미 충족된 상태에서 들어온 다음 체결만 확정한다.
                    let duration_ready =
                        tick.at.saturating_sub(started_at) >= CONFIRMATION_DURATION;
                    let ticks_ready = state.confirming_ticks >= REQUIRED_CONFIRMING_TICKS;
                    if duration_ready && ticks_ready {
                        winning_index = Some(index);
                        break;
                    }

                    state.confirming_ticks = state
                        .confirming_ticks
                        .saturating_add(1)
                        .min(REQUIRED_CONFIRMING_TICKS);
                    changed = true;
                }
            }
        }

        let Some(winning_index) = winning_index else {
            return if changed {
                TickOutcome::Updated
            } else {
                TickOutcome::Ignored
            };
        };

        let scenario = self.scenarios[winning_index].scenario.clone();
        for (index, state) in self.scenarios.iter_mut().enumerate() {
            state.confirming_since = None;
            state.confirming_ticks = 0;
            state.status = if index == winning_index {
                ScenarioStatus::Triggered
            } else {
                ScenarioStatus::CancelledByOco
            };
        }

        let triggered = TriggeredScenario {
            group_id: self.group_id,
            revision: self.revision,
            scenario,
        };
        self.winner = Some(triggered.clone());
        TickOutcome::Triggered(triggered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model(product: ProductKind, trigger_price: u64, target: f64) -> ModelScenario {
        ModelScenario {
            product,
            trigger_price,
            target_return_pct: target,
        }
    }

    fn valid_two_way_decision() -> ValidatedDecision {
        validate_decision(
            185_000,
            &[
                model(ProductKind::Leverage, 185_051, 0.3),
                model(ProductKind::Inverse, 184_949, 0.2),
            ],
        )
        .unwrap()
    }

    fn group() -> OcoGroup {
        OcoGroup::arm(
            77,
            9,
            &valid_two_way_decision(),
            Duration::from_secs(10),
            Duration::from_secs(310),
        )
        .unwrap()
    }

    fn tick(sequence: u64, second: u64, price: u64) -> TradeTick {
        TradeTick {
            sequence,
            price,
            volume: 1,
            at: Duration::from_secs(second),
        }
    }

    #[test]
    fn 빈_결정은_skip으로_검증된다() {
        let decision = validate_decision(185_000, &[]).unwrap();
        assert!(decision.scenarios.is_empty());
        assert!(decision.rejected.is_empty());
    }

    #[test]
    fn 두_방향을_본주_호가단위로_보수적으로_정규화한다() {
        let decision = valid_two_way_decision();
        assert_eq!(decision.scenarios.len(), 2);
        assert_eq!(decision.scenarios[0].trigger_price, 185_100);
        assert_eq!(decision.scenarios[1].trigger_price, 184_900);
        assert_eq!(decision.scenarios[0].target_return_pct, 0.3);
    }

    #[test]
    fn 가격대_경계에서도_정규화가_안전하다() {
        let decision = validate_decision(
            19_900,
            &[
                model(ProductKind::Leverage, 19_999, 0.2),
                model(ProductKind::Inverse, 19_899, 0.2),
            ],
        )
        .unwrap();
        assert_eq!(decision.scenarios[0].trigger_price, 20_000);
        assert_eq!(decision.scenarios[1].trigger_price, 19_890);
        assert_eq!(underlying_tick_size(20_000), 50);
    }

    #[test]
    fn 세_개_이상과_중복_상품은_결정_전체를_거부한다() {
        let too_many = vec![
            model(ProductKind::Leverage, 186_000, 0.2),
            model(ProductKind::Inverse, 184_000, 0.2),
            model(ProductKind::Leverage, 187_000, 0.3),
        ];
        assert_eq!(
            validate_decision(185_000, &too_many),
            Err(DecisionValidationError::TooManyScenarios { count: 3 })
        );

        let duplicate = vec![
            model(ProductKind::Leverage, 186_000, 0.2),
            // 개별로도 잘못됐지만 중복 규칙이 우선해 전체가 무효다.
            model(ProductKind::Leverage, 184_000, 9.9),
        ];
        assert_eq!(
            validate_decision(185_000, &duplicate),
            Err(DecisionValidationError::DuplicateProduct(
                ProductKind::Leverage
            ))
        );
    }

    #[test]
    fn 방향이나_목표가_잘못된_개별_시나리오만_제외한다() {
        let decision = validate_decision(
            185_000,
            &[
                model(ProductKind::Leverage, 184_000, 0.3),
                model(ProductKind::Inverse, 184_000, 0.25),
            ],
        )
        .unwrap();
        assert!(decision.scenarios.is_empty());
        assert_eq!(decision.rejected.len(), 2);
        assert_eq!(
            decision.rejected[0].reason,
            ScenarioInvalidReason::WrongDirection
        );
        assert_eq!(
            decision.rejected[1].reason,
            ScenarioInvalidReason::InvalidTargetReturn
        );
        assert!(decision
            .rejected
            .iter()
            .all(|scenario| scenario.status == ScenarioStatus::Invalid));
    }

    #[test]
    fn 목표수익률_경계와_한자리_단위만_허용한다() {
        for target in [0.2, 0.3, 1.9, 2.0] {
            let result =
                validate_decision(185_000, &[model(ProductKind::Leverage, 186_000, target)])
                    .unwrap();
            assert_eq!(result.scenarios.len(), 1, "target={target}");
        }
        for target in [0.1, 0.25, 2.1, f64::NAN, f64::INFINITY] {
            let result =
                validate_decision(185_000, &[model(ProductKind::Leverage, 186_000, target)])
                    .unwrap();
            assert!(result.scenarios.is_empty(), "target={target}");
        }
    }

    #[test]
    fn 스냅샷_가격이_없거나_arm_입력이_비정상이면_거부한다() {
        assert_eq!(
            validate_decision(0, &[]),
            Err(DecisionValidationError::InvalidSnapshotPrice)
        );
        let empty = validate_decision(185_000, &[]).unwrap();
        assert_eq!(
            OcoGroup::arm(1, 1, &empty, Duration::from_secs(1), Duration::from_secs(2))
                .unwrap_err(),
            ArmError::NoScenarios
        );
        assert_eq!(
            OcoGroup::arm(
                1,
                1,
                &valid_two_way_decision(),
                Duration::from_secs(2),
                Duration::from_secs(2)
            )
            .unwrap_err(),
            ArmError::InvalidExpiry
        );
    }

    #[test]
    fn arm_이전_틱과_호가성_틱은_확인에_포함하지_않는다() {
        let mut group = group();
        assert_eq!(
            group.on_trade_tick(tick(1, 9, 185_100)),
            TickOutcome::Ignored
        );
        assert_eq!(
            group.on_trade_tick(tick(2, 10, 185_100)),
            TickOutcome::Ignored
        );

        let mut no_volume = tick(3, 11, 185_100);
        no_volume.volume = 0;
        assert_eq!(group.on_trade_tick(no_volume), TickOutcome::Ignored);
        assert_eq!(
            group.scenario(ProductKind::Leverage).unwrap().status,
            ScenarioStatus::Armed
        );
    }

    #[test]
    fn 세_틱과_삼초를_충족한_뒤_다음_틱에서만_확정한다() {
        let mut group = group();
        assert_eq!(
            group.on_trade_tick(tick(1, 11, 185_100)),
            TickOutcome::Updated
        );
        assert_eq!(
            group.on_trade_tick(tick(2, 12, 185_200)),
            TickOutcome::Updated
        );
        // 이 시점에 3개째 틱과 3초를 함께 충족해도 아직 진입하지 않는다.
        assert_eq!(
            group.on_trade_tick(tick(3, 14, 185_300)),
            TickOutcome::Updated
        );
        assert!(group.winner().is_none());
        assert_eq!(
            group
                .scenario(ProductKind::Leverage)
                .unwrap()
                .confirming_ticks,
            3
        );

        let outcome = group.on_trade_tick(tick(4, 15, 185_100));
        let TickOutcome::Triggered(triggered) = outcome else {
            panic!("네 번째 유효 틱에서 확정돼야 한다");
        };
        assert_eq!(triggered.group_id, 77);
        assert_eq!(triggered.revision, 9);
        assert_eq!(triggered.scenario.product, ProductKind::Leverage);
        assert_eq!(
            group.scenario(ProductKind::Leverage).unwrap().status,
            ScenarioStatus::Triggered
        );
        assert_eq!(
            group.scenario(ProductKind::Inverse).unwrap().status,
            ScenarioStatus::CancelledByOco
        );
    }

    #[test]
    fn 틱수와_시간_중_하나라도_모자라면_확정하지_않는다() {
        let mut fast_ticks_group = group();
        assert_eq!(
            fast_ticks_group.on_trade_tick(tick(1, 11, 185_100)),
            TickOutcome::Updated
        );
        assert_eq!(
            fast_ticks_group.on_trade_tick(tick(2, 11, 185_100)),
            TickOutcome::Updated
        );
        assert_eq!(
            fast_ticks_group.on_trade_tick(tick(3, 11, 185_100)),
            TickOutcome::Updated
        );
        // 이미 3틱이지만 3초 전이다.
        assert_eq!(
            fast_ticks_group.on_trade_tick(tick(4, 13, 185_100)),
            TickOutcome::Updated
        );
        assert!(fast_ticks_group.winner().is_none());
        assert!(matches!(
            fast_ticks_group.on_trade_tick(tick(5, 14, 185_100)),
            TickOutcome::Triggered(_)
        ));

        let mut second_group = group();
        assert_eq!(
            second_group.on_trade_tick(tick(1, 11, 185_100)),
            TickOutcome::Updated
        );
        assert_eq!(
            second_group.on_trade_tick(tick(2, 20, 185_100)),
            TickOutcome::Updated
        );
        // 3초는 넘었지만 확정 전에 누적된 틱은 아직 2개다.
        assert_eq!(
            second_group.on_trade_tick(tick(3, 21, 185_100)),
            TickOutcome::Updated
        );
        // 앞선 세 틱과 3초가 모두 갖춰진 뒤 들어온 네 번째 틱에서 확정한다.
        assert!(matches!(
            second_group.on_trade_tick(tick(4, 22, 185_100)),
            TickOutcome::Triggered(_)
        ));
    }

    #[test]
    fn 기준_안쪽으로_되돌아오면_시간과_틱수를_초기화한다() {
        let mut group = group();
        group.on_trade_tick(tick(1, 11, 185_100));
        group.on_trade_tick(tick(2, 12, 185_200));
        assert_eq!(
            group.on_trade_tick(tick(3, 13, 185_099)),
            TickOutcome::Updated
        );

        let state = group.scenario(ProductKind::Leverage).unwrap();
        assert_eq!(state.status, ScenarioStatus::Armed);
        assert_eq!(state.confirming_ticks, 0);
        assert_eq!(state.confirming_since, None);

        // 이전 확인 시간이 길었어도 새 구간에서 다시 세어야 한다.
        group.on_trade_tick(tick(4, 20, 185_100));
        group.on_trade_tick(tick(5, 21, 185_100));
        group.on_trade_tick(tick(6, 22, 185_100));
        assert_eq!(
            group.on_trade_tick(tick(7, 22, 185_100)),
            TickOutcome::Updated
        );
        assert!(group.winner().is_none());
    }

    #[test]
    fn 재연결은_확인을_초기화하고_큐에_남은_옛_틱을_무시한다() {
        let mut group = group();
        group.on_trade_tick(tick(10, 11, 185_100));
        group.on_trade_tick(tick(11, 12, 185_100));

        assert_eq!(group.reset_for_reconnect(Duration::from_secs(13)), 1);
        let state = group.scenario(ProductKind::Leverage).unwrap();
        assert_eq!(state.status, ScenarioStatus::Armed);
        assert_eq!(state.confirming_ticks, 0);

        // 순번 기준은 재설정됐지만 reset_at 이하의 큐 잔여 틱은 받지 않는다.
        assert_eq!(
            group.on_trade_tick(tick(1, 13, 185_100)),
            TickOutcome::Ignored
        );
        assert_eq!(
            group.on_trade_tick(tick(1, 14, 185_100)),
            TickOutcome::Updated
        );
        assert_eq!(
            group
                .scenario(ProductKind::Leverage)
                .unwrap()
                .confirming_ticks,
            1
        );
    }

    #[test]
    fn 중복되거나_역행한_체결_순번은_한번만_센다() {
        let mut group = group();
        group.on_trade_tick(tick(5, 11, 185_100));
        assert_eq!(
            group.on_trade_tick(tick(5, 12, 185_100)),
            TickOutcome::Ignored
        );
        assert_eq!(
            group.on_trade_tick(tick(4, 13, 185_100)),
            TickOutcome::Ignored
        );
        assert_eq!(
            group
                .scenario(ProductKind::Leverage)
                .unwrap()
                .confirming_ticks,
            1
        );
    }

    #[test]
    fn 곱버스가_먼저_확정되면_레버리지는_oco_취소된다() {
        let mut group = group();
        group.on_trade_tick(tick(1, 11, 184_900));
        group.on_trade_tick(tick(2, 12, 184_800));
        group.on_trade_tick(tick(3, 13, 184_700));
        let outcome = group.on_trade_tick(tick(4, 14, 184_900));
        assert!(matches!(
            outcome,
            TickOutcome::Triggered(TriggeredScenario {
                scenario: ValidatedScenario {
                    product: ProductKind::Inverse,
                    ..
                },
                ..
            })
        ));
        assert_eq!(
            group.scenario(ProductKind::Leverage).unwrap().status,
            ScenarioStatus::CancelledByOco
        );

        // 승자 확정 뒤 반대 방향 틱이 와도 두 번째 승자는 생기지 않는다.
        assert_eq!(
            group.on_trade_tick(tick(5, 20, 185_100)),
            TickOutcome::Ignored
        );
        assert_eq!(
            group.winner().unwrap().scenario.product,
            ProductKind::Inverse
        );
    }

    #[test]
    fn 교체는_모든_진행상태를_replaced로_종결한다() {
        let mut group = group();
        group.on_trade_tick(tick(1, 11, 185_100));
        assert!(group.replace());
        assert!(group.scenarios().iter().all(|state| {
            state.status == ScenarioStatus::Replaced
                && state.confirming_since.is_none()
                && state.confirming_ticks == 0
        }));
        assert_eq!(
            group.on_trade_tick(tick(2, 12, 185_100)),
            TickOutcome::Ignored
        );
    }

    #[test]
    fn 만료_경계의_틱은_진입시키지_않는다() {
        let mut group = group();
        group.on_trade_tick(tick(1, 300, 185_100));
        group.on_trade_tick(tick(2, 306, 185_100));
        group.on_trade_tick(tick(3, 307, 185_100));

        assert_eq!(
            group.on_trade_tick(tick(4, 310, 185_100)),
            TickOutcome::Expired
        );
        assert!(group
            .scenarios()
            .iter()
            .all(|state| state.status == ScenarioStatus::Expired));
        assert!(group.winner().is_none());
        assert!(!group.expire(Duration::from_secs(311)));
    }

    #[test]
    fn 확인_표시시간은_삼초에서_고정한다() {
        let mut group = group();
        group.on_trade_tick(tick(1, 11, 185_100));
        assert_eq!(
            group.confirming_elapsed(ProductKind::Leverage, Duration::from_millis(12_500)),
            Duration::from_millis(1_500)
        );
        assert_eq!(
            group.confirming_elapsed(ProductKind::Leverage, Duration::from_secs(99)),
            CONFIRMATION_DURATION
        );
    }
}
