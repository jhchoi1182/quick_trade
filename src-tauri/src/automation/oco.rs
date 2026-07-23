//! SK하이닉스 본주 체결 틱으로 추세·반전 진입 조건을 판정하는 순수 OCO 상태 머신.
//!
//! 이 모듈은 네트워크·시계·주문 실행을 소유하지 않는다. 호출자가 단조 증가 시간과
//! 체결 순번을 주입하고, `TriggeredScenario`를 받은 뒤 주문 actor에 전달한다.

use std::time::Duration;

use crate::types::{
    MarketRegime, ModelDecision, ModelScenario, ProductKind, ScenarioStatus, SetupType,
};

/// 확인가 바깥에서 유지해야 하는 최소 시간.
pub const CONFIRMATION_DURATION: Duration = Duration::from_secs(3);
/// 확정 틱 전에 이미 관측되어야 하는 서로 다른 체결 틱 수.
pub const REQUIRED_CONFIRMING_TICKS: u8 = 3;

pub const MIN_TARGET_RETURN_PCT: f64 = 0.2;
pub const MAX_TARGET_RETURN_PCT: f64 = 2.0;
pub const TARGET_RETURN_STEP_PCT: f64 = 0.1;
/// 기준가와 확인가 사이에 모델이 확보해야 하는 최소 거리(10bp = 0.1%).
pub const MIN_CONFIRMATION_DISTANCE_BPS: u64 = 10;
const BASIS_POINTS_DENOMINATOR: u64 = 10_000;
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
    InvalidPrice,
    WrongPriceOrder,
    ConfirmationTooClose,
    InvalidTargetReturn,
}

/// 의미 검증을 통과한 주문 독립적 시나리오.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedScenario {
    pub product: ProductKind,
    pub setup_type: SetupType,
    /// 실행 시 기준선 시험에 사용하는 보수적 호가 정규화 가격.
    pub reference_price: u64,
    /// 모델이 제시한 확인가. 장부에서 모델 출력과 실제 발동가를 구분할 때 사용한다.
    pub confirmation_price: u64,
    /// 모델 확인가를 진입 방향으로 보수적으로 호가 정규화한 실제 발동가.
    /// 0.1% 같은 추가 실행기 버퍼는 더하지 않는다.
    pub trigger_price: u64,
    /// 무효화를 더 일찍 감지하는 방향으로 호가 정규화한 가격.
    pub invalidation_price: u64,
    /// 선택 ETF의 실제 체결평단에 직접 적용할 목표수익률.
    pub target_return_pct: f64,
    pub rationale_ko: String,
}

impl ValidatedScenario {
    pub fn confirmation_reached(&self, price: u64) -> bool {
        match self.product {
            ProductKind::Leverage => price >= self.trigger_price,
            ProductKind::Inverse => price <= self.trigger_price,
        }
    }

    pub fn reference_reached(&self, price: u64) -> bool {
        match (self.product, self.setup_type) {
            (ProductKind::Leverage, SetupType::Continuation)
            | (ProductKind::Inverse, SetupType::Reversal) => price >= self.reference_price,
            (ProductKind::Inverse, SetupType::Continuation)
            | (ProductKind::Leverage, SetupType::Reversal) => price <= self.reference_price,
        }
    }

    pub fn invalidation_reached(&self, price: u64) -> bool {
        match self.product {
            ProductKind::Leverage => price <= self.invalidation_price,
            ProductKind::Inverse => price >= self.invalidation_price,
        }
    }
}

/// 전체 결정은 유효하지만 제외된 개별 시나리오.
#[derive(Debug, Clone, PartialEq)]
pub struct RejectedScenario {
    pub product: ProductKind,
    pub setup_type: SetupType,
    pub reference_price: u64,
    pub confirmation_price: u64,
    pub trigger_price: u64,
    pub invalidation_price: u64,
    pub target_return_pct: f64,
    pub rationale_ko: String,
    pub status: ScenarioStatus,
    pub reason: ScenarioInvalidReason,
}

/// 검증 결과. `scenarios`가 비고 `rejected`도 비면 SKIP, `rejected`가 있으면
/// 전체 후보가 의미 검증에 실패한 INVALID다.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedDecision {
    pub market_regime: MarketRegime,
    pub decision_summary_ko: String,
    pub scenarios: Vec<ValidatedScenario>,
    pub rejected: Vec<RejectedScenario>,
}

/// 모델 결정을 의미 검증하고 본주 호가단위에 맞춘다.
///
/// 중복 상품과 3개 이상 출력은 전체 오류다. 가격 순서·확인가 거리·목표수익률이
/// 잘못된 개별 시나리오는 제외한다. 확인가에는 모델이 이미 선택한 진입 여유 외에
/// 실행기가 별도 비율 버퍼를 추가하지 않는다.
pub fn validate_decision(
    snapshot_price: u64,
    decision: &ModelDecision,
) -> Result<ValidatedDecision, DecisionValidationError> {
    if snapshot_price == 0 {
        return Err(DecisionValidationError::InvalidSnapshotPrice);
    }
    if decision.scenarios.len() > 2 {
        return Err(DecisionValidationError::TooManyScenarios {
            count: decision.scenarios.len(),
        });
    }

    for (index, scenario) in decision.scenarios.iter().enumerate() {
        if decision.scenarios[..index]
            .iter()
            .any(|previous| previous.product == scenario.product)
        {
            return Err(DecisionValidationError::DuplicateProduct(scenario.product));
        }
    }

    let mut result = ValidatedDecision {
        market_regime: decision.market_regime,
        decision_summary_ko: decision.decision_summary_ko.clone(),
        scenarios: Vec::new(),
        rejected: Vec::new(),
    };
    for scenario in &decision.scenarios {
        let invalid_reason = validate_raw_scenario(snapshot_price, scenario);
        if let Some(reason) = invalid_reason {
            result.rejected.push(rejected(scenario, reason));
            continue;
        }

        let Some(reference_price) = normalize_reference_price(scenario) else {
            result
                .rejected
                .push(rejected(scenario, ScenarioInvalidReason::InvalidPrice));
            continue;
        };
        let Some(trigger_price) = normalize_activation_price(
            scenario.confirmation_price,
            entry_direction(scenario.product),
        ) else {
            result
                .rejected
                .push(rejected(scenario, ScenarioInvalidReason::InvalidPrice));
            continue;
        };
        let Some(invalidation_price) = normalize_invalidation_price(scenario) else {
            result
                .rejected
                .push(rejected(scenario, ScenarioInvalidReason::InvalidPrice));
            continue;
        };

        if !has_correct_price_order(
            scenario.product,
            scenario.setup_type,
            snapshot_price,
            reference_price,
            trigger_price,
            invalidation_price,
        ) {
            result
                .rejected
                .push(rejected(scenario, ScenarioInvalidReason::WrongPriceOrder));
            continue;
        }
        if !has_minimum_confirmation_distance(scenario.product, reference_price, trigger_price) {
            result.rejected.push(rejected(
                scenario,
                ScenarioInvalidReason::ConfirmationTooClose,
            ));
            continue;
        }

        // 검증 오차를 장부와 목표가 계산으로 전파하지 않도록 정확히 한 자리로 만든다.
        let target_return_pct =
            (scenario.target_return_pct * TARGET_STEP_SCALE).round() / TARGET_STEP_SCALE;
        result.scenarios.push(ValidatedScenario {
            product: scenario.product,
            setup_type: scenario.setup_type,
            reference_price,
            confirmation_price: scenario.confirmation_price,
            trigger_price,
            invalidation_price,
            target_return_pct,
            rationale_ko: scenario.rationale_ko.clone(),
        });
    }

    Ok(result)
}

fn validate_raw_scenario(
    snapshot_price: u64,
    scenario: &ModelScenario,
) -> Option<ScenarioInvalidReason> {
    if scenario.reference_price == 0
        || scenario.confirmation_price == 0
        || scenario.invalidation_price == 0
    {
        return Some(ScenarioInvalidReason::InvalidPrice);
    }
    if !has_correct_price_order(
        scenario.product,
        scenario.setup_type,
        snapshot_price,
        scenario.reference_price,
        scenario.confirmation_price,
        scenario.invalidation_price,
    ) {
        return Some(ScenarioInvalidReason::WrongPriceOrder);
    }
    if !has_minimum_confirmation_distance(
        scenario.product,
        scenario.reference_price,
        scenario.confirmation_price,
    ) {
        return Some(ScenarioInvalidReason::ConfirmationTooClose);
    }
    if !is_valid_target_return(scenario.target_return_pct) {
        return Some(ScenarioInvalidReason::InvalidTargetReturn);
    }
    None
}

fn rejected(scenario: &ModelScenario, reason: ScenarioInvalidReason) -> RejectedScenario {
    let trigger_price = normalize_activation_price(
        scenario.confirmation_price,
        entry_direction(scenario.product),
    )
    .unwrap_or(scenario.confirmation_price);
    RejectedScenario {
        product: scenario.product,
        setup_type: scenario.setup_type,
        reference_price: scenario.reference_price,
        confirmation_price: scenario.confirmation_price,
        trigger_price,
        invalidation_price: scenario.invalidation_price,
        target_return_pct: scenario.target_return_pct,
        rationale_ko: scenario.rationale_ko.clone(),
        status: ScenarioStatus::Invalid,
        reason,
    }
}

fn has_correct_price_order(
    product: ProductKind,
    setup_type: SetupType,
    snapshot_price: u64,
    reference_price: u64,
    confirmation_price: u64,
    invalidation_price: u64,
) -> bool {
    match (product, setup_type) {
        (ProductKind::Leverage, SetupType::Continuation) => {
            invalidation_price < snapshot_price
                && snapshot_price < reference_price
                && reference_price < confirmation_price
        }
        (ProductKind::Inverse, SetupType::Continuation) => {
            confirmation_price < reference_price
                && reference_price < snapshot_price
                && snapshot_price < invalidation_price
        }
        (ProductKind::Leverage, SetupType::Reversal) => {
            invalidation_price < reference_price
                && reference_price < snapshot_price
                && snapshot_price < confirmation_price
        }
        (ProductKind::Inverse, SetupType::Reversal) => {
            confirmation_price < snapshot_price
                && snapshot_price < reference_price
                && reference_price < invalidation_price
        }
    }
}

fn has_minimum_confirmation_distance(
    product: ProductKind,
    reference_price: u64,
    confirmation_price: u64,
) -> bool {
    let distance = match product {
        ProductKind::Leverage => confirmation_price.checked_sub(reference_price),
        ProductKind::Inverse => reference_price.checked_sub(confirmation_price),
    };
    distance.is_some_and(|distance| {
        u128::from(distance) * u128::from(BASIS_POINTS_DENOMINATOR)
            >= u128::from(reference_price) * u128::from(MIN_CONFIRMATION_DISTANCE_BPS)
    })
}

fn is_valid_target_return(target: f64) -> bool {
    if !target.is_finite() || !(MIN_TARGET_RETURN_PCT..=MAX_TARGET_RETURN_PCT).contains(&target) {
        return false;
    }
    let scaled = target * TARGET_STEP_SCALE;
    (scaled - scaled.round()).abs() <= FLOAT_EPSILON
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PriceDirection {
    AtOrAbove,
    AtOrBelow,
}

fn entry_direction(product: ProductKind) -> PriceDirection {
    match product {
        ProductKind::Leverage => PriceDirection::AtOrAbove,
        ProductKind::Inverse => PriceDirection::AtOrBelow,
    }
}

fn reference_direction(scenario: &ModelScenario) -> PriceDirection {
    match scenario.setup_type {
        SetupType::Continuation => entry_direction(scenario.product),
        SetupType::Reversal => match scenario.product {
            ProductKind::Leverage => PriceDirection::AtOrBelow,
            ProductKind::Inverse => PriceDirection::AtOrAbove,
        },
    }
}

fn normalize_reference_price(scenario: &ModelScenario) -> Option<u64> {
    normalize_activation_price(scenario.reference_price, reference_direction(scenario))
}

/// 무효화는 더 일찍 안전 취소되도록 레버리지 하단은 올림, 곱버스 상단은 내림한다.
fn normalize_invalidation_price(scenario: &ModelScenario) -> Option<u64> {
    match scenario.product {
        ProductKind::Leverage => ceil_to_tick(scenario.invalidation_price),
        ProductKind::Inverse => floor_to_tick(scenario.invalidation_price),
    }
}

/// 상향 활성화는 올림, 하향 활성화는 내림하여 모델 가격보다 활성화를 쉽게 만들지 않는다.
fn normalize_activation_price(price: u64, direction: PriceDirection) -> Option<u64> {
    match direction {
        PriceDirection::AtOrAbove => ceil_to_tick(price),
        PriceDirection::AtOrBelow => floor_to_tick(price),
    }
}

fn ceil_to_tick(price: u64) -> Option<u64> {
    if price == 0 {
        return None;
    }
    let tick = underlying_tick_size(price);
    let remainder = price % tick;
    if remainder == 0 {
        Some(price)
    } else {
        price.checked_add(tick - remainder)
    }
}

fn floor_to_tick(price: u64) -> Option<u64> {
    if price == 0 {
        return None;
    }
    let tick = underlying_tick_size(price);
    let normalized = price - price % tick;
    (normalized > 0).then_some(normalized)
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

/// 응답 적용 시 최신 시세·최근 봉으로 판정한 시나리오 초기 상태.
///
/// 같은 상품의 seed가 여러 개면 `Invalidated > Missed > Armed` 순으로 합친다.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScenarioSeed {
    pub product: ProductKind,
    pub status: ScenarioStatus,
    /// 반전 기준선이 최근 봉에서 이미 시험된 시각. continuation은 사용하지 않는다.
    pub reference_observed_at: Option<i64>,
}

impl ScenarioSeed {
    pub fn armed(product: ProductKind, reference_observed_at: Option<i64>) -> Self {
        Self {
            product,
            status: ScenarioStatus::Armed,
            reference_observed_at,
        }
    }

    pub fn missed(product: ProductKind, reference_observed_at: Option<i64>) -> Self {
        Self {
            product,
            status: ScenarioStatus::Missed,
            reference_observed_at,
        }
    }

    pub fn invalidated(product: ProductKind, reference_observed_at: Option<i64>) -> Self {
        Self {
            product,
            status: ScenarioStatus::Invalidated,
            reference_observed_at,
        }
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
    /// `Candle.time`과 같은 KST 가짜 epoch. 반전 기준선 관측 시각 표시에 사용한다.
    pub epoch: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScenarioState {
    pub scenario: ValidatedScenario,
    pub status: ScenarioStatus,
    pub reference_observed: bool,
    pub reference_observed_at: Option<i64>,
    pub confirming_since: Option<Duration>,
    pub confirming_ticks: u8,
}

impl ScenarioState {
    fn seeded(scenario: ValidatedScenario, seed: ScenarioSeed) -> Self {
        let terminal = matches!(
            seed.status,
            ScenarioStatus::Missed | ScenarioStatus::Invalidated
        );
        let reference_observed_at = (scenario.setup_type == SetupType::Reversal)
            .then_some(seed.reference_observed_at)
            .flatten();
        Self {
            scenario,
            status: if terminal {
                seed.status
            } else {
                ScenarioStatus::Armed
            },
            reference_observed: reference_observed_at.is_some(),
            reference_observed_at,
            confirming_since: None,
            confirming_ticks: 0,
        }
    }

    fn is_active(&self) -> bool {
        matches!(
            self.status,
            ScenarioStatus::Armed | ScenarioStatus::Confirming
        )
    }

    fn is_confirmation_met(&self, price: u64) -> bool {
        self.scenario.confirmation_reached(price)
    }

    fn is_reference_met(&self, price: u64) -> bool {
        self.scenario.reference_reached(price)
    }

    fn is_invalidated(&self, price: u64) -> bool {
        self.scenario.invalidation_reached(price)
    }

    fn reset_confirmation(&mut self) -> bool {
        let changed = self.status == ScenarioStatus::Confirming
            || self.confirming_since.is_some()
            || self.confirming_ticks != 0;
        if self.is_active() {
            self.status = ScenarioStatus::Armed;
            self.confirming_since = None;
            self.confirming_ticks = 0;
        }
        changed
    }

    fn invalidate(&mut self) {
        self.status = ScenarioStatus::Invalidated;
        self.confirming_since = None;
        self.confirming_ticks = 0;
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
    /// 최초 무장은 피드 FIFO 배리어와 마지막 체결 순번으로 경계를 세우므로 0이다.
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
        seeds: &[ScenarioSeed],
        last_trade_sequence: Option<u64>,
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
            accept_ticks_after: Duration::ZERO,
            scenarios: decision
                .scenarios
                .iter()
                .cloned()
                .map(|scenario| {
                    let seed = merged_seed(scenario.product, seeds);
                    ScenarioState::seeded(scenario, seed)
                })
                .collect(),
            last_sequence: last_trade_sequence,
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

    pub fn has_active_scenarios(&self) -> bool {
        self.scenarios.iter().any(ScenarioState::is_active)
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
            if state.is_active() {
                state.status = ScenarioStatus::Expired;
                state.confirming_since = None;
                state.confirming_ticks = 0;
                changed = true;
            }
        }
        changed
    }

    /// 새 LLM 결정으로 교체하기 전에 기존 그룹의 활성 시나리오만 종결한다.
    pub fn replace(&mut self) -> bool {
        if self.winner.is_some() {
            return false;
        }

        let mut changed = false;
        for state in &mut self.scenarios {
            if state.is_active() {
                state.status = ScenarioStatus::Replaced;
                state.confirming_since = None;
                state.confirming_ticks = 0;
                changed = true;
            }
        }
        changed
    }

    /// WebSocket 재연결 또는 시세 공백 때 진행 중 확인과 체결 순번을 초기화한다.
    /// 반전은 과거 seed를 포함한 기준선 활성화도 버리고 새 연결의 시험을 요구한다.
    /// `reset_at` 이전에 생성된 큐 잔여 틱은 이후 도착해도 무시된다.
    pub fn reset_for_reconnect(&mut self, reset_at: Duration) -> usize {
        let active_before_expiry = self
            .scenarios
            .iter()
            .filter(|state| state.is_active())
            .count();
        if self.expire(reset_at) {
            return active_before_expiry;
        }
        if self.winner.is_some() {
            return 0;
        }

        self.accept_ticks_after = reset_at;
        self.last_sequence = None;
        let mut reset_count = 0;
        for state in &mut self.scenarios {
            if !state.is_active() {
                continue;
            }
            let mut changed = state.reset_confirmation();
            if state.scenario.setup_type == SetupType::Reversal && state.reference_observed {
                state.reference_observed = false;
                state.reference_observed_at = None;
                changed = true;
            }
            if changed {
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

        // 같은 틱에서 한쪽이 확정되고 상대 무효화가도 침범할 수 있으므로 배열
        // 순서와 무관하게 모든 무효화를 confirmation보다 먼저 종결한다.
        for state in &mut self.scenarios {
            if state.is_active() && state.is_invalidated(tick.price) {
                state.invalidate();
                changed = true;
            }
        }

        for (index, state) in self.scenarios.iter_mut().enumerate() {
            if !state.is_active() {
                continue;
            }

            if state.scenario.setup_type == SetupType::Reversal && !state.reference_observed {
                if state.is_reference_met(tick.price) {
                    state.reference_observed = true;
                    state.reference_observed_at = Some(tick.epoch);
                    changed = true;
                }
                // 기준선 시험과 확인을 같은 틱으로 합치지 않는다.
                continue;
            }

            if !state.is_confirmation_met(tick.price) {
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
            if index == winning_index {
                state.status = ScenarioStatus::Triggered;
                state.confirming_since = None;
                state.confirming_ticks = 0;
            } else if state.is_active() {
                // 이미 missed/invalidated로 종결된 상대 시나리오는 이력을 보존한다.
                state.status = ScenarioStatus::CancelledByOco;
                state.confirming_since = None;
                state.confirming_ticks = 0;
            }
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

fn merged_seed(product: ProductKind, seeds: &[ScenarioSeed]) -> ScenarioSeed {
    let mut merged = ScenarioSeed::armed(product, None);
    for seed in seeds.iter().filter(|seed| seed.product == product) {
        if seed.reference_observed_at > merged.reference_observed_at {
            merged.reference_observed_at = seed.reference_observed_at;
        }
        if seed_priority(seed.status) > seed_priority(merged.status) {
            merged.status = seed.status;
        }
    }
    merged
}

fn seed_priority(status: ScenarioStatus) -> u8 {
    match status {
        ScenarioStatus::Invalidated => 3,
        ScenarioStatus::Missed => 2,
        ScenarioStatus::Armed => 1,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SNAPSHOT: u64 = 185_000;

    fn model(
        product: ProductKind,
        setup_type: SetupType,
        reference_price: u64,
        confirmation_price: u64,
        invalidation_price: u64,
        target: f64,
    ) -> ModelScenario {
        ModelScenario {
            product,
            setup_type,
            reference_price,
            confirmation_price,
            invalidation_price,
            target_return_pct: target,
            rationale_ko: "가격·거래량 근거".into(),
        }
    }

    fn decision(scenarios: Vec<ModelScenario>) -> ModelDecision {
        ModelDecision {
            market_regime: MarketRegime::Range,
            decision_summary_ko: "시험용 판단".into(),
            scenarios,
        }
    }

    fn continuation_two_way() -> ValidatedDecision {
        validate_decision(
            SNAPSHOT,
            &decision(vec![
                model(
                    ProductKind::Leverage,
                    SetupType::Continuation,
                    185_100,
                    185_300,
                    184_500,
                    0.3,
                ),
                model(
                    ProductKind::Inverse,
                    SetupType::Continuation,
                    184_900,
                    184_700,
                    185_600,
                    0.2,
                ),
            ]),
        )
        .unwrap()
    }

    fn reversal_two_way() -> ValidatedDecision {
        validate_decision(
            SNAPSHOT,
            &decision(vec![
                model(
                    ProductKind::Leverage,
                    SetupType::Reversal,
                    184_800,
                    185_200,
                    184_600,
                    0.3,
                ),
                model(
                    ProductKind::Inverse,
                    SetupType::Reversal,
                    185_200,
                    184_800,
                    185_400,
                    0.2,
                ),
            ]),
        )
        .unwrap()
    }

    fn group(decision: &ValidatedDecision, seeds: &[ScenarioSeed]) -> OcoGroup {
        OcoGroup::arm(
            77,
            9,
            decision,
            seeds,
            None,
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
            epoch: 1_700_000_000 + second as i64,
        }
    }

    fn confirm(group: &mut OcoGroup, price: u64, start_sequence: u64, start_second: u64) {
        assert_eq!(
            group.on_trade_tick(tick(start_sequence, start_second, price)),
            TickOutcome::Updated
        );
        assert_eq!(
            group.on_trade_tick(tick(start_sequence + 1, start_second + 1, price)),
            TickOutcome::Updated
        );
        assert_eq!(
            group.on_trade_tick(tick(start_sequence + 2, start_second + 3, price)),
            TickOutcome::Updated
        );
    }

    #[test]
    fn 빈_결정은_국면과_요약을_보존한_skip으로_검증된다() {
        let decision = decision(vec![]);
        let validated = validate_decision(SNAPSHOT, &decision).unwrap();
        assert!(validated.scenarios.is_empty());
        assert!(validated.rejected.is_empty());
        assert_eq!(validated.market_regime, MarketRegime::Range);
        assert_eq!(validated.decision_summary_ko, "시험용 판단");
    }

    #[test]
    fn 네_셋업의_s_r_c_i_순서를_허용한다() {
        let scenarios = vec![
            model(
                ProductKind::Leverage,
                SetupType::Continuation,
                185_100,
                185_300,
                184_800,
                0.3,
            ),
            model(
                ProductKind::Inverse,
                SetupType::Continuation,
                184_900,
                184_700,
                185_200,
                0.2,
            ),
            model(
                ProductKind::Leverage,
                SetupType::Reversal,
                184_800,
                185_200,
                184_600,
                0.3,
            ),
            model(
                ProductKind::Inverse,
                SetupType::Reversal,
                185_200,
                184_800,
                185_400,
                0.2,
            ),
        ];
        for scenario in scenarios {
            let validated = validate_decision(SNAPSHOT, &decision(vec![scenario])).unwrap();
            assert_eq!(validated.scenarios.len(), 1);
        }
    }

    #[test]
    fn 잘못된_가격순서와_10bp미만_확인가는_개별_제외한다() {
        let wrong = validate_decision(
            SNAPSHOT,
            &decision(vec![model(
                ProductKind::Leverage,
                SetupType::Reversal,
                185_100,
                185_300,
                184_800,
                0.3,
            )]),
        )
        .unwrap();
        assert_eq!(
            wrong.rejected[0].reason,
            ScenarioInvalidReason::WrongPriceOrder
        );

        let too_close = validate_decision(
            SNAPSHOT,
            &decision(vec![model(
                ProductKind::Leverage,
                SetupType::Continuation,
                185_100,
                185_200,
                184_800,
                0.3,
            )]),
        )
        .unwrap();
        assert_eq!(
            too_close.rejected[0].reason,
            ScenarioInvalidReason::ConfirmationTooClose
        );

        // 원가격은 10bp를 넘지만 두 가격을 각각 호가 정규화하면 100원 차이로
        // 줄어드는 경우도 실행 경계에서 다시 제외한다.
        let normalized_too_close = validate_decision(
            SNAPSHOT,
            &decision(vec![model(
                ProductKind::Leverage,
                SetupType::Continuation,
                185_001,
                185_187,
                184_800,
                0.3,
            )]),
        )
        .unwrap();
        assert_eq!(
            normalized_too_close.rejected[0].reason,
            ScenarioInvalidReason::ConfirmationTooClose
        );
    }

    #[test]
    fn 확인가에는_추가버퍼없이_진입방향_호가정규화만_적용한다() {
        let validated = validate_decision(
            SNAPSHOT,
            &decision(vec![
                model(
                    ProductKind::Leverage,
                    SetupType::Continuation,
                    185_051,
                    185_251,
                    184_849,
                    0.3,
                ),
                model(
                    ProductKind::Inverse,
                    SetupType::Continuation,
                    184_949,
                    184_749,
                    185_251,
                    0.2,
                ),
            ]),
        )
        .unwrap();
        let leverage = &validated.scenarios[0];
        assert_eq!(leverage.reference_price, 185_100);
        assert_eq!(leverage.confirmation_price, 185_251);
        assert_eq!(leverage.trigger_price, 185_300);
        assert_eq!(leverage.invalidation_price, 184_900);
        let inverse = &validated.scenarios[1];
        assert_eq!(inverse.reference_price, 184_900);
        assert_eq!(inverse.trigger_price, 184_700);
        assert_eq!(inverse.invalidation_price, 185_200);
    }

    #[test]
    fn 세개_이상과_중복상품은_결정전체를_거부한다() {
        let base = model(
            ProductKind::Leverage,
            SetupType::Continuation,
            185_100,
            185_300,
            184_800,
            0.3,
        );
        let too_many = decision(vec![
            base.clone(),
            model(
                ProductKind::Inverse,
                SetupType::Continuation,
                184_900,
                184_700,
                185_200,
                0.2,
            ),
            base.clone(),
        ]);
        assert_eq!(
            validate_decision(SNAPSHOT, &too_many),
            Err(DecisionValidationError::TooManyScenarios { count: 3 })
        );

        let duplicate = decision(vec![base.clone(), base]);
        assert_eq!(
            validate_decision(SNAPSHOT, &duplicate),
            Err(DecisionValidationError::DuplicateProduct(
                ProductKind::Leverage
            ))
        );
    }

    #[test]
    fn 목표수익률_경계와_한자리_단위만_허용한다() {
        for target in [0.2, 0.3, 1.9, 2.0] {
            let result = validate_decision(
                SNAPSHOT,
                &decision(vec![model(
                    ProductKind::Leverage,
                    SetupType::Continuation,
                    185_100,
                    185_300,
                    184_800,
                    target,
                )]),
            )
            .unwrap();
            assert_eq!(result.scenarios.len(), 1, "target={target}");
        }
        for target in [0.1, 0.25, 2.1, f64::NAN, f64::INFINITY] {
            let result = validate_decision(
                SNAPSHOT,
                &decision(vec![model(
                    ProductKind::Leverage,
                    SetupType::Continuation,
                    185_100,
                    185_300,
                    184_800,
                    target,
                )]),
            )
            .unwrap();
            assert!(result.scenarios.is_empty(), "target={target}");
        }
    }

    #[test]
    fn continuation은_즉시_확인가를_감시하고_다음틱에서_확정한다() {
        let decision = continuation_two_way();
        let up = decision.scenarios[0].trigger_price;
        let mut group = group(&decision, &[]);
        confirm(&mut group, up, 1, 11);
        assert!(group.winner().is_none());
        let outcome = group.on_trade_tick(tick(4, 15, up));
        assert!(matches!(
            outcome,
            TickOutcome::Triggered(TriggeredScenario {
                scenario: ValidatedScenario {
                    product: ProductKind::Leverage,
                    setup_type: SetupType::Continuation,
                    ..
                },
                ..
            })
        ));
        assert_eq!(
            group.scenario(ProductKind::Inverse).unwrap().status,
            ScenarioStatus::CancelledByOco
        );
    }

    #[test]
    fn winner틱이_상대_무효화가도_침범하면_terminal_무효화를_보존한다() {
        let decision = continuation_two_way();
        let leverage_trigger = decision.scenarios[0].trigger_price;
        let inverse_invalidation = decision.scenarios[1].invalidation_price;
        let mut group = group(&decision, &[]);
        confirm(&mut group, leverage_trigger, 1, 11);

        assert!(matches!(
            group.on_trade_tick(tick(4, 15, inverse_invalidation)),
            TickOutcome::Triggered(_)
        ));
        assert_eq!(
            group.scenario(ProductKind::Inverse).unwrap().status,
            ScenarioStatus::Invalidated
        );
    }

    #[test]
    fn 네_셋업은_각각_삼초_세틱_뒤_다음틱에서_발동한다() {
        let scenarios = [
            model(
                ProductKind::Leverage,
                SetupType::Continuation,
                185_100,
                185_300,
                184_800,
                0.3,
            ),
            model(
                ProductKind::Inverse,
                SetupType::Continuation,
                184_900,
                184_700,
                185_200,
                0.2,
            ),
            model(
                ProductKind::Leverage,
                SetupType::Reversal,
                184_800,
                185_200,
                184_600,
                0.3,
            ),
            model(
                ProductKind::Inverse,
                SetupType::Reversal,
                185_200,
                184_800,
                185_400,
                0.2,
            ),
        ];

        for scenario in scenarios {
            let setup_type = scenario.setup_type;
            let product = scenario.product;
            let validated = validate_decision(SNAPSHOT, &decision(vec![scenario])).unwrap();
            let trigger = validated.scenarios[0].trigger_price;
            let seeds = (setup_type == SetupType::Reversal)
                .then(|| ScenarioSeed::armed(product, Some(1_700_000_000)))
                .into_iter()
                .collect::<Vec<_>>();
            let mut group = group(&validated, &seeds);
            confirm(&mut group, trigger, 1, 11);
            assert!(
                matches!(
                    group.on_trade_tick(tick(4, 15, trigger)),
                    TickOutcome::Triggered(_)
                ),
                "{product:?} {setup_type:?}"
            );
        }
    }

    #[test]
    fn reversal은_기준선_시험전_확인가를_무시한다() {
        let decision = reversal_two_way();
        let leverage = &decision.scenarios[0];
        let mut group = group(&decision, &[]);

        // 같은 틱이 반대 시나리오의 저항 기준선을 관찰할 수 있어 그룹 결과는
        // Updated지만, 레버리지 반전 확인은 아직 시작되지 않아야 한다.
        assert_eq!(
            group.on_trade_tick(tick(1, 11, leverage.trigger_price)),
            TickOutcome::Updated
        );
        let state = group.scenario(ProductKind::Leverage).unwrap();
        assert_eq!(state.status, ScenarioStatus::Armed);
        assert!(!state.reference_observed);

        assert_eq!(
            group.on_trade_tick(tick(2, 12, leverage.reference_price)),
            TickOutcome::Updated
        );
        let state = group.scenario(ProductKind::Leverage).unwrap();
        assert!(state.reference_observed);
        assert_eq!(state.reference_observed_at, Some(1_700_000_012));

        confirm(&mut group, leverage.trigger_price, 3, 13);
        assert!(matches!(
            group.on_trade_tick(tick(6, 17, leverage.trigger_price)),
            TickOutcome::Triggered(_)
        ));
    }

    #[test]
    fn 최근봉_seed는_reversal을_활성화하고_우선순위는_무효화가_가장_높다() {
        let decision = reversal_two_way();
        let seeds = [
            ScenarioSeed::armed(ProductKind::Leverage, Some(111)),
            ScenarioSeed::missed(ProductKind::Leverage, Some(222)),
            ScenarioSeed::invalidated(ProductKind::Leverage, Some(333)),
        ];
        let group = group(&decision, &seeds);
        let state = group.scenario(ProductKind::Leverage).unwrap();
        assert_eq!(state.status, ScenarioStatus::Invalidated);
        assert!(state.reference_observed);
        assert_eq!(state.reference_observed_at, Some(333));
    }

    #[test]
    fn 무효화가는_확인중에도_즉시_terminal로_종결한다() {
        let decision = continuation_two_way();
        let leverage = &decision.scenarios[0];
        let mut group = group(&decision, &[]);
        group.on_trade_tick(tick(1, 11, leverage.trigger_price));
        assert_eq!(
            group.on_trade_tick(tick(2, 12, leverage.invalidation_price)),
            TickOutcome::Updated
        );
        let state = group.scenario(ProductKind::Leverage).unwrap();
        assert_eq!(state.status, ScenarioStatus::Invalidated);
        assert_eq!(state.confirming_ticks, 0);
    }

    #[test]
    fn 승자는_이미_missed나_invalidated인_상대를_덮어쓰지_않는다() {
        let decision = continuation_two_way();
        let inverse = &decision.scenarios[1];
        let mut missed_group = group(
            &decision,
            &[ScenarioSeed::missed(ProductKind::Leverage, None)],
        );
        confirm(&mut missed_group, inverse.trigger_price, 1, 11);
        assert!(matches!(
            missed_group.on_trade_tick(tick(4, 15, inverse.trigger_price)),
            TickOutcome::Triggered(_)
        ));
        assert_eq!(
            missed_group.scenario(ProductKind::Leverage).unwrap().status,
            ScenarioStatus::Missed
        );
    }

    #[test]
    fn 재연결은_확인과_reversal_기준선활성화를_모두_초기화한다() {
        let decision = reversal_two_way();
        let leverage = &decision.scenarios[0];
        let mut group = group(
            &decision,
            &[ScenarioSeed::armed(ProductKind::Leverage, Some(111))],
        );
        group.on_trade_tick(tick(10, 11, leverage.trigger_price));

        assert_eq!(group.reset_for_reconnect(Duration::from_secs(12)), 2);
        let state = group.scenario(ProductKind::Leverage).unwrap();
        assert_eq!(state.status, ScenarioStatus::Armed);
        assert_eq!(state.confirming_ticks, 0);
        assert!(!state.reference_observed);
        assert_eq!(state.reference_observed_at, None);

        // 순번 기준은 재설정됐지만 reset_at 이하의 큐 잔여 틱은 받지 않는다.
        assert_eq!(
            group.on_trade_tick(tick(1, 12, leverage.reference_price)),
            TickOutcome::Ignored
        );
        assert_eq!(
            group.on_trade_tick(tick(1, 13, leverage.reference_price)),
            TickOutcome::Updated
        );
    }

    #[test]
    fn 재연결_reset이_만료경계면_expired_변경을_호출자에게_알린다() {
        let decision = continuation_two_way();
        let mut group = group(&decision, &[]);

        assert_eq!(group.reset_for_reconnect(Duration::from_secs(310)), 2);
        assert!(!group.has_active_scenarios());
        assert!(group
            .scenarios()
            .iter()
            .all(|state| state.status == ScenarioStatus::Expired));
    }

    #[test]
    fn 중복_역행_무거래량_틱은_확인에_포함하지_않는다() {
        let decision = continuation_two_way();
        let up = decision.scenarios[0].trigger_price;
        let mut group = group(&decision, &[]);
        group.on_trade_tick(tick(5, 11, up));
        assert_eq!(group.on_trade_tick(tick(5, 12, up)), TickOutcome::Ignored);
        assert_eq!(group.on_trade_tick(tick(4, 13, up)), TickOutcome::Ignored);
        let mut no_volume = tick(6, 14, up);
        no_volume.volume = 0;
        assert_eq!(group.on_trade_tick(no_volume), TickOutcome::Ignored);
        assert_eq!(
            group
                .scenario(ProductKind::Leverage)
                .unwrap()
                .confirming_ticks,
            1
        );
    }

    #[test]
    fn 입력직전_마지막_체결순번의_재전송도_무장후_무시한다() {
        let decision = continuation_two_way();
        let up = decision.scenarios[0].trigger_price;
        let mut group = OcoGroup::arm(
            77,
            9,
            &decision,
            &[],
            Some(41),
            Duration::from_secs(10),
            Duration::from_secs(310),
        )
        .unwrap();

        assert_eq!(group.on_trade_tick(tick(41, 11, up)), TickOutcome::Ignored);
        assert_eq!(
            group
                .scenario(ProductKind::Leverage)
                .unwrap()
                .confirming_ticks,
            0
        );
        assert_eq!(group.on_trade_tick(tick(42, 12, up)), TickOutcome::Updated);
    }

    #[test]
    fn fifo_배리어_뒤_도착한_새_순번은_무장시각보다_일찍_수신됐어도_처리한다() {
        let decision = continuation_two_way();
        let up = decision.scenarios[0].trigger_price;
        let mut group = OcoGroup::arm(
            77,
            9,
            &decision,
            &[],
            Some(41),
            Duration::from_secs(100),
            Duration::from_secs(310),
        )
        .unwrap();

        assert_eq!(group.on_trade_tick(tick(42, 90, up)), TickOutcome::Updated);
        assert_eq!(
            group
                .scenario(ProductKind::Leverage)
                .unwrap()
                .confirming_ticks,
            1
        );
    }

    #[test]
    fn 만료는_활성상태만_expired로_바꾸고_terminal은_보존한다() {
        let decision = continuation_two_way();
        let mut group = group(
            &decision,
            &[ScenarioSeed::missed(ProductKind::Inverse, None)],
        );
        assert!(group.expire(Duration::from_secs(310)));
        assert_eq!(
            group.scenario(ProductKind::Leverage).unwrap().status,
            ScenarioStatus::Expired
        );
        assert_eq!(
            group.scenario(ProductKind::Inverse).unwrap().status,
            ScenarioStatus::Missed
        );
        assert!(group.winner().is_none());
    }

    #[test]
    fn 만료_경계의_틱은_진입시키지_않는다() {
        let decision = continuation_two_way();
        let up = decision.scenarios[0].trigger_price;
        let mut group = group(&decision, &[]);
        group.on_trade_tick(tick(1, 300, up));
        group.on_trade_tick(tick(2, 306, up));
        group.on_trade_tick(tick(3, 307, up));

        assert_eq!(group.on_trade_tick(tick(4, 310, up)), TickOutcome::Expired);
        assert!(group
            .scenarios()
            .iter()
            .all(|state| state.status == ScenarioStatus::Expired));
        assert!(group.winner().is_none());
    }

    #[test]
    fn 확인_표시시간은_삼초에서_고정한다() {
        let decision = continuation_two_way();
        let up = decision.scenarios[0].trigger_price;
        let mut group = group(&decision, &[]);
        group.on_trade_tick(tick(1, 11, up));
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
