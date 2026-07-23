//! 자동·섀도 매매의 실행 상태와 OCO 소유권을 한 잠금 안에서 관리한다.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::oco::{
    OcoGroup, ScenarioSeed, TickOutcome, TradeTick, TriggeredScenario, ValidatedDecision,
};
use crate::types::{
    AutoSymbols, AutomationDecisionStatus, AutomationPhase, AutomationPositionInfo,
    AutomationScenarioInfo, AutomationSnapshot, ControlMode, MarketDayStatus, MarketRegime,
    ProductKind, ScenarioStatus,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnedPosition {
    pub product: ProductKind,
    pub code: String,
    #[serde(default)]
    pub entry_qty: u64,
    pub qty: u64,
    pub avg_price: f64,
    #[serde(default)]
    pub last_price: f64,
    pub target_return_pct: f64,
    pub target_price: u64,
    pub first_fill_at: i64,
    pub exit_deadline: i64,
    pub target_order_no: Option<String>,
    pub target_org_no: Option<String>,
    #[serde(default)]
    pub target_intent_id: Option<String>,
    #[serde(default)]
    pub exit_order_no: Option<String>,
    #[serde(default)]
    pub exit_order_org_no: Option<String>,
    #[serde(default)]
    pub exit_intent_id: Option<String>,
    #[serde(default)]
    pub exit_requested_at: Option<i64>,
    #[serde(default)]
    pub pending_exit_reason: Option<String>,
    #[serde(default)]
    pub exit_qty: u64,
    #[serde(default)]
    pub exit_value: f64,
    pub trade_id: String,
    pub shadow: bool,
    #[serde(default)]
    pub profit_guard_armed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PositionPriceUpdate {
    pub rate: f64,
    pub guard_armed_changed: bool,
    pub profit_guard_triggered: bool,
}

fn exit_reason_priority(reason: &str) -> u8 {
    match reason {
        "market_close" => 4,
        "stop_loss" => 3,
        "profit_guard" => 2,
        "max_holding" => 1,
        _ => 0,
    }
}

fn decision_status_for_group(group: &OcoGroup) -> AutomationDecisionStatus {
    if group.winner().is_some() {
        return AutomationDecisionStatus::Triggered;
    }
    if group.has_active_scenarios() {
        return AutomationDecisionStatus::Armed;
    }
    if group
        .scenarios()
        .iter()
        .any(|state| state.status == ScenarioStatus::Invalidated)
    {
        return AutomationDecisionStatus::Invalidated;
    }
    if group
        .scenarios()
        .iter()
        .all(|state| state.status == ScenarioStatus::Missed)
    {
        return AutomationDecisionStatus::Missed;
    }
    if group
        .scenarios()
        .iter()
        .any(|state| state.status == ScenarioStatus::Expired)
    {
        return AutomationDecisionStatus::Expired;
    }
    if group
        .scenarios()
        .iter()
        .any(|state| state.status == ScenarioStatus::Triggered)
    {
        return AutomationDecisionStatus::Triggered;
    }
    AutomationDecisionStatus::Replaced
}

pub(crate) fn scenario_terminal_reason_ko(status: ScenarioStatus) -> Option<&'static str> {
    match status {
        ScenarioStatus::Missed => Some("응답 적용 시 확인가를 이미 지나 추격 진입하지 않음"),
        ScenarioStatus::Invalidated => Some("무효화가 침범"),
        ScenarioStatus::Expired => Some("다음 판단 슬롯 만료"),
        ScenarioStatus::Replaced => Some("새 LLM 판단으로 교체"),
        ScenarioStatus::CancelledByOco => Some("반대 시나리오 진입 확정"),
        ScenarioStatus::Invalid => Some("의미 검증 실패"),
        _ => None,
    }
}

/// 실전 Auto 진입 POST 전부터 목표 주문의 소유권이 확정될 때까지 유지하는 복구 표식.
/// 주문 세부값은 장부의 `orders`가 소스 오브 트루스이며, 여기에는 계좌 수량 귀속과
/// 목표 주문 재구성에 꼭 필요한 값만 중복 저장한다.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingAutoEntry {
    pub intent_id: String,
    pub trade_id: String,
    pub baseline_qty: u64,
    pub product: ProductKind,
    pub target_return_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistedAutomation {
    pub mode: ControlMode,
    pub session_id: Option<String>,
    pub position: Option<OwnedPosition>,
    #[serde(default)]
    pub pending_entry: Option<PendingAutoEntry>,
    pub shadow_cash: Option<u64>,
    /// 이미 예약된 다음 판단 슬롯. 앱 재시작이 정확한 5분 경계에 겹쳐도
    /// 방금 호출한 슬롯을 다시 호출하지 않도록 장부에 함께 보존한다.
    #[serde(default)]
    pub next_decision_at: Option<i64>,
    /// 15:05처럼 다음 슬롯이 없는 경우에도 마지막 호출 슬롯을 구분한다.
    #[serde(default)]
    pub last_decision_slot: Option<i64>,
}

impl Default for PersistedAutomation {
    fn default() -> Self {
        Self {
            mode: ControlMode::Manual,
            session_id: None,
            position: None,
            pending_entry: None,
            shadow_cash: None,
            next_decision_at: None,
            last_decision_slot: None,
        }
    }
}

pub struct AutomationRuntime {
    mode: ControlMode,
    phase: AutomationPhase,
    revision: u64,
    next_decision_at: Option<i64>,
    last_decision_slot: Option<i64>,
    decision_row_id: Option<i64>,
    decision_key: Option<String>,
    decision_status: Option<AutomationDecisionStatus>,
    market_regime: Option<MarketRegime>,
    decision_summary_ko: Option<String>,
    group: Option<OcoGroup>,
    group_expires_epoch: Option<i64>,
    position: Option<OwnedPosition>,
    pending_entry: Option<PendingAutoEntry>,
    shadow_cash: Option<u64>,
    session_id: Option<String>,
    error: Option<String>,
    exit_reason: Option<String>,
}

impl AutomationRuntime {
    pub fn new(saved: PersistedAutomation, next_decision_at: Option<i64>) -> Self {
        let phase = if saved.position.is_some() || saved.pending_entry.is_some() {
            AutomationPhase::Reconciling
        } else {
            AutomationPhase::Idle
        };
        let exit_reason = saved
            .position
            .as_ref()
            .and_then(|position| position.pending_exit_reason.clone());
        Self {
            mode: saved.mode,
            phase,
            revision: 0,
            next_decision_at,
            last_decision_slot: saved.last_decision_slot,
            decision_row_id: None,
            decision_key: None,
            decision_status: None,
            market_regime: None,
            decision_summary_ko: None,
            group: None,
            group_expires_epoch: None,
            position: saved.position,
            pending_entry: saved.pending_entry,
            shadow_cash: saved.shadow_cash,
            session_id: saved.session_id,
            error: None,
            exit_reason,
        }
    }

    pub fn mode(&self) -> ControlMode {
        self.mode
    }

    pub fn phase(&self) -> AutomationPhase {
        self.phase
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn next_decision_at(&self) -> Option<i64> {
        self.next_decision_at
    }

    pub fn last_decision_slot(&self) -> Option<i64> {
        self.last_decision_slot
    }

    pub fn set_next_decision_at(&mut self, next: Option<i64>) {
        self.next_decision_at = next;
    }

    /// 휴장·개장일 확인 실패 중에는 기존 포지션 소유권은 그대로 두고 신규 판단
    /// 상태만 폐기한다. 진행 중인 진입 태스크도 revision/phase 불일치로 주문 전에 멈춘다.
    pub fn pause_for_market_day(&mut self) -> bool {
        let mut changed = self.next_decision_at.take().is_some();
        if self.position.is_none()
            && self.pending_entry.is_none()
            && matches!(
                self.phase,
                AutomationPhase::Idle
                    | AutomationPhase::Analyzing
                    | AutomationPhase::ArmedOco
                    | AutomationPhase::EntryPending
            )
        {
            changed |= self.group.is_some() || self.phase != AutomationPhase::Idle;
            self.replace_group();
            self.phase = AutomationPhase::Idle;
        }
        if changed {
            self.revision = self.revision.saturating_add(1);
        }
        changed
    }

    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    pub fn set_session_id(&mut self, session_id: Option<String>) {
        self.session_id = session_id;
    }

    pub fn decision_key(&self) -> Option<&str> {
        self.decision_key.as_deref()
    }

    pub fn position(&self) -> Option<&OwnedPosition> {
        self.position.as_ref()
    }

    pub fn pending_entry(&self) -> Option<&PendingAutoEntry> {
        self.pending_entry.as_ref()
    }

    /// 주문 intent보다 먼저 저장한다. 이 상태만 있고 장부 주문이 없으면 POST가
    /// 실행되지 않았음을 시작 조정에서 안전하게 판정할 수 있다.
    pub fn set_pending_entry(&mut self, pending: PendingAutoEntry) -> bool {
        if self.mode != ControlMode::Auto
            || self.phase != AutomationPhase::EntryPending
            || self.pending_entry.is_some()
        {
            return false;
        }
        self.pending_entry = Some(pending);
        self.revision = self.revision.saturating_add(1);
        true
    }

    /// IOC 체결분을 목표 주문 POST 전에 먼저 소유 포지션으로 저장한다.
    pub fn stage_entry_position(&mut self, position: OwnedPosition) -> bool {
        // 지연 체결 복구에서는 pending 표식이 디스크/WS 경계에서 먼저 사라졌을 수
        // 있다. 실체결 포지션의 보호가 우선이므로 Auto·무포지션이면 복구를 허용한다.
        if self.mode != ControlMode::Auto || position.shadow || self.position.is_some() {
            return false;
        }
        self.position = Some(position);
        self.phase = AutomationPhase::EntryPending;
        self.error = None;
        self.revision = self.revision.saturating_add(1);
        true
    }

    /// 목표 주문 intent를 POST 전에 포지션과 결합해 재시작 시 소유권을 보존한다.
    pub fn set_pending_target_intent(&mut self, intent_id: String) -> bool {
        let Some(position) = self.position.as_mut() else {
            return false;
        };
        if self.pending_entry.is_none()
            || position.target_intent_id.is_some()
            || position.target_order_no.is_some()
        {
            return false;
        }
        position.target_intent_id = Some(intent_id);
        self.revision = self.revision.saturating_add(1);
        true
    }

    /// 목표 주문번호가 확정된 뒤에만 pending 표식을 제거하고 Holding으로 승격한다.
    pub fn complete_pending_entry(&mut self) -> bool {
        let protected = self.position.as_ref().is_some_and(|position| {
            !position.shadow
                && position.target_intent_id.is_some()
                && position.target_order_no.is_some()
                && position.target_org_no.is_some()
        });
        if self.pending_entry.is_none() || !protected {
            return false;
        }
        self.pending_entry = None;
        self.phase = AutomationPhase::Holding;
        self.error = None;
        self.revision = self.revision.saturating_add(1);
        true
    }

    pub fn clear_pending_entry(&mut self) {
        self.pending_entry = None;
        if self.position.is_some() {
            self.phase = AutomationPhase::Holding;
        } else {
            self.phase = AutomationPhase::Idle;
        }
        self.revision = self.revision.saturating_add(1);
    }

    /// 15:15 무노출 확인 뒤 도착한 지연 체결 통보가 저장된 Auto intent와
    /// 일치할 때만 조정용 pending을 되살린다. 새 주문을 제출하는 전이가 아니다.
    pub fn restore_pending_entry(&mut self, pending: PendingAutoEntry) -> bool {
        if self.mode != ControlMode::Auto || self.position.is_some() || self.pending_entry.is_some()
        {
            return false;
        }
        self.pending_entry = Some(pending);
        self.phase = AutomationPhase::EntryPending;
        self.error = Some("마감 조정 뒤 지연된 자동 진입 체결을 다시 대조합니다".into());
        self.revision = self.revision.saturating_add(1);
        true
    }

    pub fn pending_matches(&self, triggered: &TriggeredScenario) -> bool {
        self.phase == AutomationPhase::EntryPending
            && self.group.as_ref().is_some_and(|group| {
                group.group_id() == triggered.group_id
                    && group.revision() == triggered.revision
                    && group
                        .winner()
                        .is_some_and(|winner| winner.scenario.product == triggered.scenario.product)
            })
    }

    pub fn pending_matches_at(&self, triggered: &TriggeredScenario, now_epoch: i64) -> bool {
        self.pending_matches(triggered)
            && self
                .group_expires_epoch
                .is_some_and(|expires_at| now_epoch < expires_at)
    }

    pub fn shadow_cash(&self) -> Option<u64> {
        self.shadow_cash
    }

    pub fn set_shadow_cash(&mut self, cash: Option<u64>) {
        self.shadow_cash = cash;
    }

    pub fn sync_position(&mut self, qty: u64, last_price: f64) {
        if let Some(position) = &mut self.position {
            position.qty = qty;
            position.last_price = last_price;
        }
    }

    /// 실전 목표/청산 체결을 남은 Auto 수량에 한 번 반영한다.
    pub fn apply_real_exit_fill(
        &mut self,
        code: &str,
        order_no: &str,
        qty: u64,
        price: f64,
    ) -> bool {
        let Some(position) = &mut self.position else {
            return false;
        };
        if position.shadow || position.code != code || qty == 0 || price <= 0.0 {
            return false;
        }
        let owned_order = position.target_order_no.as_deref() == Some(order_no)
            || position.exit_order_no.as_deref() == Some(order_no);
        if !owned_order {
            return false;
        }
        let applied = qty.min(position.qty);
        position.qty -= applied;
        position.exit_qty = position.exit_qty.saturating_add(applied);
        position.exit_value += price * applied as f64;
        position.last_price = price;
        position.qty == 0
    }

    pub fn persistable(&self) -> PersistedAutomation {
        PersistedAutomation {
            mode: self.mode,
            session_id: self.session_id.clone(),
            position: self.position.clone(),
            pending_entry: self.pending_entry.clone(),
            shadow_cash: self.shadow_cash,
            next_decision_at: self.next_decision_at,
            last_decision_slot: self.last_decision_slot,
        }
    }

    /// 앱 시작 조정이 끝난 뒤 정상 대기 또는 보유 관리 상태로 진입한다.
    pub fn finish_reconcile(&mut self, suspended_reason: Option<String>) {
        if let Some(reason) = suspended_reason {
            self.phase = AutomationPhase::Suspended;
            self.error = Some(reason);
        } else if self.pending_entry.is_some() {
            self.phase = AutomationPhase::EntryPending;
            self.error = None;
        } else if self.phase == AutomationPhase::ExitPending
            || self
                .position
                .as_ref()
                .is_some_and(|position| position.exit_intent_id.is_some())
        {
            self.phase = AutomationPhase::ExitPending;
            self.exit_reason = self
                .position
                .as_ref()
                .and_then(|position| position.pending_exit_reason.clone())
                .or_else(|| Some("startup_reconcile".into()));
            self.error = None;
        } else if self.position.is_some() {
            self.phase = AutomationPhase::Holding;
            self.error = None;
        } else {
            self.phase = AutomationPhase::Idle;
            self.error = None;
        }
    }

    pub fn suspend(&mut self, reason: impl Into<String>) {
        self.replace_group();
        self.phase = AutomationPhase::Suspended;
        self.error = Some(reason.into());
        self.revision = self.revision.saturating_add(1);
    }

    /// 안전상 즉시 신규 진입을 막아야 하지만 기존 OCO 장부 종결이 실패한 경우,
    /// 그룹을 메모리에 남겨 다음 재시도에서 정확한 자식 상태를 복구할 수 있게 한다.
    /// Suspended 단계에서는 `on_trade_tick`이 그룹을 실행하지 않는다.
    pub fn suspend_preserving_group(&mut self, reason: impl Into<String>) {
        self.phase = AutomationPhase::Suspended;
        self.error = Some(reason.into());
        self.revision = self.revision.saturating_add(1);
    }

    pub fn has_group_pending_ledger_repair(&self) -> bool {
        self.phase == AutomationPhase::Suspended && self.group.is_some()
    }

    /// 보존한 그룹의 장부 종결이 성공한 뒤 메모리 그룹만 제거한다.
    pub fn finish_group_ledger_repair(&mut self) -> bool {
        if !self.has_group_pending_ledger_repair() {
            return false;
        }
        self.replace_group();
        self.revision = self.revision.saturating_add(1);
        true
    }

    /// 새 모드 설정만 원자적으로 반영한다. 실주식 인계·섀도 청산은 호출자가 먼저 끝낸다.
    pub fn set_mode_after_cleanup(
        &mut self,
        mode: ControlMode,
        next_decision_at: Option<i64>,
        shadow_cash: Option<u64>,
    ) {
        self.replace_group();
        self.mode = mode;
        self.next_decision_at = next_decision_at;
        self.decision_row_id = None;
        self.decision_key = None;
        self.decision_status = None;
        self.market_regime = None;
        self.decision_summary_ko = None;
        self.pending_entry = None;
        self.shadow_cash = if mode == ControlMode::Shadow {
            shadow_cash
        } else {
            None
        };
        self.error = None;
        self.phase = if self.position.is_some() {
            AutomationPhase::Holding
        } else {
            AutomationPhase::Idle
        };
        self.revision = self.revision.saturating_add(1);
    }

    pub fn mark_handoff(&mut self) {
        self.replace_group();
        self.phase = AutomationPhase::Handoff;
        self.revision = self.revision.saturating_add(1);
    }

    /// 제어 모드 전환이 주문 actor를 선점한 뒤 OCO 실행권을 폐기한다.
    ///
    /// 호출자는 먼저 같은 그룹의 장부 종결을 성공시켜야 한다. 그룹 제거와 revision
    /// 증가는 늦게 깨어난 트리거 태스크가 새 모드에서 진입을 커밋하지 못하게 한다.
    pub fn clear_group_for_mode_transition(&mut self) -> bool {
        if self.group.is_none() {
            return false;
        }
        self.replace_group();
        self.phase = if self.position.is_some() {
            AutomationPhase::Holding
        } else {
            AutomationPhase::Idle
        };
        self.revision = self.revision.saturating_add(1);
        true
    }

    /// 수동 인계가 중간에 실패하면 Auto 소유권을 유지한다. 포지션이 남아 있으면
    /// 손절·최대보유·15:15 청산이 계속 작동하도록 Holding으로 되돌리고, 이미
    /// 포지션이 사라졌다면 재진입을 막기 위해 Suspended에 둔다.
    pub fn abort_handoff(&mut self, message: impl Into<String>) {
        self.phase = if self.position.is_some() {
            AutomationPhase::Holding
        } else {
            AutomationPhase::Suspended
        };
        self.error = Some(message.into());
        self.revision = self.revision.saturating_add(1);
    }

    pub fn clear_position_for_handoff(&mut self) -> Option<OwnedPosition> {
        let position = self.position.take();
        self.pending_entry = None;
        self.phase = AutomationPhase::Idle;
        self.revision = self.revision.saturating_add(1);
        position
    }

    /// 정규 슬롯에서만 분석을 시작한다. 반환 revision과 응답 적용 revision이 같아야 한다.
    pub fn begin_analysis(
        &mut self,
        decision_slot: i64,
        next_decision_at: Option<i64>,
    ) -> Option<u64> {
        if !self.can_begin_analysis() {
            return None;
        }
        self.replace_group();
        self.revision = self.revision.saturating_add(1);
        self.phase = AutomationPhase::Analyzing;
        self.next_decision_at = next_decision_at;
        self.last_decision_slot = Some(decision_slot);
        self.decision_row_id = None;
        self.decision_key = None;
        self.decision_status = None;
        self.market_regime = None;
        self.decision_summary_ko = None;
        self.error = None;
        Some(self.revision)
    }

    pub fn can_begin_analysis(&self) -> bool {
        matches!(self.mode, ControlMode::Auto | ControlMode::Shadow)
            && matches!(
                self.phase,
                AutomationPhase::Idle | AutomationPhase::ArmedOco
            )
            && self.position.is_none()
            && self.pending_entry.is_none()
    }

    pub fn fail_analysis(&mut self, expected_revision: u64, message: impl Into<String>) -> bool {
        if self.phase != AutomationPhase::Analyzing || self.revision != expected_revision {
            return false;
        }
        self.phase = AutomationPhase::Idle;
        self.decision_status = Some(AutomationDecisionStatus::Error);
        self.error = Some(message.into());
        true
    }

    #[allow(clippy::too_many_arguments)]
    pub fn accept_decision(
        &mut self,
        expected_revision: u64,
        decision_row_id: i64,
        decision_key: String,
        decision: &ValidatedDecision,
        decision_status: AutomationDecisionStatus,
        seeds: &[ScenarioSeed],
        last_trade_sequence: Option<u64>,
        group_id: i64,
        armed_at: Duration,
        expires_at: Duration,
        expires_epoch: i64,
    ) -> bool {
        if self.phase != AutomationPhase::Analyzing || self.revision != expected_revision {
            return false;
        }
        self.decision_row_id = Some(decision_row_id);
        self.decision_key = Some(decision_key);
        self.decision_status = Some(decision_status);
        self.market_regime = Some(decision.market_regime);
        self.decision_summary_ko = Some(decision.decision_summary_ko.clone());
        self.group_expires_epoch = Some(expires_epoch);
        if decision.scenarios.is_empty() {
            self.group = None;
            self.phase = AutomationPhase::Idle;
            return true;
        }
        let Ok(group) = OcoGroup::arm(
            group_id,
            expected_revision,
            decision,
            seeds,
            last_trade_sequence,
            armed_at,
            expires_at,
        ) else {
            self.group = None;
            self.phase = AutomationPhase::Idle;
            self.decision_status = Some(AutomationDecisionStatus::Error);
            self.error = Some("OCO 그룹 생성 실패".into());
            return false;
        };
        let has_active_scenarios = group.has_active_scenarios();
        if !has_active_scenarios {
            self.decision_status = Some(decision_status_for_group(&group));
        }
        self.group = Some(group);
        self.phase = if has_active_scenarios {
            AutomationPhase::ArmedOco
        } else {
            AutomationPhase::Idle
        };
        self.error = None;
        true
    }

    pub fn on_trade_tick(&mut self, tick: TradeTick) -> Option<TriggeredScenario> {
        if self.phase != AutomationPhase::ArmedOco {
            return None;
        }
        let group = self.group.as_mut()?;
        match group.on_trade_tick(tick) {
            TickOutcome::Triggered(triggered) => {
                // 상태 전환과 winner 결정은 같은 mutex 임계구역에서 일어난다.
                self.phase = AutomationPhase::EntryPending;
                self.decision_status = Some(AutomationDecisionStatus::Triggered);
                self.revision = self.revision.saturating_add(1);
                Some(triggered)
            }
            TickOutcome::Expired => {
                self.phase = AutomationPhase::Idle;
                self.decision_status = Some(decision_status_for_group(group));
                self.revision = self.revision.saturating_add(1);
                None
            }
            TickOutcome::Updated => {
                if !group.has_active_scenarios() {
                    self.phase = AutomationPhase::Idle;
                    self.decision_status = Some(decision_status_for_group(group));
                    self.revision = self.revision.saturating_add(1);
                }
                None
            }
            TickOutcome::Ignored => None,
        }
    }

    pub fn reset_confirmation(&mut self, at: Duration) -> bool {
        let Some(group) = self.group.as_mut() else {
            return false;
        };
        let changed = group.reset_for_reconnect(at) > 0;
        if changed && !group.has_active_scenarios() {
            self.phase = AutomationPhase::Idle;
            self.decision_status = Some(decision_status_for_group(group));
            self.revision = self.revision.saturating_add(1);
        }
        changed
    }

    pub fn expire_group(&mut self, at: Duration) -> bool {
        let changed = self.group.as_mut().is_some_and(|group| group.expire(at));
        if changed {
            self.phase = AutomationPhase::Idle;
            if let Some(group) = &self.group {
                self.decision_status = Some(decision_status_for_group(group));
            }
            self.revision = self.revision.saturating_add(1);
        }
        changed
    }

    pub fn mark_holding(&mut self, position: OwnedPosition) {
        self.position = Some(position);
        self.phase = AutomationPhase::Holding;
        self.error = None;
        self.revision = self.revision.saturating_add(1);
    }

    pub fn entry_failed(&mut self, message: impl Into<String>) {
        self.pending_entry = None;
        self.phase = AutomationPhase::Idle;
        self.error = Some(message.into());
        self.revision = self.revision.saturating_add(1);
    }

    pub fn begin_exit(&mut self, reason: impl Into<String>) -> Option<OwnedPosition> {
        let reason = reason.into();
        if self.phase == AutomationPhase::ExitPending {
            let current = self.exit_reason.as_deref().unwrap_or_default();
            if exit_reason_priority(&reason) > exit_reason_priority(current) {
                self.exit_reason = Some(reason.clone());
                if let Some(position) = &mut self.position {
                    position.pending_exit_reason = Some(reason);
                }
                self.error = None;
                self.revision = self.revision.saturating_add(1);
            }
            return None;
        }
        if self.phase != AutomationPhase::Holding {
            return None;
        }
        self.phase = AutomationPhase::ExitPending;
        self.exit_reason = Some(reason.clone());
        if let Some(position) = &mut self.position {
            position.pending_exit_reason = Some(reason);
        }
        self.revision = self.revision.saturating_add(1);
        self.position.clone()
    }

    pub fn set_active_exit_order(
        &mut self,
        intent_id: String,
        order_no: Option<String>,
        org_no: Option<String>,
        requested_at: i64,
    ) -> bool {
        let Some(position) = self.position.as_mut() else {
            return false;
        };
        position.exit_intent_id = Some(intent_id);
        position.exit_order_no = order_no;
        position.exit_order_org_no = org_no;
        position.exit_requested_at = Some(requested_at);
        true
    }

    pub fn clear_active_exit_order(&mut self) {
        if let Some(position) = &mut self.position {
            position.exit_order_no = None;
            position.exit_order_org_no = None;
            position.exit_intent_id = None;
            position.exit_requested_at = None;
        }
    }

    pub fn clear_target_order(&mut self) {
        if let Some(position) = &mut self.position {
            position.target_order_no = None;
            position.target_org_no = None;
            position.target_intent_id = None;
        }
    }

    /// POST가 시작되지 않은 목표 intent를 종결한 뒤 런타임 저장이 실패했을 때
    /// 주문번호 없는 소유권 표식을 되돌린다.
    pub fn restore_target_intent(&mut self, intent_id: String) -> bool {
        let Some(position) = self.position.as_mut() else {
            return false;
        };
        position.target_intent_id = Some(intent_id);
        position.target_order_no = None;
        position.target_org_no = None;
        true
    }

    /// 정확 종결 상태를 저장하지 못해 목표 주문 해제를 되돌릴 때 사용한다.
    pub fn restore_target_order(
        &mut self,
        intent_id: String,
        order_no: String,
        org_no: String,
    ) -> bool {
        let Some(position) = self.position.as_mut() else {
            return false;
        };
        position.target_intent_id = Some(intent_id);
        position.target_order_no = Some(order_no);
        position.target_org_no = Some(org_no);
        true
    }

    /// 응답이 불명확했던 목표 주문을 조회로 찾은 뒤 소유 주문번호를 결합한다.
    pub fn bind_target_order(&mut self, order_no: String, org_no: String) -> bool {
        let Some(position) = self.position.as_mut() else {
            return false;
        };
        if position.target_intent_id.is_none() {
            return false;
        }
        match position.target_order_no.as_deref() {
            Some(existing) if existing != order_no => return false,
            Some(_) => {
                if !org_no.trim().is_empty() {
                    position.target_org_no = Some(org_no);
                }
            }
            None => {
                position.target_order_no = Some(order_no);
                position.target_org_no = Some(org_no);
            }
        }
        true
    }

    /// 15:15 청산은 복구 불일치로 Suspended여도 저장된 Auto 소유 포지션에 우선한다.
    pub fn begin_market_close_exit(&mut self) -> Option<OwnedPosition> {
        let position = self.position.as_ref()?;
        if position.shadow {
            return None;
        }
        // 이미 제출·조정 중인 청산은 새 POST를 시작하지 않되, 재시작 시 저장된
        // max_holding/stop_loss 사유는 장마감으로 승격한다. 시작 호출부가 이 값을
        // 보고 기존 주문 조정을 같은 identity로 재개한다.
        if self.phase == AutomationPhase::ExitPending {
            let already_market_close = self.exit_reason.as_deref() == Some("market_close")
                && self
                    .position
                    .as_ref()
                    .and_then(|position| position.pending_exit_reason.as_deref())
                    == Some("market_close");
            if !already_market_close {
                self.exit_reason = Some("market_close".into());
                if let Some(position) = &mut self.position {
                    position.pending_exit_reason = Some("market_close".into());
                }
                self.error = None;
                self.revision = self.revision.saturating_add(1);
            }
            return None;
        }
        if !matches!(
            self.phase,
            AutomationPhase::Holding | AutomationPhase::Suspended
        ) {
            return None;
        }
        self.phase = AutomationPhase::ExitPending;
        self.exit_reason = Some("market_close".into());
        if let Some(position) = &mut self.position {
            position.pending_exit_reason = Some("market_close".into());
        }
        self.error = None;
        self.revision = self.revision.saturating_add(1);
        self.position.clone()
    }

    pub fn exit_reason(&self) -> Option<&str> {
        self.exit_reason.as_deref()
    }

    pub fn exit_waiting(&mut self, message: impl Into<String>) {
        self.phase = AutomationPhase::ExitPending;
        self.error = Some(message.into());
        self.revision = self.revision.saturating_add(1);
    }

    pub fn complete_exit(&mut self) -> Option<OwnedPosition> {
        let position = self.position.take();
        self.pending_entry = None;
        self.phase = AutomationPhase::Idle;
        self.error = None;
        self.exit_reason = None;
        self.revision = self.revision.saturating_add(1);
        position
    }

    pub fn owns_real_code(&self, code: &str) -> bool {
        self.position
            .as_ref()
            .is_some_and(|position| !position.shadow && position.code == code)
    }

    /// 유효하지 않은 가격은 `last_price`도 갱신하지 않고 `None`을 반환한다.
    /// 호출자가 `None`을 "판정 근거 없음"으로 다루므로, 0이나 무한대가 손절·수익
    /// 보호를 발동시키는 경로를 피드 검사와 별개로 여기서도 차단한다.
    pub fn update_position_price(&mut self, code: &str, price: f64) -> Option<PositionPriceUpdate> {
        if !price.is_finite() || price <= 0.0 {
            return None;
        }
        let position = self.position.as_mut()?;
        if position.code != code || position.avg_price <= 0.0 {
            return None;
        }
        position.last_price = price;
        let rate = (price / position.avg_price - 1.0) * 100.0;
        let was_armed = position.profit_guard_armed;
        if position.target_return_pct > crate::util::AUTO_PROFIT_GUARD_PCT
            && rate > crate::util::AUTO_PROFIT_GUARD_PCT
        {
            position.profit_guard_armed = true;
        }
        Some(PositionPriceUpdate {
            rate,
            guard_armed_changed: !was_armed && position.profit_guard_armed,
            profit_guard_triggered: position.profit_guard_armed
                && rate <= crate::util::AUTO_PROFIT_GUARD_PCT,
        })
    }

    pub fn snapshot(
        &self,
        runtime_id: &str,
        runtime_generation: u64,
        symbols: &AutoSymbols,
        now: Duration,
        market_day_status: MarketDayStatus,
        market_day_message: Option<String>,
    ) -> AutomationSnapshot {
        let scenarios = self
            .group
            .as_ref()
            .map(|group| {
                group
                    .scenarios()
                    .iter()
                    .enumerate()
                    .map(|(index, state)| AutomationScenarioInfo {
                        id: group.group_id().saturating_mul(10) + index as i64,
                        product: state.scenario.product,
                        code: state.scenario.product.code(symbols).to_string(),
                        setup_type: state.scenario.setup_type,
                        reference_price: state.scenario.reference_price,
                        confirmation_price: state.scenario.confirmation_price,
                        trigger_price: state.scenario.trigger_price,
                        invalidation_price: state.scenario.invalidation_price,
                        target_return_pct: state.scenario.target_return_pct,
                        rationale_ko: state.scenario.rationale_ko.clone(),
                        reference_observed_at: state.reference_observed_at,
                        status: state.status,
                        terminal_reason: scenario_terminal_reason_ko(state.status)
                            .map(str::to_owned),
                        confirming_ticks: u32::from(state.confirming_ticks),
                        confirming_elapsed_ms: group
                            .confirming_elapsed(state.scenario.product, now)
                            .as_millis()
                            .try_into()
                            .unwrap_or(u64::MAX),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let position = self
            .position
            .as_ref()
            .map(|position| AutomationPositionInfo {
                product: position.product,
                code: position.code.clone(),
                qty: position.qty,
                avg_price: position.avg_price,
                pnl_rate: if position.avg_price > 0.0 && position.last_price > 0.0 {
                    (position.last_price / position.avg_price - 1.0) * 100.0
                } else {
                    0.0
                },
                target_return_pct: position.target_return_pct,
                target_price: position.target_price,
                exit_deadline: position.exit_deadline,
                shadow: position.shadow,
                profit_guard_armed: position.profit_guard_armed,
            });
        AutomationSnapshot {
            runtime_id: runtime_id.to_string(),
            runtime_generation,
            mode: self.mode,
            phase: self.phase,
            revision: self.revision,
            next_decision_at: self.next_decision_at,
            decision_id: self.decision_row_id,
            group_id: self.group.as_ref().map(OcoGroup::group_id),
            decision_status: self.decision_status,
            market_regime: self.market_regime,
            decision_summary_ko: self.decision_summary_ko.clone(),
            scenarios,
            position,
            shadow_cash: self.shadow_cash,
            error: self.error.clone(),
            market_day_status,
            market_day_message,
        }
    }

    fn replace_group(&mut self) {
        if let Some(group) = &mut self.group {
            group.replace();
            self.decision_status = Some(decision_status_for_group(group));
        }
        self.group = None;
        self.group_expires_epoch = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position(target_return_pct: f64) -> OwnedPosition {
        OwnedPosition {
            product: ProductKind::Leverage,
            code: "0193T0".into(),
            entry_qty: 10,
            qty: 10,
            avg_price: 10_000.0,
            last_price: 10_000.0,
            target_return_pct,
            target_price: 10_040,
            first_fill_at: 1_000,
            exit_deadline: 1_600,
            target_order_no: None,
            target_org_no: None,
            target_intent_id: None,
            exit_order_no: None,
            exit_order_org_no: None,
            exit_intent_id: None,
            exit_requested_at: None,
            pending_exit_reason: None,
            exit_qty: 0,
            exit_value: 0.0,
            trade_id: "profit-guard-test".into(),
            shadow: false,
            profit_guard_armed: false,
        }
    }

    #[test]
    fn 판단_시작_슬롯과_다음_슬롯을_재시작_상태에_보존한다() {
        let slot = 1_753_153_200;
        let next = slot + 300;
        let mut runtime = AutomationRuntime::new(
            PersistedAutomation {
                mode: ControlMode::Auto,
                ..PersistedAutomation::default()
            },
            Some(slot),
        );

        assert!(runtime.begin_analysis(slot, Some(next)).is_some());
        let saved = runtime.persistable();

        assert_eq!(saved.last_decision_slot, Some(slot));
        assert_eq!(saved.next_decision_at, Some(next));
    }

    #[test]
    fn 구버전_상태에는_판단_슬롯_필드가_없어도_된다() {
        let saved: PersistedAutomation = serde_json::from_str(
            r#"{"mode":"manual","sessionId":null,"position":null,"pendingEntry":null,"shadowCash":null}"#,
        )
        .unwrap();

        assert_eq!(saved.next_decision_at, None);
        assert_eq!(saved.last_decision_slot, None);
    }

    #[test]
    fn 목표가_03_초과일_때만_초과후_되밀림에서_수익보호가_발동한다() {
        let mut runtime = AutomationRuntime::new(PersistedAutomation::default(), None);
        runtime.mark_holding(position(0.4));

        let before = runtime.update_position_price("0193T0", 10_030.0).unwrap();
        assert!(!before.guard_armed_changed);
        assert!(!before.profit_guard_triggered);

        let armed = runtime.update_position_price("0193T0", 10_035.0).unwrap();
        assert!(armed.guard_armed_changed);
        assert!(!armed.profit_guard_triggered);
        assert!(runtime.position().unwrap().profit_guard_armed);

        let retraced = runtime.update_position_price("0193T0", 10_030.0).unwrap();
        assert!(retraced.profit_guard_triggered);

        let mut target_point_three = AutomationRuntime::new(PersistedAutomation::default(), None);
        target_point_three.mark_holding(position(0.3));
        let update = target_point_three
            .update_position_price("0193T0", 10_035.0)
            .unwrap();
        assert!(!update.guard_armed_changed);
        assert!(!update.profit_guard_triggered);
    }

    #[test]
    fn 유효하지_않은_가격은_수익률_판정에도_보유가_갱신에도_쓰이지_않는다() {
        let mut runtime = AutomationRuntime::new(PersistedAutomation::default(), None);
        runtime.mark_holding(position(0.4));
        // 수익 보호선을 무장시켜 되밀림 판정이 실제로 가능한 상태로 만든다.
        assert!(
            runtime
                .update_position_price("0193T0", 10_035.0)
                .unwrap()
                .guard_armed_changed
        );

        // 0·음수·NaN·무한대는 손절(-100%)이나 수익보호(0%)로 읽히면 안 된다.
        for invalid in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(
                runtime.update_position_price("0193T0", invalid).is_none(),
                "가격 {invalid}가 수익률 판정에 사용됐다"
            );
            assert_eq!(
                runtime.position().unwrap().last_price,
                10_035.0,
                "가격 {invalid}가 마지막 체결가를 덮어썼다"
            );
        }

        // 유효한 가격은 그대로 판정된다.
        let retraced = runtime.update_position_price("0193T0", 10_030.0).unwrap();
        assert!(retraced.profit_guard_triggered);
    }

    #[test]
    fn 수익보호_무장상태는_구버전_호환과_재시작을_모두_지킨다() {
        let mut saved = PersistedAutomation {
            position: Some(position(0.4)),
            ..PersistedAutomation::default()
        };
        saved.position.as_mut().unwrap().profit_guard_armed = true;
        let json = serde_json::to_value(&saved).unwrap();
        let restored: PersistedAutomation = serde_json::from_value(json.clone()).unwrap();
        assert!(restored.position.unwrap().profit_guard_armed);

        let mut old_json = json;
        old_json["position"]
            .as_object_mut()
            .unwrap()
            .remove("profitGuardArmed");
        let old: PersistedAutomation = serde_json::from_value(old_json).unwrap();
        assert!(!old.position.unwrap().profit_guard_armed);
    }

    #[test]
    fn auto_청산사유는_장마감_손절_수익보호_최대보유_순으로_승격된다() {
        let mut runtime = AutomationRuntime::new(PersistedAutomation::default(), None);
        runtime.mode = ControlMode::Auto;
        runtime.mark_holding(position(0.4));

        assert!(runtime.begin_exit("max_holding").is_some());
        assert!(runtime.begin_exit("profit_guard").is_none());
        assert_eq!(runtime.exit_reason(), Some("profit_guard"));
        assert!(runtime.begin_exit("max_holding").is_none());
        assert_eq!(runtime.exit_reason(), Some("profit_guard"));
        assert!(runtime.begin_exit("stop_loss").is_none());
        assert_eq!(runtime.exit_reason(), Some("stop_loss"));
        assert!(runtime.begin_market_close_exit().is_none());
        assert_eq!(runtime.exit_reason(), Some("market_close"));
        assert_eq!(
            runtime
                .position()
                .and_then(|position| position.pending_exit_reason.as_deref()),
            Some("market_close")
        );
    }

    fn reversal_decision() -> ValidatedDecision {
        super::super::oco::validate_decision(
            185_000,
            &crate::types::ModelDecision {
                market_regime: MarketRegime::Range,
                decision_summary_ko: "저항 재시험 뒤 반락 후보".into(),
                scenarios: vec![crate::types::ModelScenario {
                    product: ProductKind::Inverse,
                    setup_type: crate::types::SetupType::Reversal,
                    reference_price: 185_200,
                    confirmation_price: 184_800,
                    invalidation_price: 185_400,
                    target_return_pct: 0.3,
                    rationale_ko: "반복 저항 시험과 거래량 감소".into(),
                }],
            },
        )
        .unwrap()
    }

    #[test]
    fn terminal_seed와_판단설명은_idle_snapshot에_보존된다() {
        let mut runtime = AutomationRuntime::new(PersistedAutomation::default(), None);
        runtime.mode = ControlMode::Shadow;
        let revision = runtime.begin_analysis(1_000, Some(1_300)).unwrap();
        let decision = reversal_decision();

        assert!(runtime.accept_decision(
            revision,
            17,
            "decision-17".into(),
            &decision,
            AutomationDecisionStatus::Missed,
            &[ScenarioSeed::missed(ProductKind::Inverse, Some(999))],
            None,
            17,
            Duration::from_secs(10),
            Duration::from_secs(310),
            1_300,
        ));
        assert_eq!(runtime.phase(), AutomationPhase::Idle);

        let snapshot = runtime.snapshot(
            "runtime",
            1,
            &AutoSymbols::default(),
            Duration::from_secs(11),
            MarketDayStatus::Open,
            None,
        );
        assert_eq!(snapshot.market_regime, Some(MarketRegime::Range));
        assert_eq!(
            snapshot.decision_status,
            Some(AutomationDecisionStatus::Missed)
        );
        assert_eq!(
            snapshot.decision_summary_ko.as_deref(),
            Some("저항 재시험 뒤 반락 후보")
        );
        assert_eq!(snapshot.scenarios.len(), 1);
        let scenario = &snapshot.scenarios[0];
        assert_eq!(scenario.status, crate::types::ScenarioStatus::Missed);
        assert_eq!(scenario.setup_type, crate::types::SetupType::Reversal);
        assert_eq!(scenario.reference_price, 185_200);
        assert_eq!(scenario.confirmation_price, 184_800);
        assert_eq!(scenario.trigger_price, 184_800);
        assert_eq!(scenario.invalidation_price, 185_400);
        assert_eq!(scenario.reference_observed_at, Some(999));
        assert_eq!(scenario.rationale_ko, "반복 저항 시험과 거래량 감소");
        assert_eq!(
            scenario.terminal_reason.as_deref(),
            Some("응답 적용 시 확인가를 이미 지나 추격 진입하지 않음")
        );
        let json = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(
            json["scenarios"][0]["terminalReason"],
            "응답 적용 시 확인가를 이미 지나 추격 진입하지 않음"
        );
    }

    #[test]
    fn 시나리오_종료사유는_실시간과_장부가_공통_문구를_사용한다() {
        let cases = [
            (
                ScenarioStatus::Missed,
                "응답 적용 시 확인가를 이미 지나 추격 진입하지 않음",
            ),
            (ScenarioStatus::Invalidated, "무효화가 침범"),
            (ScenarioStatus::Expired, "다음 판단 슬롯 만료"),
            (ScenarioStatus::Replaced, "새 LLM 판단으로 교체"),
            (ScenarioStatus::CancelledByOco, "반대 시나리오 진입 확정"),
            (ScenarioStatus::Invalid, "의미 검증 실패"),
        ];
        for (status, expected) in cases {
            assert_eq!(scenario_terminal_reason_ko(status), Some(expected));
        }
        for status in [
            ScenarioStatus::Armed,
            ScenarioStatus::Confirming,
            ScenarioStatus::Triggered,
        ] {
            assert_eq!(scenario_terminal_reason_ko(status), None);
        }
    }

    #[test]
    fn 장부종결_실패_suspend는_그룹을_실행없이_보존하고_재시도후_제거한다() {
        let mut runtime = AutomationRuntime::new(PersistedAutomation::default(), None);
        runtime.mode = ControlMode::Shadow;
        let revision = runtime.begin_analysis(1_000, Some(1_300)).unwrap();
        let decision = reversal_decision();
        assert!(runtime.accept_decision(
            revision,
            20,
            "decision-20".into(),
            &decision,
            AutomationDecisionStatus::Armed,
            &[],
            None,
            20,
            Duration::from_secs(10),
            Duration::from_secs(310),
            1_300,
        ));

        runtime.suspend_preserving_group("장부 종결 재시도");
        assert!(runtime.has_group_pending_ledger_repair());
        assert!(runtime
            .on_trade_tick(TradeTick {
                sequence: 1,
                price: 184_800,
                volume: 1,
                at: Duration::from_secs(20),
                epoch: 1_020,
            })
            .is_none());
        assert!(runtime.finish_group_ledger_repair());
        assert!(!runtime.has_group_pending_ledger_repair());
        assert_eq!(runtime.phase(), AutomationPhase::Suspended);
    }

    #[test]
    fn skip도_국면과_판단요약을_snapshot에_남긴다() {
        let mut runtime = AutomationRuntime::new(PersistedAutomation::default(), None);
        runtime.mode = ControlMode::Shadow;
        let revision = runtime.begin_analysis(1_000, Some(1_300)).unwrap();
        let decision = super::super::oco::validate_decision(
            185_000,
            &crate::types::ModelDecision {
                market_regime: MarketRegime::Unclear,
                decision_summary_ko: "시간축 충돌과 목표 공간 부족으로 관망".into(),
                scenarios: vec![],
            },
        )
        .unwrap();

        assert!(runtime.accept_decision(
            revision,
            19,
            "decision-19".into(),
            &decision,
            AutomationDecisionStatus::Skipped,
            &[],
            None,
            19,
            Duration::from_secs(10),
            Duration::from_secs(310),
            1_300,
        ));
        let snapshot = runtime.snapshot(
            "runtime",
            1,
            &AutoSymbols::default(),
            Duration::from_secs(11),
            MarketDayStatus::Open,
            None,
        );
        assert_eq!(runtime.phase(), AutomationPhase::Idle);
        assert_eq!(snapshot.market_regime, Some(MarketRegime::Unclear));
        assert_eq!(
            snapshot.decision_status,
            Some(AutomationDecisionStatus::Skipped)
        );
        assert_eq!(
            snapshot.decision_summary_ko.as_deref(),
            Some("시간축 충돌과 목표 공간 부족으로 관망")
        );
        assert!(snapshot.scenarios.is_empty());
    }

    #[test]
    fn 전부_의미검증에_실패한_판단은_skip과_구분한다() {
        let mut runtime = AutomationRuntime::new(PersistedAutomation::default(), None);
        runtime.mode = ControlMode::Shadow;
        let revision = runtime.begin_analysis(1_000, Some(1_300)).unwrap();
        let decision = super::super::oco::validate_decision(
            185_000,
            &crate::types::ModelDecision {
                market_regime: MarketRegime::Range,
                decision_summary_ko: "가격 순서가 잘못된 후보".into(),
                scenarios: vec![crate::types::ModelScenario {
                    product: ProductKind::Inverse,
                    setup_type: crate::types::SetupType::Reversal,
                    reference_price: 184_900,
                    confirmation_price: 185_200,
                    invalidation_price: 184_700,
                    target_return_pct: 0.3,
                    rationale_ko: "잘못된 가격 순서".into(),
                }],
            },
        )
        .unwrap();
        assert!(decision.scenarios.is_empty());
        assert!(!decision.rejected.is_empty());

        assert!(runtime.accept_decision(
            revision,
            21,
            "decision-21".into(),
            &decision,
            AutomationDecisionStatus::Invalid,
            &[],
            None,
            21,
            Duration::from_secs(10),
            Duration::from_secs(310),
            1_300,
        ));
        let snapshot = runtime.snapshot(
            "runtime",
            1,
            &AutoSymbols::default(),
            Duration::from_secs(11),
            MarketDayStatus::Open,
            None,
        );
        assert_eq!(runtime.phase(), AutomationPhase::Idle);
        assert_eq!(
            snapshot.decision_status,
            Some(AutomationDecisionStatus::Invalid)
        );
        assert!(snapshot.scenarios.is_empty());
    }

    #[test]
    fn 마지막_active가_무효화되면_runtime은_idle로_내려간다() {
        let mut runtime = AutomationRuntime::new(PersistedAutomation::default(), None);
        runtime.mode = ControlMode::Shadow;
        let revision = runtime.begin_analysis(1_000, Some(1_300)).unwrap();
        let decision = reversal_decision();
        assert!(runtime.accept_decision(
            revision,
            18,
            "decision-18".into(),
            &decision,
            AutomationDecisionStatus::Armed,
            &[],
            None,
            18,
            Duration::from_secs(10),
            Duration::from_secs(310),
            1_300,
        ));

        assert!(runtime
            .on_trade_tick(TradeTick {
                sequence: 1,
                price: 185_400,
                volume: 1,
                at: Duration::from_secs(11),
                epoch: 1_001,
            })
            .is_none());
        assert_eq!(runtime.phase(), AutomationPhase::Idle);
        let snapshot = runtime.snapshot(
            "runtime",
            1,
            &AutoSymbols::default(),
            Duration::from_secs(11),
            MarketDayStatus::Open,
            None,
        );
        assert_eq!(
            snapshot.scenarios[0].status,
            crate::types::ScenarioStatus::Invalidated
        );
        assert_eq!(
            snapshot.decision_status,
            Some(AutomationDecisionStatus::Invalidated)
        );
    }

    #[test]
    fn 재연결_reset이_만료경계와_겹쳐도_runtime이_고착되지_않는다() {
        let mut runtime = AutomationRuntime::new(PersistedAutomation::default(), None);
        runtime.mode = ControlMode::Shadow;
        let revision = runtime.begin_analysis(1_000, Some(1_300)).unwrap();
        let decision = reversal_decision();
        assert!(runtime.accept_decision(
            revision,
            20,
            "decision-20".into(),
            &decision,
            AutomationDecisionStatus::Armed,
            &[],
            None,
            20,
            Duration::from_secs(10),
            Duration::from_secs(310),
            1_300,
        ));

        assert!(runtime.reset_confirmation(Duration::from_secs(310)));
        assert_eq!(runtime.phase(), AutomationPhase::Idle);
        let snapshot = runtime.snapshot(
            "runtime",
            1,
            &AutoSymbols::default(),
            Duration::from_secs(310),
            MarketDayStatus::Open,
            None,
        );
        assert_eq!(
            snapshot.scenarios[0].status,
            crate::types::ScenarioStatus::Expired
        );
        assert_eq!(
            snapshot.decision_status,
            Some(AutomationDecisionStatus::Expired)
        );
    }

    #[test]
    fn 한쪽_무효화뒤_반대쪽이_만료되어도_부모는_무효화를_우선한다() {
        let mut runtime = AutomationRuntime::new(PersistedAutomation::default(), None);
        runtime.mode = ControlMode::Shadow;
        let revision = runtime.begin_analysis(1_000, Some(1_300)).unwrap();
        let decision = super::super::oco::validate_decision(
            185_000,
            &crate::types::ModelDecision {
                market_regime: MarketRegime::Transition,
                decision_summary_ko: "양방향 후보 중 한쪽 무효화".into(),
                scenarios: vec![
                    crate::types::ModelScenario {
                        product: ProductKind::Leverage,
                        setup_type: crate::types::SetupType::Continuation,
                        reference_price: 185_200,
                        confirmation_price: 185_400,
                        invalidation_price: 184_600,
                        target_return_pct: 0.3,
                        rationale_ko: "상단 돌파 대기".into(),
                    },
                    crate::types::ModelScenario {
                        product: ProductKind::Inverse,
                        setup_type: crate::types::SetupType::Reversal,
                        reference_price: 185_200,
                        confirmation_price: 184_800,
                        invalidation_price: 185_400,
                        target_return_pct: 0.3,
                        rationale_ko: "저항 반락 대기".into(),
                    },
                ],
            },
        )
        .unwrap();

        assert!(runtime.accept_decision(
            revision,
            22,
            "decision-22".into(),
            &decision,
            AutomationDecisionStatus::Armed,
            &[
                ScenarioSeed::armed(ProductKind::Leverage, None),
                ScenarioSeed::invalidated(ProductKind::Inverse, Some(990)),
            ],
            None,
            22,
            Duration::from_secs(10),
            Duration::from_secs(310),
            1_300,
        ));
        assert!(runtime
            .on_trade_tick(TradeTick {
                sequence: 1,
                price: 185_000,
                volume: 1,
                at: Duration::from_secs(310),
                epoch: 1_300,
            })
            .is_none());

        let snapshot = runtime.snapshot(
            "runtime",
            1,
            &AutoSymbols::default(),
            Duration::from_secs(310),
            MarketDayStatus::Open,
            None,
        );
        assert_eq!(
            snapshot.decision_status,
            Some(AutomationDecisionStatus::Invalidated)
        );
        assert!(snapshot
            .scenarios
            .iter()
            .any(|scenario| scenario.status == ScenarioStatus::Expired));
        assert!(snapshot
            .scenarios
            .iter()
            .any(|scenario| scenario.status == ScenarioStatus::Invalidated));
    }
}
