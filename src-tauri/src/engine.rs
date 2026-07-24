use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

use chrono::{Datelike, Weekday};
use sha2::{Digest, Sha256};
use tauri::{AppHandle, Emitter};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::automation::oco::{
    validate_decision, OcoGroup, ScenarioInvalidReason, ScenarioSeed, TradeTick, TriggeredScenario,
    ValidatedDecision, ValidatedScenario,
};
use crate::automation::openai::{
    serialized_dynamic_input, DecisionInput, MarketQuoteInput, OpenAiClient, TokenUsage, MODEL,
    PROMPT_VERSION,
};
use crate::automation::runtime::{
    scenario_terminal_reason_ko, AutomationRuntime, OwnedPosition, PendingAutoEntry,
    PersistedAutomation,
};
use crate::automation::schedule;
use crate::automation::shadow::{
    EntryOutcome as ShadowEntryOutcome, ExitReason as ShadowExitReason, ShadowBook,
    ShadowEntryExecutor, ShadowEntryRequest, ShadowExecutionError, ShadowOrder, ShadowOrderKind,
    ShadowOrderStatus, ShadowProduct, ShadowSession, ShadowSide, ShadowTradeTick,
};
use crate::broker::{Broker, BrokerShadowCashSource};
use crate::chart_image;
use crate::error::{AppError, AppResult};
use crate::kis::rest::is_rate_limit_error;
use crate::kis::KisBroker;
use crate::ledger::{
    BrokerOrderKey, Ledger, LedgerControlMode, LedgerDecisionStatus, LedgerExecutionKind,
    LedgerMarketRegime, LedgerOrderStatus, LedgerOrderType, LedgerOrigin, LedgerProductKind,
    LedgerScenarioStatus, LedgerSetupType, LedgerSide, LedgerTradeStatus, NewDecision,
    NewDecisionScenario, NewFill, NewFillNotice, NewOrderIntent, NewSession, NewTrade,
    OrderAcknowledgement, OrderRecord, ScenarioStatusUpdate,
};
use crate::market_history::MarketHistory;
use crate::types::{
    AccountSnapshot, AutomationDecisionStatus, AutomationSnapshot, Candle, ControlMode, FeedEvent,
    MarketDayStatus, MarketRegime, OrderResult, ProductKind, Quote, ReservationInfo, Settings,
    SetupType, Side,
};
use crate::util::{
    buy_limit_price, max_buy_qty, now_kst_fake_epoch, now_kst_fake_epoch_millis, sell_target_price,
};

/// 강제 손절 발동 후 같은 종목의 재발동을 막는 잠금 시간.
/// 정상 체결이면 그 전에 잔고 갱신(700ms)이 포지션을 지워 재발동이 없고,
/// 주문이 거부되면 이 간격만큼 쉬었다가 재시도해 초당 주문 폭주를 막는다.
const STOP_LOSS_RELOCK: std::time::Duration = std::time::Duration::from_secs(5);

/// 캐시가 이 초수보다 낡으면 주문 전에 REST 스냅샷으로 폴백
const QUOTE_FRESH_SECS: i64 = 10;
/// 스냅샷 실패 시 이 초수 이내의 캐시는 최후 수단으로 허용
const QUOTE_STALE_LIMIT_SECS: i64 = 60;
/// 자동매매 종목의 체결·호가 중 한쪽 스트림만 죽은(미수신) 상태가 이 시간 지속되면
/// 피드 감시견이 웹소켓 전체 재접속을 요청한다. 반대쪽 스트림이 신선할 때만
/// 판정하므로 시장이 조용한 것과 구독이 죽은 것을 혼동하지 않는다.
const FEED_HALF_DEAD_RECONNECT_SECS: i64 = 60;
/// 감시견 재접속 요청의 최소 간격 — 재접속 백필 버스트가 유량을 치지 않게 제한.
const FEED_WATCHDOG_MIN_GAP_SECS: i64 = 300;
const ACCOUNT_REFRESH_SECS: u64 = 30;
const AUTOMATION_STATE_KEY: &str = "automation_runtime_v1";
const AUTOMATION_BUNDLE_STATE_KEY: &str = "automation_runtime_bundle_v2";
const AUTO_HANDOFF_STATE_KEY: &str = "auto_handoff_v1";
const LATE_AUTO_ENTRY_WATCH_KEY: &str = "late_auto_entry_watch_v1";
const MARKET_DAY_STATE_KEY: &str = "market_day_state_v1";
/// 개장일 조회 실패는 분당 한 번만 재시도한다. KIS 호출을 버스트시키지 않으면서
/// 실시간 시세 폴백이 사라진 경우에도 장중 상태를 오래 막지 않는다.
const MARKET_DAY_RETRY_SECS: i64 = 60;
const MARKET_DAY_REQUEST_TIMEOUT_SECS: u64 = 60;
const AUTO_MAX_HOLD_SECS: i64 = 600;
/// 구버전이 EGW00201을 Unknown으로 저장한 주문은 조회 반영 여유를 둔 뒤에만
/// 잔고·체결·미체결 무변화를 함께 확인해 미접수로 마이그레이션한다.
const LEGACY_RATE_LIMIT_RECONCILE_SECS: i64 = 10;
/// REST 누적체결이 같은 값으로 유지돼야 WebSocket 통보 조정을 끝내는 최소 시간.
const FILL_RECONCILE_SETTLE_SECS: u64 = 2;
/// 식별 불명확/REST 장애 때 단일 작업이 무한 폴링하지 않도록 제한한다.
const FILL_RECONCILE_MAX_ATTEMPTS: u64 = 90;
/// LLM 입력 이후 본주 체결을 정확한 순서로 재생하기 위한 메모리 상한.
///
/// 5분 판단 한 회차에 이 수를 넘기면 오래된 틱을 추측하지 않고 해당 판단을
/// 안전하게 폐기한다.
const AUTOMATION_TRADE_JOURNAL_CAPACITY: usize = 50_000;
/// 같은 프로세스에서 재생성된 엔진의 스냅샷 순서를 프론트가 판별하는 단조 세대값.
static ENGINE_GENERATION_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("String 쓰기는 실패하지 않음");
    }
    encoded
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedAutomationBundle {
    automation: PersistedAutomation,
    shadow: Option<ShadowSession>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedMarketDayState {
    date: String,
    status: MarketDayStatus,
    message: Option<String>,
    last_attempt_at: i64,
}

fn is_transient_market_day_message(message: Option<&str>) -> bool {
    message.is_some_and(|message| message.contains("개장일 여부를 확인하고 있습니다"))
}

fn market_date_info(now_fake_epoch: i64) -> Option<(String, bool)> {
    let now = chrono::DateTime::from_timestamp(now_fake_epoch, 0)?.naive_utc();
    let weekend = matches!(now.weekday(), Weekday::Sat | Weekday::Sun);
    Some((now.format("%Y%m%d").to_string(), weekend))
}

fn initial_market_day_state(
    ledger: &Ledger,
    now: i64,
) -> Result<PersistedMarketDayState, AppError> {
    let (date, weekend) = market_date_info(now)
        .ok_or_else(|| AppError::Config("현재 KST 날짜를 계산할 수 없습니다".into()))?;
    if weekend {
        return Ok(PersistedMarketDayState {
            date,
            status: MarketDayStatus::Closed,
            message: Some("주말 휴장일입니다".into()),
            last_attempt_at: 0,
        });
    }
    let cached = ledger
        .get_runtime_state::<PersistedMarketDayState>(MARKET_DAY_STATE_KEY)
        .map_err(|error| AppError::Config(error.to_string()))?;
    let mut state = cached
        .filter(|state| state.date == date)
        .unwrap_or(PersistedMarketDayState {
            date,
            status: MarketDayStatus::Unknown,
            message: Some("개장일 여부를 확인하고 있습니다".into()),
            last_attempt_at: 0,
        });
    // OpenByQuotes는 현재 웹소켓으로만 파생하는 상태라 재시작 후 복원하지 않는다.
    // 구버전이 조회 직전 저장했던 "확인 중"도 완료된 실패로 보지 않고 즉시 재조회한다.
    if state.status == MarketDayStatus::OpenByQuotes
        || (state.status == MarketDayStatus::Unknown
            && state.last_attempt_at > 0
            && is_transient_market_day_message(state.message.as_deref()))
    {
        state.status = MarketDayStatus::Unknown;
        state.message = Some("개장일 여부를 다시 확인합니다".into());
        state.last_attempt_at = 0;
    }
    Ok(state)
}

/// Auto 소유 포지션을 Manual/Shadow로 넘기는 작업의 내구성 있는 복구 표식.
///
/// 목표 주문 취소 POST보다 먼저 저장한다. 그 뒤 프로세스가 종료되면 앱 시작 조정이
/// 피드·자동 청산 스케줄러를 띄우기 전에 같은 인계를 끝까지 재개한다.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PendingAutoHandoff {
    target_mode: ControlMode,
    requested_at: i64,
}

/// 15:15의 계좌·체결·미체결 조회가 비어도 바로 뒤늦게 도착할 수 있는 체결을
/// 수동 주문으로 오귀속하지 않도록 거래일 동안 보존하는 Auto 진입 tombstone.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LateAutoEntryWatch {
    pending: PendingAutoEntry,
    /// KIS 주문 소유권은 거래일·조직번호·주문번호를 모두 알아야 확정된다.
    /// 구 tombstone은 빈 값으로 역직렬화해 자동 복원하지 않고 안전하게 무시한다.
    #[serde(default)]
    trading_date: String,
    #[serde(default)]
    order_no: String,
    #[serde(default)]
    org_no: String,
    code: String,
    ordered_qty: u64,
    limit_price: Option<u64>,
    requested_at: i64,
}

/// 프론트로 이벤트를 내보내는 콜백. Engine이 tauri 타입을 직접 들지 않게 분리한다 —
/// tauri 심볼을 링크하면 매니페스트 없는 단위 테스트 exe가 comctl32 v6 로드 실패로 죽는다.
type EmitFn = Box<dyn Fn(&str, serde_json::Value) + Send + Sync>;

/// 걸어둔 예약 매도(지정가). 원주문번호/조직번호는 취소에 필요한 내부 식별자로 프론트에는 안 보낸다.
#[derive(Clone)]
struct Reservation {
    target_pct: f64,
    target_price: u64,
    qty: u64,
    order_no: String,
    org_no: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManualTradeAccumulator {
    trade_id: String,
    code: String,
    #[serde(default = "default_manual_origin")]
    origin: LedgerOrigin,
    entry_qty: u64,
    entry_value: f64,
    exit_qty: u64,
    exit_value: f64,
    entered_at: i64,
}

fn default_manual_origin() -> LedgerOrigin {
    LedgerOrigin::Manual
}

impl ManualTradeAccumulator {
    fn remaining(&self) -> u64 {
        self.entry_qty.saturating_sub(self.exit_qty)
    }
}

/// 주문별 WebSocket 통보와 REST 누적체결 사이의 지연을 흡수한다.
/// 경제 체결수량은 통보 수량을 더하지 않고 정확한 REST 누적값만 신뢰한다.
struct FillReconcileState {
    generation: u64,
    running: bool,
    trading_date: String,
    order_no: String,
    /// WebSocket에 없는 조직번호는 ACK 또는 REST의 유일 후보로 결합한 뒤 고정한다.
    org_no: Option<String>,
    code: String,
    side: Side,
}

/// LLM에 전달한 시세 스냅샷과 이후 체결 재생의 정확한 경계.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AutomationMarketMarker {
    feed_generation: u64,
    trade_cursor: u64,
    trade_sequence: u64,
    /// 마지막 연결/시세 공백 뒤 첫 journal 위치. 재연결 분의 bucket 시작시각이
    /// reset보다 이르더라도 실제 reset 이후 R 시험만 복원하는 데 사용한다.
    reset_cursor: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct JournalTrade {
    cursor: u64,
    tick: TradeTick,
}

#[derive(Debug, Default)]
struct AutomationTradeJournal {
    next_cursor: u64,
    dropped_through: u64,
    reset_cursor: Option<u64>,
    trades: VecDeque<JournalTrade>,
}

impl AutomationTradeJournal {
    fn marker(&self, feed_generation: u64, trade_sequence: u64) -> AutomationMarketMarker {
        AutomationMarketMarker {
            feed_generation,
            trade_cursor: self.next_cursor,
            trade_sequence,
            reset_cursor: self.reset_cursor,
        }
    }

    fn mark_reset(&mut self) {
        self.reset_cursor = Some(self.next_cursor);
    }

    fn push(&mut self, tick: TradeTick) {
        self.next_cursor = self.next_cursor.saturating_add(1);
        self.trades.push_back(JournalTrade {
            cursor: self.next_cursor,
            tick,
        });
        while self.trades.len() > AUTOMATION_TRADE_JOURNAL_CAPACITY {
            if let Some(dropped) = self.trades.pop_front() {
                self.dropped_through = dropped.cursor;
            }
        }
    }

    fn after(&self, marker: AutomationMarketMarker) -> Result<Vec<TradeTick>, String> {
        if marker.trade_cursor < self.dropped_through {
            return Err("LLM 응답 대기 중 본주 체결이 재생 버퍼를 초과해 판단을 폐기합니다".into());
        }
        let mut last_sequence = (marker.trade_sequence > 0).then_some(marker.trade_sequence);
        Ok(self
            .trades
            .iter()
            .filter(|trade| trade.cursor > marker.trade_cursor)
            .filter_map(|trade| {
                let tick = trade.tick;
                if tick.volume == 0
                    || tick.price == 0
                    || last_sequence.is_some_and(|sequence| tick.sequence <= sequence)
                {
                    return None;
                }
                last_sequence = Some(tick.sequence);
                Some(tick)
            })
            .collect())
    }

    /// 마지막 reset보다 뒤이면서 입력 marker까지 처리된 정확한 체결만 반환한다.
    /// 거래소 체결 순번은 재연결·거래일 경계에서 다시 시작할 수 있어 reset 이전
    /// marker 순번을 기준으로 필터링하지 않는다.
    fn since_reset_through(
        &self,
        marker: AutomationMarketMarker,
    ) -> Result<Vec<TradeTick>, String> {
        let Some(reset_cursor) = marker.reset_cursor else {
            return Ok(Vec::new());
        };
        if reset_cursor < self.dropped_through {
            return Err(
                "재연결 이후 기준선 시험 체결이 재생 버퍼를 초과해 판단을 폐기합니다".into(),
            );
        }
        let mut last_sequence = None;
        Ok(self
            .trades
            .iter()
            .filter(|trade| trade.cursor > reset_cursor && trade.cursor <= marker.trade_cursor)
            .filter_map(|trade| {
                let tick = trade.tick;
                if tick.volume == 0
                    || tick.price == 0
                    || last_sequence.is_some_and(|sequence| tick.sequence <= sequence)
                {
                    return None;
                }
                last_sequence = Some(tick.sequence);
                Some(tick)
            })
            .collect())
    }
}

/// 예약 상태를 프론트 emit/조회용 직렬화 타입으로 변환
fn reservation_info(
    code: &str,
    r: &Reservation,
    status: &str,
    reason: Option<String>,
) -> ReservationInfo {
    ReservationInfo {
        code: code.to_string(),
        target_pct: r.target_pct,
        target_price: r.target_price,
        qty: r.qty,
        status: status.to_string(),
        reason,
    }
}

pub struct Engine {
    /// EngineHandle이 폐기되면 false가 된다. 추적되지 않은 자식 태스크도 외부 호출
    /// 응답을 장부나 최신 런타임에 적용하기 전에 이 값을 다시 확인한다.
    active: AtomicBool,
    /// 동기 장부 저장과 엔진 폐기를 직렬화한다. drop이 반환된 뒤에는 이전 엔진의
    /// 장부 저장이 새로 시작될 수 없다.
    retirement_gate: Mutex<()>,
    /// 엔진 재시작 때 revision이 0으로 돌아가도 프론트가 새 인스턴스를 구분한다.
    runtime_id: String,
    /// runtime_id가 다른 스냅샷끼리도 도착 순서를 비교할 수 있는 프로세스 단조 세대값.
    runtime_generation: u64,
    /// UI 전용 필드(테마·차트 주기 등)는 엔진 재시작 없이 갱신되므로 RwLock으로 보관
    settings: RwLock<Settings>,
    broker: Arc<dyn Broker>,
    /// 주문 메서드가 노출되지 않는 capability만 가진 섀도 진입 실행기.
    shadow_entry_executor: ShadowEntryExecutor,
    quotes: RwLock<HashMap<String, Quote>>,
    account: RwLock<AccountSnapshot>,
    /// 걸어둔 예약 매도 (code -> Reservation). 코드당 최대 1건.
    /// 엔진 메모리에만 존재 — 엔진 재시작 시 유실(거래소 실주문은 남음)
    reservations: RwLock<HashMap<String, Reservation>>,
    /// 강제 손절 재발동 잠금 (code -> 이 시각 전에는 재발동 금지).
    /// 발동 시 STOP_LOSS_RELOCK만큼 잠가 연속 틱의 중복 매도를 막는다.
    /// 포지션 청산/재매수 시 즉시 해제된다.
    stop_loss_lock: Mutex<HashMap<String, std::time::Instant>>,
    market_history: MarketHistory,
    /// 모드·OCO·포지션 소유권은 한 잠금 안에서만 전환한다.
    automation: Mutex<AutomationRuntime>,
    /// 피드의 시세·연결 변경과 LLM의 최종 OCO 무장을 직렬화한다.
    ///
    /// 응답 적용 검사와 실제 무장 사이에 재연결 또는 새 C/I 체결이 끼어드는
    /// TOCTOU를 막기 위한 잠금이며, 외부 REST 호출은 이 잠금 안에서 하지 않는다.
    automation_market_gate: tokio::sync::Mutex<()>,
    /// LLM 입력 직후부터 최종 무장 직전까지의 본주 체결을 순서대로 보존한다.
    automation_trade_journal: Mutex<AutomationTradeJournal>,
    /// 실제 피드와 같은 FIFO에 LLM 적용 배리어를 넣기 위한 송신 핸들.
    automation_feed_tx: Mutex<Option<mpsc::Sender<FeedEvent>>>,
    market_day: RwLock<PersistedMarketDayState>,
    /// 취소·panic 때 자동으로 해제되는 개장일 조회 단일 비행 잠금.
    market_day_refresh_gate: tokio::sync::Mutex<()>,
    shadow: Mutex<Option<ShadowSession>>,
    ledger: Arc<Ledger>,
    /// 장부에 반영한 시나리오 상태가 실제로 바뀐 경우에만 이력 갱신 이벤트를 보낸다.
    scenario_history_fingerprint: Mutex<Option<String>>,
    /// 수동·자동 주문 POST를 하나의 actor 임계구역으로 직렬화한다.
    order_actor: tokio::sync::Mutex<()>,
    /// KIS REST 누적체결의 조회→delta 기록을 프로세스 안에서 원자화한다.
    fill_ingest_lock: Mutex<()>,
    /// 같은 정확 주문의 연속 WebSocket 알림을 하나의 REST 조정 작업으로 합친다.
    /// 통보 수량은 신뢰하지 않고 거래일·조직번호·주문번호가 맞는 REST 누적값만 반영한다.
    fill_reconcile_pending: Mutex<HashMap<String, FillReconcileState>>,
    trade_sequence: AtomicU64,
    connected: AtomicBool,
    /// 최초 정상 연결은 과거 15봉 seed를 허용하고, 그 이후 연결만 재연결로 구분한다.
    automation_feed_seen_connection: AtomicBool,
    /// WebSocket 재연결·시세 공백마다 증가한다. 분석 중 값이 바뀌면 응답을 폐기한다.
    automation_feed_generation: AtomicU64,
    /// 반전 seed는 이 시각 이후에 새로 관측된 기준선 시험만 인정한다.
    automation_feed_reset_epoch: AtomicI64,
    /// 피드 감시견이 WS 태스크에 전체 재접속을 요청하는 신호.
    feed_reconnect: Arc<tokio::sync::Notify>,
    /// 한쪽 스트림만 죽은 상태가 처음 관측된 fake epoch. 0 = 정상.
    feed_half_dead_since: AtomicI64,
    /// 감시견이 마지막으로 재접속을 요청한 fake epoch.
    feed_watchdog_last_fired: AtomicI64,
    auto_flatten_pending: AtomicBool,
    entry_reconcile_pending: AtomicBool,
    exit_reconcile_pending: AtomicBool,
    last_flatten_day: AtomicI64,
    last_error: Mutex<String>,
    /// 지연 잔고 갱신이 이미 예약되어 있으면 true — 연속 체결통보를 1회 갱신으로 합류
    refresh_pending: AtomicBool,
    /// 잔고를 바꾼 사건(주문 접수/체결통보)마다 +1 — 캐시 최신성 판정용
    account_gen: AtomicU64,
    /// 마지막 성공 갱신이 반영한 account_gen 값. account_gen보다 작으면 캐시가 낡은 것
    account_refreshed_gen: AtomicU64,
    /// 자동 진입 시각 경계 테스트를 장중 시각으로 고정하기 위한 주입값.
    #[cfg(test)]
    automation_now_override: AtomicI64,
    /// 테스트에서는 None (이벤트 emit 생략)
    emit_fn: Option<EmitFn>,
    /// 지연 잔고 갱신 태스크에서 자기 자신을 참조하기 위한 약한 포인터
    weak: Weak<Engine>,
}

/// 엔진 + 백그라운드 태스크 핸들. drop 시 태스크 전부 중단.
pub struct EngineHandle {
    pub engine: Arc<Engine>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for EngineHandle {
    fn drop(&mut self) {
        self.engine.deactivate();
        for t in &self.tasks {
            t.abort();
        }
    }
}

fn active_open_order_summary(orders: &[crate::broker::BrokerOpenOrder]) -> Option<String> {
    let active: Vec<_> = orders
        .iter()
        .filter(|order| order.cancelable_qty > 0 || order.filled_qty < order.ordered_qty)
        .collect();
    if active.is_empty() {
        return None;
    }
    let examples = active
        .iter()
        .take(3)
        .map(|order| {
            let remaining = order
                .cancelable_qty
                .max(order.ordered_qty.saturating_sub(order.filled_qty));
            format!(
                "{} {}/{} {}주",
                order.code, order.org_no, order.order_no, remaining
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!(
        "KIS 미체결 주문 {}건이 남아 있어 초기화하지 않았습니다. 먼저 주문을 취소·체결한 뒤 다시 시도하세요: {examples}",
        active.len()
    ))
}

/// 강제 재동기화는 과거 장부의 비종결 표식 대신 현재 KIS 계좌를 원장으로 삼는다.
/// 그 전에 잔고·미체결을 두 번 연속 조회해 실제로 체결 가능한 주문이 남아 있지
/// 않은지 확인한다. 조회 실패나 미체결 존재는 모두 초기화 거부로 처리한다.
async fn verify_runtime_resync_safe(broker: &dyn Broker) -> Result<AccountSnapshot, String> {
    let _ = broker
        .account()
        .await
        .map_err(|error| format!("초기화 전 KIS 잔고 조회 실패: {error}"))?;
    let first_orders = broker
        .open_orders()
        .await
        .map_err(|error| format!("초기화 전 KIS 미체결 조회 실패: {error}"))?;
    if let Some(message) = active_open_order_summary(&first_orders) {
        return Err(message);
    }

    let account = broker
        .account()
        .await
        .map_err(|error| format!("초기화 직전 KIS 최종 잔고 조회 실패: {error}"))?;
    let final_orders = broker
        .open_orders()
        .await
        .map_err(|error| format!("초기화 직전 KIS 최종 미체결 조회 실패: {error}"))?;
    if let Some(message) = active_open_order_summary(&final_orders) {
        return Err(message);
    }
    Ok(account)
}

/// 저장된 자동 인계 상태 때문에 엔진 시작 자체가 실패한 경우의 복구 경로.
/// 임시 브로커는 읽기 조회만 수행하며 주문 POST를 노출하지 않는다.
pub async fn reset_runtime_without_engine(
    settings: &Settings,
    ledger: &Ledger,
) -> Result<(), String> {
    let broker = KisBroker::new(settings).map_err(|error| error.to_string())?;
    verify_runtime_resync_safe(&broker).await?;
    ledger
        .reset_runtime_state_to_manual(now_kst_fake_epoch())
        .map_err(|error| format!("런타임 상태 초기화 실패: {error}"))
}

pub async fn start(
    app: AppHandle,
    settings: Settings,
    ledger: Arc<Ledger>,
) -> AppResult<EngineHandle> {
    let broker: Arc<dyn Broker> = Arc::new(KisBroker::new(&settings)?);
    let shadow_entry_executor =
        ShadowEntryExecutor::new(Arc::new(BrokerShadowCashSource::new(Arc::clone(&broker))));

    let now = now_kst_fake_epoch();
    let closed_oco_count = ledger
        .close_unrestorable_oco_decisions(now)
        .map_err(|error| {
            crate::error::AppError::Config(format!(
                "재시작 시 활성 LLM 판단 장부 종결 실패: {error}"
            ))
        })?;
    if closed_oco_count > 0 {
        tracing::warn!(
            "재시작으로 복원할 수 없는 활성 LLM OCO {closed_oco_count}건을 장부에서 종결했습니다"
        );
    }
    let market_day = initial_market_day_state(&ledger, now)?;
    let bundle = ledger
        .get_runtime_state::<PersistedAutomationBundle>(AUTOMATION_BUNDLE_STATE_KEY)
        .map_err(|e| crate::error::AppError::Config(e.to_string()))?;
    let (mut saved, mut saved_shadow) = if let Some(bundle) = bundle {
        (bundle.automation, bundle.shadow)
    } else {
        (
            ledger
                .get_runtime_state::<PersistedAutomation>(AUTOMATION_STATE_KEY)
                .map_err(|e| crate::error::AppError::Config(e.to_string()))?
                .unwrap_or_default(),
            None,
        )
    };
    if saved.session_id.is_none() {
        if let Some(mode) = ledger
            .get_control_mode()
            .map_err(|e| crate::error::AppError::Config(e.to_string()))?
        {
            saved.mode = control_mode_from_ledger(mode);
        }
    }
    if saved.mode == ControlMode::Shadow {
        if let Some(session) = &mut saved_shadow {
            session.prepare_for_restart();
        }
    } else {
        saved_shadow = None;
    }
    let restored_decision_slot = (market_day.status == MarketDayStatus::Open)
        .then(|| {
            schedule::recover_persisted_decision_slot(
                saved.next_decision_at,
                saved.last_decision_slot,
                now,
            )
        })
        .flatten();
    let runtime = AutomationRuntime::new(saved, restored_decision_slot);

    let engine = Arc::new_cyclic(|weak| Engine {
        active: AtomicBool::new(true),
        retirement_gate: Mutex::new(()),
        runtime_id: unique_id("engine"),
        runtime_generation: ENGINE_GENERATION_SEQUENCE.fetch_add(1, Ordering::Relaxed),
        settings: RwLock::new(settings.clone()),
        broker: Arc::clone(&broker),
        shadow_entry_executor,
        quotes: RwLock::new(HashMap::new()),
        account: RwLock::new(AccountSnapshot {
            cash: 0,
            positions: Vec::new(),
        }),
        reservations: RwLock::new(HashMap::new()),
        stop_loss_lock: Mutex::new(HashMap::new()),
        market_history: MarketHistory::new(),
        automation: Mutex::new(runtime),
        automation_market_gate: tokio::sync::Mutex::new(()),
        automation_trade_journal: Mutex::new(AutomationTradeJournal::default()),
        automation_feed_tx: Mutex::new(None),
        market_day: RwLock::new(market_day),
        market_day_refresh_gate: tokio::sync::Mutex::new(()),
        shadow: Mutex::new(saved_shadow),
        ledger: Arc::clone(&ledger),
        scenario_history_fingerprint: Mutex::new(None),
        order_actor: tokio::sync::Mutex::new(()),
        fill_ingest_lock: Mutex::new(()),
        fill_reconcile_pending: Mutex::new(HashMap::new()),
        trade_sequence: AtomicU64::new(0),
        connected: AtomicBool::new(false),
        automation_feed_seen_connection: AtomicBool::new(false),
        automation_feed_generation: AtomicU64::new(0),
        automation_feed_reset_epoch: AtomicI64::new(0),
        feed_reconnect: Arc::new(tokio::sync::Notify::new()),
        feed_half_dead_since: AtomicI64::new(0),
        feed_watchdog_last_fired: AtomicI64::new(0),
        auto_flatten_pending: AtomicBool::new(false),
        entry_reconcile_pending: AtomicBool::new(false),
        exit_reconcile_pending: AtomicBool::new(false),
        last_flatten_day: AtomicI64::new(-1),
        last_error: Mutex::new(String::new()),
        refresh_pending: AtomicBool::new(false),
        // refreshed=0보다 크게 시작해 첫 주문 전 계좌 조회를 강제한다.
        account_gen: AtomicU64::new(1),
        account_refreshed_gen: AtomicU64::new(0),
        #[cfg(test)]
        automation_now_override: AtomicI64::new(0),
        emit_fn: Some(Box::new(move |event, payload| {
            let _ = app.emit(event, payload);
        })),
        weak: weak.clone(),
    });

    engine.reconcile_startup().await;
    // Auto→Manual/Shadow 인계 도중 종료됐다면 피드와 15:15 자동 청산 태스크를
    // 시작하기 전에 목표 주문 종결과 소유권 이전부터 완료한다.
    engine
        .resume_pending_auto_handoff()
        .await
        .map_err(|error| AppError::Config(format!("저장된 Auto 인계 복구 실패: {error}")))?;
    if engine.automation.lock().unwrap().pending_entry().is_some() {
        engine.start_pending_entry_reconcile();
    }
    engine.emit_automation_state();

    // 재시작 전에 제출된 청산 주문은 같은 POST를 다시 보내지 않고 저장된
    // 주문번호/요청시각으로 체결·미체결·계좌 조정부터 재개한다.
    let exit_recovery = {
        let runtime = engine.automation.lock().unwrap();
        (runtime.mode() == ControlMode::Auto
            && runtime.phase() == crate::types::AutomationPhase::ExitPending)
            .then(|| runtime.position().cloned())
            .flatten()
            .map(|position| {
                let reason = match position.pending_exit_reason.as_deref() {
                    Some("market_close") => "market_close",
                    Some("stop_loss") => "stop_loss",
                    Some("profit_guard") => "profit_guard",
                    Some("max_holding") => "max_holding",
                    Some("target_order_failed") => "target_order_failed",
                    _ => "startup_reconcile",
                };
                (
                    reason,
                    position.exit_order_no,
                    position.exit_intent_id,
                    position.exit_requested_at,
                )
            })
    };
    if let Some((reason, order_no, intent_id, requested_at)) = exit_recovery {
        if let Some(order_no) = order_no {
            engine.start_active_exit_reconcile(reason, order_no);
        } else if let (Some(intent_id), Some(requested_at)) = (intent_id, requested_at) {
            engine.start_unknown_exit_reconcile(reason, intent_id, requested_at);
        } else {
            let recovery_engine = Arc::clone(&engine);
            tokio::spawn(async move { recovery_engine.execute_auto_exit(reason).await });
        }
    }

    let (tx, rx) = mpsc::channel::<FeedEvent>(512);
    *engine.automation_feed_tx.lock().unwrap() = Some(tx.clone());
    let mut tasks = broker
        .start_feed(
            settings.all_codes(),
            tx,
            Arc::clone(&engine.feed_reconnect),
        )
        .await?;

    tasks.push(tokio::spawn(consume_feed(Arc::clone(&engine), rx)));
    tasks.push(tokio::spawn(periodic_refresh(Arc::clone(&engine))));
    tasks.push(tokio::spawn(automation_scheduler(Arc::clone(&engine))));
    tasks.push(tokio::spawn(market_day_scheduler(Arc::clone(&engine))));

    // 차트 종목을 순차로 미리 백필해 셀렉터 전환이 즉시 되게 한다.
    // 프론트가 같은 종목을 요청하면 단일 비행으로 합쳐지므로 중복 호출은 없다.
    let warm_engine = Arc::clone(&engine);
    let warm_codes: Vec<String> = settings
        .chart_symbols
        .iter()
        .map(|s| s.code.clone())
        .collect();
    let mut seed_codes: Vec<String> = settings
        .trade_symbols
        .iter()
        .map(|s| s.code.clone())
        .collect();
    // 자동매매 3종목(특히 trade_symbols에 없는 본주)은 신선도 검사가 현재가·호가를
    // 요구하므로 같이 시드한다.
    seed_codes.extend([
        settings.auto_symbols.underlying.clone(),
        settings.auto_symbols.leverage.clone(),
        settings.auto_symbols.inverse.clone(),
    ]);
    seed_codes.sort();
    seed_codes.dedup();
    tasks.push(tokio::spawn(async move {
        // 매매 종목 시세 1회 시드: 첫 틱 도착 전에도 수익률 기준가가 있고,
        // 기준 Quote 부재로 Book 이벤트가 버려지는 공백도 없앤다 (폴링 아님)
        for code in &seed_codes {
            warm_engine.fresh_quote(code).await;
        }
        for code in warm_codes {
            if let Err(e) = warm_engine.candles(&code).await {
                tracing::warn!("차트 워밍업 실패({code}): {e}");
            }
        }
    }));

    Ok(EngineHandle { engine, tasks })
}

async fn consume_feed(engine: Arc<Engine>, mut rx: mpsc::Receiver<FeedEvent>) {
    // 실가동 검증용: 종목별 첫 틱·첫 호가 로그로 구독이 실제 시세를 내려주는지 즉시 판별
    let mut first_tick_seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut first_book_seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    while let Some(ev) = rx.recv().await {
        match ev {
            FeedEvent::Quote(mut q) => {
                let _market_guard = engine.automation_market_gate.lock().await;
                if first_tick_seen.insert(q.code.clone()) {
                    tracing::info!("실시간 체결가 첫 수신: {}", q.code);
                }
                let trade_gap = {
                    let mut quotes = engine.quotes.write().unwrap();
                    let mut trade_gap = false;
                    if let Some(previous) = quotes.get(&q.code) {
                        trade_gap = previous.trade_ts > 0
                            && q.trade_ts.saturating_sub(previous.trade_ts) > QUOTE_FRESH_SECS;
                        q.ask1_qty = previous.ask1_qty;
                        q.bid1_qty = previous.bid1_qty;
                        q.book_ts = previous.book_ts;
                        if q.ask1 <= 0.0 {
                            q.ask1 = previous.ask1;
                        }
                        if q.bid1 <= 0.0 {
                            q.bid1 = previous.bid1;
                        }
                    }
                    quotes.insert(q.code.clone(), q.clone());
                    trade_gap
                };
                if trade_gap {
                    engine.reset_confirmation_for_market_gap(&q.code, "체결");
                }
                engine.market_history.apply_trade(&q).await;
                let underlying_code = engine
                    .settings
                    .read()
                    .unwrap()
                    .auto_symbols
                    .underlying
                    .clone();
                if q.code == underlying_code
                    && q.volume > 0.0
                    && q.price > 0.0
                    && q.trade_sequence > 0
                    && q.received_at_micros > 0
                {
                    engine
                        .automation_trade_journal
                        .lock()
                        .unwrap()
                        .push(TradeTick {
                            sequence: q.trade_sequence,
                            price: q.price.round() as u64,
                            volume: q.volume.max(0.0).round() as u64,
                            at: Duration::from_micros(q.received_at_micros),
                            epoch: q.trade_ts,
                        });
                }
                engine.emit("quote", &q);
                engine.handle_automation_quote(&q);
                // 실시간 체결가 기준 수익률이 강제 손절선(-0.6%) 이하면 전량 시장가 매도.
                // 등록까지 원자적이라 연속 틱이 중복 매도를 내지 않는다.
                if !engine.automation.lock().unwrap().owns_real_code(&q.code)
                    && engine.arm_stop_loss_if_breached(&q.code, q.price)
                {
                    let engine = Arc::clone(&engine);
                    let code = q.code.clone();
                    tokio::spawn(async move { engine.force_stop_loss(&code).await });
                }
            }
            FeedEvent::Book {
                code,
                ask1,
                bid1,
                ask1_qty,
                bid1_qty,
                ts,
            } => {
                let _market_guard = engine.automation_market_gate.lock().await;
                if first_book_seen.insert(code.clone()) {
                    tracing::info!("실시간 호가 첫 수신: {}", code);
                }
                let (merged, book_gap) = {
                    let mut map = engine.quotes.write().unwrap();
                    // 첫 체결보다 호가가 먼저 도착해도 book_ts를 잃지 않도록
                    // 엔트리를 만들어 병합한다. 체결 전(가격 0)에는 캐시만 채우고
                    // 프론트·자동매매로는 내보내지 않는다.
                    let q = map.entry(code.clone()).or_insert_with(|| Quote {
                        code: code.clone(),
                        price: 0.0,
                        change_rate: 0.0,
                        ask1: 0.0,
                        bid1: 0.0,
                        ask1_qty: 0,
                        bid1_qty: 0,
                        volume: 0.0,
                        trade_sequence: 0,
                        received_at_micros: 0,
                        trade_ts: 0,
                        book_ts: 0,
                    });
                    let book_gap =
                        q.book_ts > 0 && ts.saturating_sub(q.book_ts) > QUOTE_FRESH_SECS;
                    q.ask1 = ask1;
                    q.bid1 = bid1;
                    q.ask1_qty = ask1_qty;
                    q.bid1_qty = bid1_qty;
                    q.book_ts = ts;
                    // 캐시에는 마지막 체결량을 남겨 LLM 입력에 전달하고, 호가
                    // 이벤트 복사본만 volume=0으로 표시해 차트 중복 반영을 막는다.
                    let merged = (q.price > 0.0).then(|| {
                        let mut emitted = q.clone();
                        emitted.volume = 0.0;
                        emitted
                    });
                    (merged, book_gap)
                };
                if book_gap {
                    engine.reset_confirmation_for_market_gap(&code, "호가");
                }
                if let Some(q) = merged {
                    engine.emit("quote", &q);
                    engine.handle_automation_book(&q);
                }
            }
            FeedEvent::Fill(fill) => {
                // WebSocket 체결은 순서가 뒤바뀌거나 REST 누적체결과 식별자가 달라질 수
                // 있으므로 경제 장부에 직접 반영하지 않는다. 주문 소유권을 먼저 결합한
                // 뒤 authoritative REST 누적체결 조정만 깨운다.
                engine.mark_account_stale();
                let restored_late_entry = engine.restore_late_auto_entry_notice(&fill);
                engine.claim_pending_auto_sell_notice(&fill);
                engine.start_fill_notice_reconcile(fill.clone());
                if restored_late_entry {
                    engine.start_pending_entry_reconcile();
                }
                engine.emit("fill", &fill);
                // 부분체결이 연발해도 잔고 조회는 1회로 합류 → 리미터 큐를 채우지 않는다
                engine.schedule_account_refresh();
            }
            FeedEvent::Conn(connected) => {
                let _market_guard = engine.automation_market_gate.lock().await;
                engine.connected.store(connected, Ordering::SeqCst);
                let seen_connection = if connected {
                    engine
                        .automation_feed_seen_connection
                        .swap(true, Ordering::SeqCst)
                } else {
                    engine
                        .automation_feed_seen_connection
                        .load(Ordering::SeqCst)
                };
                if seen_connection {
                    engine.mark_automation_feed_reset();
                    engine.invalidate_automation_quote_freshness();
                }
                let now = engine.monotonic_now();
                if engine.automation.lock().unwrap().reset_confirmation(now) {
                    engine.sync_scenario_ledger();
                    engine.emit_automation_state();
                }
                if !connected {
                    engine.market_history.mark_all_gapped().await;
                }
                engine.emit("conn", &serde_json::json!({ "connected": connected }));
            }
            FeedEvent::AutomationBarrier(ack) => {
                // 단일 consumer가 이 지점에 도달했다면 같은 채널에서 앞선 모든
                // 시세·연결 이벤트의 캐시/journal 반영이 완료된 상태다.
                let _ = ack.send(());
            }
        }
    }
}

async fn periodic_refresh(engine: Arc<Engine>) {
    // 시작 조정이 계좌·당일체결을 이미 조회하므로 첫 주기 호출은 30초 뒤로 미뤄
    // 엔진 시작 순간의 KIS GET 버스트를 피한다.
    let period = Duration::from_secs(ACCOUNT_REFRESH_SECS);
    let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        timer.tick().await;
        periodic_refresh_once(&engine).await;
    }
}

/// 30초 주기 작업 한 회차. 계좌와 당일 누적체결을 같은 회차에서 조정하므로
/// WebSocket/HTS ID 통보가 없는 Manual 체결도 거래일 안에 영속 장부에 반영된다.
async fn periodic_refresh_once(engine: &Arc<Engine>) {
    let account_ok = engine.refresh_account().await;
    let mut today_fills = match engine.broker.today_fills().await {
        Ok(fills) => fills,
        Err(error) => {
            tracing::warn!("30초 당일 체결 장부 조정 실패: {error}");
            Vec::new()
        }
    };
    let restored_from_fills = engine.apply_today_fill_snapshot(&mut today_fills);
    // 위 조회 결과를 tombstone exact 조정에도 재사용해 같은 회차의 당일체결
    // REST 호출을 한 번으로 제한한다. 조회 실패는 다음 30초 회차에서 다시 본다.
    let restored_from_exact = account_ok
        && engine
            .reconcile_late_auto_entry_watch_from_rest(Some(&today_fills))
            .await;
    if restored_from_fills || restored_from_exact {
        engine.start_pending_entry_reconcile();
    }
    if account_ok {
        engine.suspend_idle_unknown_auto_exposure().await;
    }
}

async fn automation_scheduler(engine: Arc<Engine>) {
    let mut timer = tokio::time::interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        timer.tick().await;
        engine.automation_tick().await;
    }
}

async fn market_day_scheduler(engine: Arc<Engine>) {
    let mut timer = tokio::time::interval(Duration::from_secs(60));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        timer.tick().await;
        engine.refresh_market_day_if_needed().await;
    }
}

fn ledger_control_mode(mode: ControlMode) -> LedgerControlMode {
    match mode {
        ControlMode::Manual => LedgerControlMode::Manual,
        ControlMode::Auto => LedgerControlMode::Auto,
        ControlMode::Shadow => LedgerControlMode::Shadow,
    }
}

fn control_mode_from_ledger(mode: LedgerControlMode) -> ControlMode {
    match mode {
        LedgerControlMode::Manual => ControlMode::Manual,
        LedgerControlMode::Auto => ControlMode::Auto,
        LedgerControlMode::Shadow => ControlMode::Shadow,
    }
}

fn ledger_product(product: ProductKind) -> LedgerProductKind {
    match product {
        ProductKind::Leverage => LedgerProductKind::Leverage,
        ProductKind::Inverse => LedgerProductKind::Inverse,
    }
}

fn ledger_setup_type(setup_type: SetupType) -> LedgerSetupType {
    match setup_type {
        SetupType::Continuation => LedgerSetupType::Continuation,
        SetupType::Reversal => LedgerSetupType::Reversal,
    }
}

fn ledger_market_regime(regime: MarketRegime) -> LedgerMarketRegime {
    match regime {
        MarketRegime::Uptrend => LedgerMarketRegime::Uptrend,
        MarketRegime::Downtrend => LedgerMarketRegime::Downtrend,
        MarketRegime::Range => LedgerMarketRegime::Range,
        MarketRegime::Transition => LedgerMarketRegime::Transition,
        MarketRegime::Unclear => LedgerMarketRegime::Unclear,
    }
}

fn automation_decision_status(status: LedgerDecisionStatus) -> AutomationDecisionStatus {
    match status {
        LedgerDecisionStatus::Armed => AutomationDecisionStatus::Armed,
        LedgerDecisionStatus::Skipped => AutomationDecisionStatus::Skipped,
        LedgerDecisionStatus::Triggered => AutomationDecisionStatus::Triggered,
        LedgerDecisionStatus::Expired => AutomationDecisionStatus::Expired,
        LedgerDecisionStatus::Replaced => AutomationDecisionStatus::Replaced,
        LedgerDecisionStatus::Missed => AutomationDecisionStatus::Missed,
        LedgerDecisionStatus::Invalidated => AutomationDecisionStatus::Invalidated,
        LedgerDecisionStatus::Invalid => AutomationDecisionStatus::Invalid,
        LedgerDecisionStatus::Error => AutomationDecisionStatus::Error,
        LedgerDecisionStatus::Discarded => AutomationDecisionStatus::Discarded,
    }
}

fn shadow_product(product: ProductKind) -> ShadowProduct {
    match product {
        ProductKind::Leverage => ShadowProduct::Leverage,
        ProductKind::Inverse => ShadowProduct::Inverse,
    }
}

fn shadow_exit_reason(reason: ShadowExitReason) -> &'static str {
    match reason {
        ShadowExitReason::TargetReached => "target",
        ShadowExitReason::ModeExit => "mode_exit",
        ShadowExitReason::MaxHolding => "max_holding",
        ShadowExitReason::ProfitGuard => "profit_guard",
        ShadowExitReason::StopLoss => "stop_loss",
        ShadowExitReason::MarketClose => "market_close",
    }
}

fn auto_exit_reason(reason: &str) -> Option<&'static str> {
    match reason {
        "market_close" => Some("market_close"),
        "stop_loss" => Some("stop_loss"),
        "profit_guard" => Some("profit_guard"),
        "max_holding" => Some("max_holding"),
        "target_order_failed" => Some("target_order_failed"),
        "startup_reconcile" => Some("startup_reconcile"),
        _ => None,
    }
}

fn shadow_order_key(trade_id: &str, kind: ShadowOrderKind, order_id: u64) -> String {
    let kind = match kind {
        ShadowOrderKind::EntryIoc => "entry",
        ShadowOrderKind::Target => "target",
        ShadowOrderKind::ForcedExit(_) => "forced",
    };
    format!("shadow:{trade_id}:{kind}:{order_id}")
}

fn shadow_intent_for_order(position: &OwnedPosition, order: &ShadowOrder) -> String {
    if order.kind == ShadowOrderKind::Target {
        if let Some(intent_id) = &position.target_intent_id {
            return intent_id.clone();
        }
    }
    shadow_order_key(&position.trade_id, order.kind, order.id)
}

fn ledger_side(side: Side) -> LedgerSide {
    match side {
        Side::Buy => LedgerSide::Buy,
        Side::Sell => LedgerSide::Sell,
    }
}

fn ledger_scenario_status(status: crate::types::ScenarioStatus) -> LedgerScenarioStatus {
    match status {
        crate::types::ScenarioStatus::Armed => LedgerScenarioStatus::Armed,
        crate::types::ScenarioStatus::Confirming => LedgerScenarioStatus::Confirming,
        crate::types::ScenarioStatus::Triggered => LedgerScenarioStatus::Triggered,
        crate::types::ScenarioStatus::Expired => LedgerScenarioStatus::Expired,
        crate::types::ScenarioStatus::Replaced => LedgerScenarioStatus::Replaced,
        crate::types::ScenarioStatus::CancelledByOco => LedgerScenarioStatus::CancelledByOco,
        crate::types::ScenarioStatus::Invalid => LedgerScenarioStatus::Invalid,
        crate::types::ScenarioStatus::Missed => LedgerScenarioStatus::Missed,
        crate::types::ScenarioStatus::Invalidated => LedgerScenarioStatus::Invalidated,
    }
}

fn scenario_invalid_reason_ko(reason: ScenarioInvalidReason) -> &'static str {
    match reason {
        ScenarioInvalidReason::InvalidPrice => "가격이 0이거나 호가 정규화할 수 없음",
        ScenarioInvalidReason::WrongPriceOrder => "셋업별 S·R·C·I 가격 순서 불일치",
        ScenarioInvalidReason::ConfirmationTooClose => "기준가와 확인가 사이 10bp 미만",
        ScenarioInvalidReason::InvalidTargetReturn => "목표수익률 범위 또는 0.1% 단위 불일치",
    }
}

fn reference_observed_at(
    scenario: &ValidatedScenario,
    one_minute: &[Candle],
    minimum_epoch: i64,
) -> Option<i64> {
    if scenario.setup_type != SetupType::Reversal {
        return None;
    }
    // reset이 찍힌 분봉은 bucket 안에 reset 직전 틱이 섞일 수 있으므로 통째로
    // 제외한다. 같은 분의 reset 이후 시험은 journal reset cursor로만 복원한다.
    let first_whole_bar_after_reset = (minimum_epoch > 0)
        .then(|| {
            minimum_epoch
                .div_euclid(60)
                .saturating_add(1)
                .saturating_mul(60)
        })
        .unwrap_or(0);
    let mut bars: Vec<&Candle> = one_minute
        .iter()
        .filter(|bar| {
            bar.time >= first_whole_bar_after_reset
                && bar.high.is_finite()
                && bar.low.is_finite()
                && bar.high > 0.0
                && bar.low > 0.0
        })
        .collect();
    bars.sort_unstable_by_key(|bar| bar.time);
    let start = bars.len().saturating_sub(15);
    bars[start..].iter().rev().find_map(|bar| {
        let touched = match scenario.product {
            ProductKind::Leverage => bar.low <= scenario.reference_price as f64,
            ProductKind::Inverse => bar.high >= scenario.reference_price as f64,
        };
        touched.then_some(bar.time)
    })
}

fn recent_input_window_start(
    input_one_minute: &chart_image::TimeframeIndicatorPayload,
) -> Option<i64> {
    let mut bars = input_one_minute.completed_candles.clone();
    if let Some(forming) = input_one_minute.forming_candle {
        bars.push(forming);
    }
    bars.retain(|bar| {
        bar.time > 0
            && bar.high.is_finite()
            && bar.low.is_finite()
            && bar.high > 0.0
            && bar.low > 0.0
    });
    bars.sort_unstable_by_key(|bar| bar.time);
    bars.get(bars.len().saturating_sub(15)).map(|bar| bar.time)
}

fn needs_reset_minute_tick_recovery(
    input_one_minute: &chart_image::TimeframeIndicatorPayload,
    minimum_reference_epoch: i64,
) -> bool {
    if minimum_reference_epoch <= 0 {
        return false;
    }
    let reset_bucket = minimum_reference_epoch.div_euclid(60) * 60;
    recent_input_window_start(input_one_minute)
        .is_some_and(|window_start| reset_bucket >= window_start)
}

fn seed_scenario_from_latest_market(
    scenario: &ValidatedScenario,
    current_price: u64,
    current_epoch: i64,
    one_minute: &[Candle],
    input_one_minute: &chart_image::TimeframeIndicatorPayload,
    minimum_reference_epoch: i64,
    pre_input_reference_ticks: &[TradeTick],
    replay_ticks: &[TradeTick],
) -> ScenarioSeed {
    let mut input_bars = input_one_minute.completed_candles.clone();
    if let Some(forming) = input_one_minute.forming_candle {
        input_bars.push(forming);
    }
    let mut observed_at = reference_observed_at(scenario, &input_bars, minimum_reference_epoch);
    let recent_window_start = recent_input_window_start(input_one_minute).unwrap_or(i64::MIN);
    if scenario.setup_type == SetupType::Reversal {
        for tick in pre_input_reference_ticks {
            if tick.epoch >= minimum_reference_epoch
                && tick.epoch >= recent_window_start
                && scenario.reference_reached(tick.price)
            {
                observed_at = Some(
                    observed_at
                        .map(|observed| observed.max(tick.epoch))
                        .unwrap_or(tick.epoch),
                );
            }
        }
    }
    let mut market_bars: Vec<&Candle> = one_minute
        .iter()
        .filter(|bar| {
            bar.time <= current_epoch
                && bar.high.is_finite()
                && bar.low.is_finite()
                && bar.high > 0.0
                && bar.low > 0.0
        })
        .collect();
    market_bars.sort_unstable_by_key(|bar| bar.time);
    if let (Some(observed), Some(oldest_recent)) = (
        observed_at,
        market_bars
            .get(market_bars.len().saturating_sub(15))
            .map(|bar| bar.time),
    ) {
        if observed < oldest_recent {
            observed_at = None;
        }
    }

    let mut invalidated_while_waiting = false;
    let mut confirmation_passed = false;
    for tick in replay_ticks {
        invalidated_while_waiting |= scenario.invalidation_reached(tick.price);

        if scenario.setup_type == SetupType::Reversal
            && observed_at.is_none()
            && tick.epoch >= minimum_reference_epoch
            && scenario.reference_reached(tick.price)
        {
            observed_at = Some(tick.epoch);
        }
        let reference_ready =
            scenario.setup_type == SetupType::Continuation || observed_at.is_some();
        if reference_ready && scenario.confirmation_reached(tick.price) {
            confirmation_passed = true;
        }
    }

    if scenario.setup_type == SetupType::Reversal
        && observed_at.is_none()
        && current_epoch >= minimum_reference_epoch
        && scenario.reference_reached(current_price)
    {
        observed_at = Some(current_epoch);
    }
    if invalidated_while_waiting || scenario.invalidation_reached(current_price) {
        return ScenarioSeed::invalidated(scenario.product, observed_at);
    }

    let reference_ready = scenario.setup_type == SetupType::Continuation || observed_at.is_some();
    if reference_ready && (confirmation_passed || scenario.confirmation_reached(current_price)) {
        return ScenarioSeed::missed(scenario.product, observed_at);
    }
    ScenarioSeed::armed(scenario.product, observed_at)
}

fn merge_scenario_seed(previous: ScenarioSeed, latest: ScenarioSeed) -> ScenarioSeed {
    let priority = |status| match status {
        crate::types::ScenarioStatus::Invalidated => 3,
        crate::types::ScenarioStatus::Missed => 2,
        crate::types::ScenarioStatus::Armed => 1,
        _ => 0,
    };
    ScenarioSeed {
        product: previous.product,
        status: if priority(latest.status) > priority(previous.status) {
            latest.status
        } else {
            previous.status
        },
        reference_observed_at: match (previous.reference_observed_at, latest.reference_observed_at)
        {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        },
    }
}

fn initial_decision_status(
    current_matches: bool,
    decision: &ValidatedDecision,
    seeds: &[ScenarioSeed],
) -> LedgerDecisionStatus {
    if !current_matches {
        return LedgerDecisionStatus::Discarded;
    }
    if decision.scenarios.is_empty() {
        return if decision.rejected.is_empty() {
            LedgerDecisionStatus::Skipped
        } else {
            LedgerDecisionStatus::Invalid
        };
    }
    if decision.scenarios.iter().any(|scenario| {
        seeds
            .iter()
            .find(|seed| seed.product == scenario.product)
            .is_none_or(|seed| seed.status == crate::types::ScenarioStatus::Armed)
    }) {
        return LedgerDecisionStatus::Armed;
    }
    if seeds
        .iter()
        .any(|seed| seed.status == crate::types::ScenarioStatus::Invalidated)
    {
        LedgerDecisionStatus::Invalidated
    } else {
        LedgerDecisionStatus::Missed
    }
}

fn unique_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{:016x}",
        now_kst_fake_epoch(),
        rand::random::<u64>()
    )
}

impl Engine {
    fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    fn deactivate(&self) {
        let _retirement = self.retirement_gate.lock().unwrap();
        self.active.store(false, Ordering::SeqCst);
        self.automation_feed_tx.lock().unwrap().take();
    }

    fn monotonic_now(&self) -> Duration {
        crate::util::monotonic_now()
    }

    /// 같은 mpsc FIFO에서 배리어보다 앞선 피드를 모두 cache/journal에 반영한다.
    ///
    /// 이 경계를 지난 뒤 최종 seed를 다시 계산하면 모델 응답 중 이미 지나간 C/I를
    /// 놓치지 않고 `missed`/`invalidated`로 종결할 수 있다. 배리어 뒤에 들어온
    /// 이벤트는 OCO 무장 뒤의 정상 실시간 틱으로 처리된다.
    async fn drain_automation_feed_queue(&self, expiry: i64) -> Result<(), String> {
        let tx = self
            .automation_feed_tx
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| "자동매매 피드 큐가 준비되지 않았습니다".to_string())?;
        let remaining = expiry.saturating_sub(now_kst_fake_epoch());
        if remaining <= 0 {
            return Err("피드 동기화 전에 판단 슬롯이 만료되었습니다".into());
        }
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel();
        tokio::time::timeout(Duration::from_secs(remaining as u64), async move {
            tx.send(FeedEvent::AutomationBarrier(ack_tx))
                .await
                .map_err(|_| "자동매매 피드 큐가 종료되었습니다".to_string())?;
            ack_rx
                .recv()
                .await
                .ok_or_else(|| "자동매매 피드 배리어 응답이 끊겼습니다".to_string())
        })
        .await
        .map_err(|_| "피드 동기화 중 판단 슬롯이 만료되었습니다".to_string())?
    }

    /// 제품에서는 KST 벽시계를 그대로 사용한다. 단위 테스트만 명시적으로 주입한
    /// 장중 시각을 사용해 실행 시각과 무관하게 15:15 경계를 검증한다.
    fn automation_now(&self) -> i64 {
        #[cfg(test)]
        {
            let overridden = self.automation_now_override.load(Ordering::SeqCst);
            if overridden > 0 {
                return overridden;
            }
        }
        now_kst_fake_epoch()
    }

    fn effective_market_day_state(&self) -> PersistedMarketDayState {
        let now = self.automation_now();
        let Some((date, weekend)) = market_date_info(now) else {
            return PersistedMarketDayState {
                date: String::new(),
                status: MarketDayStatus::Unknown,
                message: Some("현재 KST 날짜를 계산할 수 없습니다".into()),
                last_attempt_at: 0,
            };
        };
        if weekend {
            return PersistedMarketDayState {
                date,
                status: MarketDayStatus::Closed,
                message: Some("주말 휴장일입니다".into()),
                last_attempt_at: 0,
            };
        }
        let mut cached = self.market_day.read().unwrap().clone();
        if cached.date == date {
            if cached.status == MarketDayStatus::Unknown && self.auto_quotes_fresh(now) {
                cached.status = MarketDayStatus::OpenByQuotes;
                cached.message =
                    Some("KIS 개장일 확인 실패 · 신선한 KRX 실시간 시세로 장중 동작 중".into());
            }
            cached
        } else {
            PersistedMarketDayState {
                date,
                status: MarketDayStatus::Unknown,
                message: Some("오늘의 개장일 여부를 확인하고 있습니다".into()),
                last_attempt_at: 0,
            }
        }
    }

    fn market_is_open(&self) -> bool {
        matches!(
            self.effective_market_day_state().status,
            MarketDayStatus::Open | MarketDayStatus::OpenByQuotes
        )
    }

    fn next_automation_slot(&self, after: i64) -> Option<i64> {
        self.market_is_open()
            .then(|| schedule::next_decision_slot(after))
            .flatten()
    }

    fn persist_market_day_state(&self, state: &PersistedMarketDayState) {
        if state.status == MarketDayStatus::OpenByQuotes {
            tracing::warn!("실시간 시세 기반 개장 상태는 영속화하지 않습니다");
            return;
        }
        let _retirement = self.retirement_gate.lock().unwrap();
        if !self.is_active() {
            return;
        }
        if let Err(error) =
            self.ledger
                .set_runtime_state(MARKET_DAY_STATE_KEY, state, now_kst_fake_epoch())
        {
            tracing::error!("개장일 상태 저장 실패: {error}");
        }
    }

    fn market_allows_auto_exit(&self) -> bool {
        match self.effective_market_day_state().status {
            MarketDayStatus::Open | MarketDayStatus::OpenByQuotes => true,
            MarketDayStatus::Closed => false,
            MarketDayStatus::Unknown => {
                if !self.connected.load(Ordering::SeqCst) {
                    return false;
                }
                let code = self
                    .automation
                    .lock()
                    .unwrap()
                    .position()
                    .filter(|position| !position.shadow)
                    .map(|position| position.code.clone());
                let Some(code) = code else { return false };
                let now = self.automation_now();
                self.quotes.read().unwrap().get(&code).is_some_and(|quote| {
                    quote.trade_sequence > 0
                        && quote.trade_ts <= now
                        && now.saturating_sub(quote.trade_ts) <= QUOTE_FRESH_SECS
                })
            }
        }
    }

    fn defer_auto_exit_for_market_day(&self) -> bool {
        if self.market_allows_auto_exit() {
            return false;
        }
        let market_day = self.effective_market_day_state();
        self.automation
            .lock()
            .unwrap()
            .exit_waiting(match market_day.status {
                MarketDayStatus::Closed => {
                    "휴장일에는 새 청산 주문을 보내지 않고 다음 개장일까지 대기합니다".to_string()
                }
                MarketDayStatus::Unknown => {
                    "개장일 확인 전에는 신선한 실시간 체결이 없어 청산 주문을 보류합니다"
                        .to_string()
                }
                MarketDayStatus::Open | MarketDayStatus::OpenByQuotes => {
                    "청산 주문 가능 상태를 다시 확인합니다".to_string()
                }
            });
        self.persist_automation();
        self.emit_automation_state();
        true
    }

    fn install_market_day_state(self: &Arc<Self>, state: PersistedMarketDayState, persist: bool) {
        let previous = self.effective_market_day_state();
        let stored = self.market_day.read().unwrap().clone();
        let storage_changed = stored.date != state.date
            || stored.status != state.status
            || stored.message != state.message
            || stored.last_attempt_at != state.last_attempt_at;
        *self.market_day.write().unwrap() = state.clone();
        if persist && storage_changed {
            self.persist_market_day_state(&state);
        }
        let effective = self.effective_market_day_state();
        let changed = previous.date != effective.date
            || previous.status != effective.status
            || previous.message != effective.message;
        if !changed {
            return;
        }

        if !matches!(
            effective.status,
            MarketDayStatus::Open | MarketDayStatus::OpenByQuotes
        ) {
            let replacement_recorded = match self.mark_current_group_replaced() {
                Ok(()) => true,
                Err(error) => {
                    tracing::error!("휴장 전 OCO 장부 종결 실패, 다음 tick에서 재시도: {error}");
                    self.emit(
                        "engine-error",
                        &format!("휴장 전 자동 시나리오 장부 종결을 재시도합니다: {error}"),
                    );
                    false
                }
            };
            if replacement_recorded && self.automation.lock().unwrap().pause_for_market_day() {
                self.persist_automation();
            }
        } else {
            let now = self.automation_now();
            let mut runtime = self.automation.lock().unwrap();
            if matches!(runtime.mode(), ControlMode::Auto | ControlMode::Shadow)
                && runtime.position().is_none()
            {
                let recovered = schedule::recover_persisted_decision_slot(
                    runtime.next_decision_at(),
                    runtime.last_decision_slot(),
                    now,
                );
                runtime.set_next_decision_at(recovered);
            }
            drop(runtime);
            self.persist_automation();
        }
        self.emit_automation_state();

        if matches!(
            effective.status,
            MarketDayStatus::Open | MarketDayStatus::OpenByQuotes
        ) {
            let deferred_exit = {
                let runtime = self.automation.lock().unwrap();
                (runtime.mode() == ControlMode::Auto
                    && runtime.phase() == crate::types::AutomationPhase::ExitPending)
                    .then(|| runtime.exit_reason().map(str::to_owned))
                    .flatten()
            };
            if let Some(reason) = deferred_exit.as_deref().and_then(auto_exit_reason) {
                let engine = Arc::clone(self);
                tokio::spawn(async move { engine.execute_auto_exit(reason).await });
            }
        }
    }

    async fn refresh_market_day_if_needed(self: &Arc<Self>) {
        if !self.is_active() {
            return;
        }
        let Ok(_refresh_guard) = self.market_day_refresh_gate.try_lock() else {
            return;
        };
        let now = self.automation_now();
        let Some((date, weekend)) = market_date_info(now) else {
            return;
        };
        if weekend {
            self.install_market_day_state(
                PersistedMarketDayState {
                    date,
                    status: MarketDayStatus::Closed,
                    message: Some("주말 휴장일입니다".into()),
                    last_attempt_at: 0,
                },
                true,
            );
            return;
        }

        let current = self.market_day.read().unwrap().clone();
        let retry_cooldown = current.status == MarketDayStatus::Unknown
            && !is_transient_market_day_message(current.message.as_deref())
            && now.saturating_sub(current.last_attempt_at) < MARKET_DAY_RETRY_SECS;
        if current.date == date
            && (matches!(
                current.status,
                MarketDayStatus::Open | MarketDayStatus::Closed
            ) || retry_cooldown)
        {
            return;
        }

        let checking = PersistedMarketDayState {
            date: date.clone(),
            status: MarketDayStatus::Unknown,
            message: Some("개장일 여부를 확인하고 있습니다".into()),
            last_attempt_at: now,
        };
        // 조회 도중 엔진이 중단돼도 이 과도 상태를 다음 실행에 남기지 않는다.
        self.install_market_day_state(checking, false);
        let response = tokio::time::timeout(
            Duration::from_secs(MARKET_DAY_REQUEST_TIMEOUT_SECS),
            self.broker.market_days(&date),
        )
        .await;
        if !self.is_active() {
            return;
        }
        let resolved = match response {
            Ok(Ok(days)) => days
                .into_iter()
                .find(|day| day.date == date)
                .map(|day| PersistedMarketDayState {
                    date: date.clone(),
                    status: if day.is_open {
                        MarketDayStatus::Open
                    } else {
                        MarketDayStatus::Closed
                    },
                    message: (!day.is_open).then(|| "KRX 휴장일입니다".into()),
                    last_attempt_at: now,
                })
                .unwrap_or(PersistedMarketDayState {
                    date: date.clone(),
                    status: MarketDayStatus::Unknown,
                    message: Some("KIS 응답에 오늘 날짜가 없어 신규 진입을 중지합니다".into()),
                    last_attempt_at: now,
                }),
            Ok(Err(error)) => PersistedMarketDayState {
                date,
                status: MarketDayStatus::Unknown,
                message: Some(format!(
                    "개장일 확인 실패로 신규 진입을 중지합니다: {error}"
                )),
                last_attempt_at: now,
            },
            Err(_) => PersistedMarketDayState {
                date,
                status: MarketDayStatus::Unknown,
                message: Some("개장일 확인이 60초를 넘겨 신규 진입을 중지합니다".into()),
                last_attempt_at: now,
            },
        };
        self.install_market_day_state(resolved, true);
    }

    /// 불명확 Auto 진입은 실제 첫 체결시각을 아직 모를 수 있으므로, POST 전에
    /// 내구화한 주문 의도 시각을 장마감 기준일로 사용한다. 런타임 잠금은 장부
    /// 조회 전에 해제해 다른 조정 경로와 잠금 순서가 엇갈리지 않게 한다.
    fn pending_entry_reference_at(&self) -> Option<i64> {
        let intent_id = {
            self.automation
                .lock()
                .unwrap()
                .pending_entry()
                .map(|pending| pending.intent_id.clone())
        }?;
        self.ledger
            .get_order(&intent_id)
            .ok()
            .flatten()
            .map(|order| order.requested_at)
    }

    fn pending_entry_market_close_due(&self, now: i64) -> bool {
        self.pending_entry_reference_at().map_or_else(
            || schedule::is_at_or_after_flatten(now),
            |reference| schedule::has_reached_position_flatten(reference, now),
        )
    }

    /// actor 대기 또는 계좌 조회 중 슬롯 만료·15:15 경계를 넘었으면, 같은
    /// OCO winner가 여전히 소유한 EntryPending만 해제한다. 이미 모드나 revision이
    /// 바뀐 오래된 작업은 새 상태를 건드리지 않고 폐기한다.
    fn validate_triggered_entry_window(
        &self,
        mode: ControlMode,
        triggered: &TriggeredScenario,
    ) -> bool {
        if !self.is_active() {
            return false;
        }
        let now = self.automation_now();
        let market_open = self.market_is_open();
        let rejection = {
            let mut runtime = self.automation.lock().unwrap();
            if runtime.mode() != mode || !runtime.pending_matches(triggered) {
                return false;
            }
            let rejection = if !market_open {
                Some("오늘은 KRX 개장일이 아니어서 자동 진입을 폐기했습니다")
            } else if schedule::is_at_or_after_flatten(now) {
                Some("15:15 장 마감 정리가 시작되어 진입을 폐기했습니다")
            } else if !runtime.pending_matches_at(triggered, now) {
                Some("OCO 시나리오 슬롯이 만료되어 진입을 폐기했습니다")
            } else {
                None
            };
            if let Some(message) = rejection {
                runtime.entry_failed(message);
            }
            rejection
        };
        if rejection.is_some() {
            self.persist_automation();
            self.emit_automation_state();
            return false;
        }
        true
    }

    pub fn automation_snapshot(&self) -> AutomationSnapshot {
        let market_day = self.effective_market_day_state();
        let settings = self.settings.read().unwrap();
        self.automation.lock().unwrap().snapshot(
            &self.runtime_id,
            self.runtime_generation,
            &settings.auto_symbols,
            self.monotonic_now(),
            market_day.status,
            market_day.message,
        )
    }

    fn emit_automation_state(&self) {
        if self.is_active() {
            self.emit("automation-state", &self.automation_snapshot());
        }
    }

    fn persist_automation(&self) {
        if let Err(error) = self.persist_automation_required() {
            tracing::error!("자동매매 상태 저장 실패: {error}");
        }
    }

    /// 주문 POST 전에는 실패를 무시할 수 없으므로 저장 결과를 호출자에게 돌려준다.
    fn persist_automation_required(&self) -> Result<(), String> {
        if !self.is_active() {
            return Err("폐기된 엔진의 자동매매 상태는 저장하지 않습니다".into());
        }
        let runtime = self.automation.lock().unwrap();
        let mut automation = runtime.persistable();
        let shadow = (runtime.mode() == ControlMode::Shadow)
            .then(|| self.shadow.lock().unwrap().clone())
            .flatten();
        let mut bundle_consistent = runtime.mode() != ControlMode::Shadow;
        if let (Some(position), Some(session)) = (automation.position.as_mut(), shadow.as_ref()) {
            if position.shadow
                && session.position().is_some_and(|current| {
                    current.code == position.code
                        && shadow_product(position.product) == current.product
                })
            {
                let current = session.position().expect("위 가드에서 확인");
                position.entry_qty = current.entry_qty;
                position.qty = current.qty;
                position.avg_price = current.entry_vwap;
                position.last_price = current.last_price as f64;
                position.profit_guard_armed = current.profit_guard_armed;
                let (exit_qty, exit_value) = session.exit_summary();
                position.exit_qty = exit_qty;
                position.exit_value = exit_value;
                position.pending_exit_reason = session
                    .pending_exit_reason()
                    .map(shadow_exit_reason)
                    .map(str::to_owned);
                automation.shadow_cash = Some(session.cash());

                let forced = session.latest_trade_orders().iter().rev().find(|order| {
                    matches!(order.kind, ShadowOrderKind::ForcedExit(_))
                        && matches!(
                            order.status,
                            ShadowOrderStatus::Pending | ShadowOrderStatus::PartiallyFilled
                        )
                });
                if let Some(order) = forced {
                    let intent_id = shadow_intent_for_order(position, order);
                    position.exit_order_no = Some(intent_id.clone());
                    position.exit_order_org_no = None;
                    position.exit_intent_id = Some(intent_id);
                    position.exit_requested_at = Some(order.created_at);
                } else {
                    position.exit_order_no = None;
                    position.exit_order_org_no = None;
                    position.exit_intent_id = None;
                    position.exit_requested_at = None;
                }
                bundle_consistent = true;
            }
        }
        if automation.position.is_none()
            && shadow
                .as_ref()
                .is_some_and(|session| session.position().is_none())
        {
            bundle_consistent = true;
        }
        let bundle = PersistedAutomationBundle {
            automation: automation.clone(),
            shadow,
        };
        let now = self.automation_now();
        let _retirement = self.retirement_gate.lock().unwrap();
        if !self.is_active() {
            return Err("폐기된 엔진의 자동매매 상태는 저장하지 않습니다".into());
        }
        if bundle_consistent {
            self.ledger
                .set_runtime_state(AUTOMATION_BUNDLE_STATE_KEY, &bundle, now)
                .map_err(|error| format!("자동·섀도 통합 상태 저장 실패: {error}"))?;
        } else {
            tracing::warn!("섀도 세션과 자동 상태 반영 사이에는 통합 스냅샷을 건너뜁니다");
        }
        self.ledger
            .set_runtime_state(AUTOMATION_STATE_KEY, &automation, now)
            .map_err(|error| format!("자동매매 상태 저장 실패: {error}"))?;
        self.ledger
            .set_control_mode(ledger_control_mode(runtime.mode()), now)
            .map_err(|error| format!("제어 모드 저장 실패: {error}"))?;
        Ok(())
    }

    fn pending_auto_handoff(&self) -> Result<Option<PendingAutoHandoff>, String> {
        self.ledger
            .get_runtime_state(AUTO_HANDOFF_STATE_KEY)
            .map_err(|error| format!("Auto 인계 복구 표식 조회 실패: {error}"))
    }

    /// 앱 시작 시 저장된 Auto 인계를 자동 청산 스케줄러보다 먼저 재개한다.
    /// 최종 모드 상태가 이미 저장된 뒤 표식 삭제에서만 종료됐던 경우에는 POST 없이
    /// 표식만 제거한다.
    async fn resume_pending_auto_handoff(self: &Arc<Self>) -> Result<(), String> {
        let Some(handoff) = self.pending_auto_handoff()? else {
            return Ok(());
        };
        if handoff.target_mode == ControlMode::Auto {
            return Err("Auto 인계 복구 표식의 대상 모드가 Auto라서 복구할 수 없습니다".into());
        }

        let current = self.automation.lock().unwrap().mode();
        if current == handoff.target_mode {
            self.persist_automation_required()?;
            self.ledger
                .delete_runtime_state(AUTO_HANDOFF_STATE_KEY)
                .map_err(|error| format!("완료된 Auto 인계 복구 표식 삭제 실패: {error}"))?;
            return Ok(());
        }
        if current != ControlMode::Auto {
            return Err(format!(
                "Auto 인계 복구 표식과 저장 모드가 일치하지 않습니다: 저장={current:?}, 대상={:?}",
                handoff.target_mode
            ));
        }

        self.set_control_mode(handoff.target_mode).await?;
        Ok(())
    }

    /// 재시작 시 pending 진입은 조회로만 복구하며 매수 POST를 절대 다시 보내지 않는다.
    /// 목표 주문이 확실히 살아 있지 않으면 포지션을 즉시 청산 대기 상태로 넘긴다.
    fn reconcile_pending_auto_entry_startup(
        &self,
        account: &AccountSnapshot,
        open_orders: &[crate::broker::BrokerOpenOrder],
        today_fills: &[crate::broker::BrokerFill],
    ) -> Result<(), String> {
        let pending = self.automation.lock().unwrap().pending_entry().cloned();
        let Some(pending) = pending else {
            return Ok(());
        };
        let settings = self.settings.read().unwrap().clone();
        let code = pending.product.code(&settings.auto_symbols).to_string();
        let actual_qty = account
            .positions
            .iter()
            .find(|position| position.code == code)
            .map(|position| position.qty)
            .unwrap_or(0);
        let entry_order = self
            .ledger
            .get_order(&pending.intent_id)
            .map_err(|error| error.to_string())?;

        let Some(mut entry_order) = entry_order else {
            // pending 저장 뒤 intent 기록 전에 종료된 경우 POST가 없었음이 증명된다.
            if actual_qty != pending.baseline_qty {
                return Err(format!(
                    "진입 intent는 없지만 계좌 수량이 변했습니다: 기준 {}주 / 현재 {actual_qty}주",
                    pending.baseline_qty
                ));
            }
            self.automation.lock().unwrap().clear_pending_entry();
            return Ok(());
        };
        if entry_order.status == LedgerOrderStatus::Intent {
            // dispatch CAS 전 종료됐으므로 이 프로토콜에서는 POST가 실행되지 않았다.
            if actual_qty != pending.baseline_qty {
                return Err("미제출 진입 intent와 계좌 수량이 일치하지 않습니다".into());
            }
            let _ = self.ledger.update_order_status(
                &pending.intent_id,
                LedgerOrderStatus::Cancelled,
                Some("재시작 시 dispatch 전 종료 확인"),
                now_kst_fake_epoch(),
            );
            self.automation.lock().unwrap().clear_pending_entry();
            return Ok(());
        }

        let recovered_identity = entry_order.broker_order_id.is_none();
        let mut candidates = HashSet::new();
        if let Some(order_no) = entry_order.broker_order_id.clone() {
            candidates.insert(order_no);
        } else {
            candidates.extend(
                today_fills
                    .iter()
                    .filter(|fill| {
                        fill.code == code
                            && fill.side == Side::Buy
                            && fill.filled_at >= entry_order.requested_at
                            && fill.qty <= entry_order.qty
                            && entry_order
                                .price
                                .is_none_or(|limit| fill.price <= limit as f64)
                    })
                    .map(|fill| fill.order_no.clone()),
            );
            candidates.extend(
                open_orders
                    .iter()
                    .filter(|order| {
                        order.code == code
                            && order.side == Side::Buy
                            && order.ordered_at >= entry_order.requested_at
                            && order.ordered_qty == entry_order.qty
                            && entry_order
                                .price
                                .is_none_or(|limit| order.price.round() as u64 == limit)
                    })
                    .map(|order| order.order_no.clone()),
            );
        }
        if candidates.len() > 1 {
            return Err("재시작 진입 주문 후보가 둘 이상이라 소유권을 확정할 수 없습니다".into());
        }

        let candidate = candidates.into_iter().next();
        if let Some(order_no) = candidate.as_deref() {
            if entry_order.broker_order_id.is_none() {
                let org_no = today_fills
                    .iter()
                    .find(|fill| fill.order_no == order_no && fill.code == code)
                    .map(|fill| fill.org_no.clone())
                    .or_else(|| {
                        open_orders
                            .iter()
                            .find(|order| order.order_no == order_no && order.code == code)
                            .map(|order| order.org_no.clone())
                    })
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| "복구한 진입 주문의 조직번호가 비어 있습니다".to_string())?;
                self.ledger
                    .record_order_ack(
                        &pending.intent_id,
                        &OrderAcknowledgement {
                            broker_order_id: Some(order_no.to_string()),
                            broker_org_no: Some(org_no),
                            original_order_id: None,
                            status: LedgerOrderStatus::Submitted,
                            message: Some("재시작 조회로 진입 주문 소유권 복구".into()),
                            acknowledged_at: now_kst_fake_epoch(),
                        },
                    )
                    .map_err(|error| error.to_string())?;
                entry_order = self
                    .ledger
                    .get_order(&pending.intent_id)
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "복구한 진입 주문 장부가 사라졌습니다".to_string())?;
            }
            if open_orders.iter().any(|order| {
                order.order_no == order_no && order.side == Side::Buy && order.cancelable_qty > 0
            }) {
                return Err(
                    "재시작한 IOC 매수 주문 소유권을 복구했지만 미체결 목록에 남아 있습니다".into(),
                );
            }
        }
        if recovered_identity && candidate.is_some() {
            return Err(
                "재시작 조회로 IOC 주문번호를 복구했으며 정확한 terminal 상태를 기다립니다".into(),
            );
        }
        if self.automation.lock().unwrap().position().is_none()
            && entry_order.broker_order_id.is_some()
            && !matches!(
                entry_order.status,
                LedgerOrderStatus::Filled | LedgerOrderStatus::PartiallyFilled
            )
        {
            return Err(
                "Auto IOC 주문번호는 확인됐지만 정확한 terminal 체결 상태가 아직 없습니다".into(),
            );
        }

        if self.automation.lock().unwrap().position().is_none() {
            let Some(order_no) = candidate.as_deref() else {
                let legacy_rate_limit = entry_order.status == LedgerOrderStatus::Unknown
                    && entry_order.broker_order_id.is_none()
                    && entry_order
                        .message
                        .as_deref()
                        .is_some_and(is_rate_limit_error)
                    && self
                        .automation_now()
                        .saturating_sub(entry_order.requested_at)
                        >= LEGACY_RATE_LIMIT_RECONCILE_SECS;
                if legacy_rate_limit && actual_qty == pending.baseline_qty {
                    self.ledger
                        .update_order_status(
                            &pending.intent_id,
                            LedgerOrderStatus::Rejected,
                            Some(
                                "구버전 KIS 유량 제한 응답과 잔고·체결·미체결 무변화로 미접수 확인",
                            ),
                            now_kst_fake_epoch(),
                        )
                        .map_err(|error| error.to_string())?;
                    self.automation
                        .lock()
                        .unwrap()
                        .entry_failed("KIS 유량 제한으로 자동 진입 주문이 접수되지 않았습니다");
                    return Ok(());
                }
                return Err(
                    "제출 중이던 진입 주문을 체결·미체결에서 찾지 못해 재주문을 금지합니다".into(),
                );
            };
            let fill = today_fills
                .iter()
                .find(|fill| {
                    fill.order_no == order_no && fill.code == code && fill.side == Side::Buy
                })
                .ok_or_else(|| "복구한 진입 주문의 체결 상세가 없습니다".to_string())?;
            let account_delta = actual_qty.saturating_sub(pending.baseline_qty);
            if account_delta != fill.qty {
                return Err(format!(
                    "재시작 진입 수량 불일치: 계좌 증가 {account_delta}주 / 체결 {}주",
                    fill.qty
                ));
            }
            let target_price = sell_target_price(fill.price, pending.target_return_pct, true);
            let position = OwnedPosition {
                product: pending.product,
                code: code.clone(),
                entry_qty: fill.qty,
                qty: fill.qty,
                avg_price: fill.price,
                last_price: fill.price,
                target_return_pct: pending.target_return_pct,
                target_price,
                first_fill_at: fill.filled_at,
                exit_deadline: fill.filled_at + AUTO_MAX_HOLD_SECS,
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
                trade_id: pending.trade_id.clone(),
                shadow: false,
                profit_guard_armed: false,
            };
            if !self
                .automation
                .lock()
                .unwrap()
                .stage_entry_position(position)
            {
                return Err("재시작 체결 포지션을 런타임에 복원하지 못했습니다".into());
            }
            let _ = self.ledger.update_order_status(
                &pending.intent_id,
                if fill.qty < entry_order.qty {
                    LedgerOrderStatus::PartiallyFilled
                } else {
                    LedgerOrderStatus::Filled
                },
                Some("재시작 시 IOC 종결 복구"),
                now_kst_fake_epoch(),
            );
            let _ = self.ledger.upsert_trade(&NewTrade {
                trade_id: pending.trade_id.clone(),
                session_id: self
                    .automation
                    .lock()
                    .unwrap()
                    .session_id()
                    .map(str::to_owned),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: code.clone(),
                entry_qty: fill.qty,
                exit_qty: 0,
                entry_avg_price: fill.price,
                exit_avg_price: None,
                pnl_rate: None,
                entered_at: fill.filled_at,
                exited_at: None,
                status: LedgerTradeStatus::Open,
                exit_reason: None,
                updated_at: now_kst_fake_epoch(),
            });
        }

        let position = self
            .automation
            .lock()
            .unwrap()
            .position()
            .cloned()
            .ok_or_else(|| "pending 진입 포지션 복원에 실패했습니다".to_string())?;
        if actual_qty != position.qty {
            return Err(format!(
                "저장된 진입 포지션과 계좌 수량이 다릅니다: 저장 {}주 / 계좌 {actual_qty}주",
                position.qty
            ));
        }

        let known_target = position.target_order_no.as_ref().and_then(|order_no| {
            open_orders.iter().find(|order| {
                &order.order_no == order_no
                    && order.code == position.code
                    && order.side == Side::Sell
                    && order.cancelable_qty > 0
            })
        });
        if let Some(order) = known_target {
            if position.target_org_no.as_deref() != Some(order.org_no.as_str()) {
                return Err("저장된 목표 주문 조직번호가 KIS 조회와 다릅니다".into());
            }
            if !self.automation.lock().unwrap().complete_pending_entry() {
                return Err("복구한 목표 주문을 Holding으로 승격하지 못했습니다".into());
            }
            self.reservations.write().unwrap().insert(
                position.code.clone(),
                Reservation {
                    target_pct: position.target_return_pct,
                    target_price: position.target_price,
                    qty: position.qty,
                    order_no: order.order_no.clone(),
                    org_no: order.org_no.clone(),
                },
            );
            return Ok(());
        }

        if let Some(target_intent_id) = position.target_intent_id.clone() {
            let target_intent = self
                .ledger
                .get_order(&target_intent_id)
                .map_err(|error| error.to_string())?;
            if let Some(target_intent) = target_intent {
                let matches = open_orders
                    .iter()
                    .filter(|order| {
                        order.code == position.code
                            && order.side == Side::Sell
                            && order.cancelable_qty > 0
                            && order.ordered_at >= target_intent.requested_at
                            && order.ordered_qty == position.entry_qty
                            && order.price.round() as u64 == position.target_price
                    })
                    .collect::<Vec<_>>();
                if matches.len() == 1 {
                    let order = matches[0];
                    if self
                        .automation
                        .lock()
                        .unwrap()
                        .bind_target_order(order.order_no.clone(), order.org_no.clone())
                        && self.automation.lock().unwrap().complete_pending_entry()
                    {
                        let _ = self.ledger.record_order_ack(
                            &target_intent_id,
                            &OrderAcknowledgement {
                                broker_order_id: Some(order.order_no.clone()),
                                broker_org_no: Some(order.org_no.clone()),
                                original_order_id: None,
                                status: LedgerOrderStatus::Submitted,
                                message: Some("재시작 조회로 목표 주문 복구".into()),
                                acknowledged_at: now_kst_fake_epoch(),
                            },
                        );
                        self.reservations.write().unwrap().insert(
                            position.code.clone(),
                            Reservation {
                                target_pct: position.target_return_pct,
                                target_price: position.target_price,
                                qty: position.qty,
                                order_no: order.order_no.clone(),
                                org_no: order.org_no.clone(),
                            },
                        );
                        return Ok(());
                    }
                } else if matches.len() > 1 {
                    return Err("재시작 목표 주문 후보가 둘 이상입니다".into());
                }
            }
        }

        // 체결 포지션은 확실하지만 목표 주문은 확정되지 않았다. 매수는 재시도하지
        // 않고, 즉시 청산 경로에서 불명확 목표를 먼저 조정한 뒤 잔량만 매도한다.
        self.automation.lock().unwrap().clear_pending_entry();
        if self
            .automation
            .lock()
            .unwrap()
            .begin_exit("target_order_failed")
            .is_none()
        {
            return Err("복구한 무보호 포지션을 청산 대기로 전환하지 못했습니다".into());
        }
        Ok(())
    }

    /// 15:15까지 REST 체결 상세가 끝내 보이지 않더라도, 앱이 POST 전에 저장한
    /// pending Auto intent와 기준수량 0을 계좌와 대조해 노출 수량을 복원한다.
    /// 가격은 계좌 체결평단을 사용하며 복원 직후 시장가 마감 청산으로만 넘긴다.
    fn recover_pending_entry_for_flatten(&self, account: &AccountSnapshot) -> Result<bool, String> {
        if !self.pending_entry_market_close_due(self.automation_now()) {
            return Ok(false);
        }
        let pending = self.automation.lock().unwrap().pending_entry().cloned();
        let Some(pending) = pending else {
            return Ok(false);
        };
        if pending.baseline_qty != 0 {
            return Err(
                "기준 보유가 있던 pending 진입은 수동 보유와 분리할 수 없어 자동 청산하지 않습니다"
                    .into(),
            );
        }
        let settings = self.settings.read().unwrap().clone();
        let code = pending.product.code(&settings.auto_symbols).to_string();
        let Some(account_position) = account
            .positions
            .iter()
            .find(|position| position.code == code && position.qty > 0)
        else {
            return Ok(false);
        };
        if account_position.avg_price <= 0.0 {
            return Err("마감 pending 진입의 계좌 체결평단을 확인할 수 없습니다".into());
        }
        let first_fill_at = self
            .pending_entry_reference_at()
            .unwrap_or_else(|| self.automation_now());
        let target_price =
            sell_target_price(account_position.avg_price, pending.target_return_pct, true);
        let position = OwnedPosition {
            product: pending.product,
            code: code.clone(),
            entry_qty: account_position.qty,
            qty: account_position.qty,
            avg_price: account_position.avg_price,
            last_price: account_position.avg_price,
            target_return_pct: pending.target_return_pct,
            target_price,
            first_fill_at,
            exit_deadline: first_fill_at,
            target_order_no: None,
            target_org_no: None,
            target_intent_id: None,
            exit_order_no: None,
            exit_order_org_no: None,
            exit_intent_id: None,
            exit_requested_at: None,
            pending_exit_reason: Some("market_close".into()),
            exit_qty: 0,
            exit_value: 0.0,
            trade_id: pending.trade_id.clone(),
            shadow: false,
            profit_guard_armed: false,
        };
        {
            let mut runtime = self.automation.lock().unwrap();
            if !runtime.stage_entry_position(position) {
                return Err("마감 pending 노출을 Auto 포지션으로 복원하지 못했습니다".into());
            }
            runtime.clear_pending_entry();
            if runtime.begin_exit("market_close").is_none() {
                return Err("복원한 마감 pending 노출을 청산 대기로 전환하지 못했습니다".into());
            }
        }
        let _ = self.ledger.upsert_trade(&NewTrade {
            trade_id: pending.trade_id,
            session_id: self
                .automation
                .lock()
                .unwrap()
                .session_id()
                .map(str::to_owned),
            execution_kind: LedgerExecutionKind::Real,
            origin: LedgerOrigin::Auto,
            code,
            entry_qty: account_position.qty,
            exit_qty: 0,
            entry_avg_price: account_position.avg_price,
            exit_avg_price: None,
            pnl_rate: None,
            entered_at: first_fill_at,
            exited_at: None,
            status: LedgerTradeStatus::Open,
            exit_reason: Some("reconcile_missing_price".into()),
            updated_at: first_fill_at,
        });
        Ok(true)
    }

    /// 15:15 조정에서 계좌·미체결·당일체결을 모두 정상 조회했고 진입 노출이
    /// 전혀 없다면, 불명확 ACK만 남은 pending을 무기한 끌고 가지 않고 종결한다.
    /// 기준 보유가 0이 아닌 경우에는 수동 보유와 분리할 수 없으므로 건드리지 않는다.
    fn finish_empty_pending_entry_for_flatten(
        &self,
        account: &AccountSnapshot,
    ) -> Result<bool, String> {
        let now = self.automation_now();
        if !self.pending_entry_market_close_due(now) {
            return Ok(false);
        }
        let pending = self.automation.lock().unwrap().pending_entry().cloned();
        let Some(pending) = pending else {
            return Ok(false);
        };
        if pending.baseline_qty != 0 || self.automation.lock().unwrap().position().is_some() {
            return Ok(false);
        }
        let settings = self.settings.read().unwrap().clone();
        let code = pending.product.code(&settings.auto_symbols).to_string();
        if account
            .positions
            .iter()
            .any(|position| position.code == code && position.qty > 0)
        {
            return Ok(false);
        }

        // 체결 통보 등록·REST delta 기록과 같은 잠금에서 마지막으로 generation과
        // 단일비행을 확인한다. clear 뒤 지연 통보는 아래 tombstone이 다시 귀속한다.
        let _ingest = self.fill_ingest_lock.lock().unwrap();
        if self.account_refreshed_gen.load(Ordering::SeqCst)
            < self.account_gen.load(Ordering::SeqCst)
            || self
                .fill_reconcile_pending
                .lock()
                .unwrap()
                .values()
                .any(|state| state.running && state.code == code && state.side == Side::Buy)
        {
            return Ok(false);
        }
        let order = self
            .ledger
            .get_order(&pending.intent_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "마감 pending 진입 intent를 장부에서 찾지 못했습니다".to_string())?;
        if order.origin != LedgerOrigin::Auto
            || order.execution_kind != LedgerExecutionKind::Real
            || order.side != LedgerSide::Buy
            || order.code != code
            || order.qty == 0
        {
            return Err("마감 pending 진입 intent의 주문 형태가 일치하지 않습니다".into());
        }
        let Some(order_no) = order
            .broker_order_id
            .clone()
            .filter(|value| !value.trim().is_empty())
        else {
            // 주문번호 없는 POST 결과는 이후의 임의 매수 체결과 구분할 수 없다.
            // 정확한 KIS 주문 identity를 얻기 전에는 pending을 tombstone으로 내리지 않는다.
            return Ok(false);
        };
        let Some(org_no) = order
            .broker_org_no
            .clone()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(false);
        };
        if order.trading_date.trim().is_empty() {
            return Ok(false);
        }
        let watch = LateAutoEntryWatch {
            pending: pending.clone(),
            trading_date: order.trading_date.clone(),
            order_no,
            org_no,
            code,
            ordered_qty: order.qty,
            limit_price: order.price,
            requested_at: order.requested_at,
        };
        self.ledger
            .set_runtime_state(LATE_AUTO_ENTRY_WATCH_KEY, &watch, now)
            .map_err(|error| error.to_string())?;
        if let Err(error) = self.ledger.update_order_status(
            &pending.intent_id,
            LedgerOrderStatus::Unknown,
            Some("15:15 현재 노출 없음 확인, 거래일 종료까지 지연 체결 감시"),
            now,
        ) {
            let _ = self.ledger.delete_runtime_state(LATE_AUTO_ENTRY_WATCH_KEY);
            return Err(error.to_string());
        }
        self.automation
            .lock()
            .unwrap()
            .entry_failed("15:15 조정에서 자동 진입 노출이 없음을 확인했습니다");
        Ok(true)
    }

    /// 마감 무노출 확인과 실제 WebSocket 전달 사이의 지연을 흡수한다. tombstone의
    /// 주문 형태와 정확히 맞는 매수 통보만 원래 Auto pending으로 복원한다.
    fn restore_late_auto_entry_notice(&self, fill: &crate::types::FillEvent) -> bool {
        if fill.side != Side::Buy
            || fill.order_no.trim().is_empty()
            || fill.org_no.trim().is_empty()
            || fill.qty == 0
            || fill.price <= 0.0
        {
            return false;
        }
        let _ingest = self.fill_ingest_lock.lock().unwrap();
        let watch = match self
            .ledger
            .get_runtime_state::<LateAutoEntryWatch>(LATE_AUTO_ENTRY_WATCH_KEY)
        {
            Ok(Some(watch)) => watch,
            _ => return false,
        };
        if watch.trading_date.trim().is_empty()
            || watch.order_no.trim().is_empty()
            || watch.org_no.trim().is_empty()
            || watch.order_no != fill.order_no
            || watch.org_no != fill.org_no
            || watch.code != fill.code
            || crate::util::trading_date_from_fake_epoch(fill.filled_at)
                .ok()
                .as_deref()
                != Some(watch.trading_date.as_str())
            || fill.filled_at < watch.requested_at
            || fill.qty > watch.ordered_qty
            || watch
                .limit_price
                .is_some_and(|limit| fill.price > limit as f64)
        {
            return false;
        }
        let order = match self.ledger.get_order(&watch.pending.intent_id) {
            Ok(Some(order))
                if order.origin == LedgerOrigin::Auto
                    && order.execution_kind == LedgerExecutionKind::Real
                    && order.side == LedgerSide::Buy
                    && order.code == watch.code
                    && order.qty == watch.ordered_qty
                    && order.trading_date == watch.trading_date
                    && order.broker_order_id.as_deref() == Some(watch.order_no.as_str())
                    && order.broker_org_no.as_deref() == Some(watch.org_no.as_str()) =>
            {
                order
            }
            _ => return false,
        };
        // 모드 전환·다른 포지션 등으로 런타임 복원이 불가능하면 장부 ACK도 건드리지
        // 않는다. 두 전이를 같은 런타임 잠금 안에서 수행해 외부/수동 체결의 오귀속을 막는다.
        let mut runtime = self.automation.lock().unwrap();
        if !runtime.restore_pending_entry(watch.pending.clone()) {
            return false;
        }
        if self
            .ledger
            .record_order_ack(
                &watch.pending.intent_id,
                &OrderAcknowledgement {
                    broker_order_id: Some(watch.order_no.clone()),
                    broker_org_no: Some(watch.org_no.clone()),
                    original_order_id: (!fill.original_order_no.trim().is_empty())
                        .then(|| fill.original_order_no.clone()),
                    status: LedgerOrderStatus::Submitted,
                    message: Some("15:15 무노출 확인 뒤 지연 체결 통보로 Auto 소유권 복원".into()),
                    acknowledged_at: fill.filled_at.max(order.requested_at),
                },
            )
            .is_err()
        {
            runtime.clear_pending_entry();
            return false;
        }
        drop(runtime);
        if self.persist_automation_required().is_ok() {
            let _ = self.ledger.delete_runtime_state(LATE_AUTO_ENTRY_WATCH_KEY);
        }
        self.emit_automation_state();
        true
    }

    /// WebSocket 체결통보가 오지 않아도 정확 주문상태·당일체결·계좌를 함께 대조해
    /// 15:15 tombstone의 늦은 IOC 체결을 Auto pending으로 되살린다. 호출자는 먼저
    /// `refresh_account`에 성공해야 하며, `true`이면 pending 조정을 이어서 깨운다.
    async fn reconcile_late_auto_entry_watch_from_rest(
        &self,
        today_fills: Option<&[crate::broker::BrokerFill]>,
    ) -> bool {
        let watch = match self
            .ledger
            .get_runtime_state::<LateAutoEntryWatch>(LATE_AUTO_ENTRY_WATCH_KEY)
        {
            Ok(Some(watch)) => watch,
            _ => return false,
        };
        if watch.trading_date.trim().is_empty()
            || watch.order_no.trim().is_empty()
            || watch.org_no.trim().is_empty()
        {
            return false;
        }
        let status = match self
            .broker
            .order_status(&watch.trading_date, &watch.order_no)
            .await
        {
            Ok(Some(status)) => status,
            Ok(None) | Err(_) => return false,
        };
        if status.trading_date != watch.trading_date
            || status.order_no != watch.order_no
            || status.org_no != watch.org_no
            || status.code != watch.code
            || status.side != Side::Buy
            || status.ordered_qty != watch.ordered_qty
            || status.ordered_at < watch.requested_at
            || !status.is_terminal()
        {
            return false;
        }
        if status.filled_qty == 0 {
            let _ = self.ledger.delete_runtime_state(LATE_AUTO_ENTRY_WATCH_KEY);
            return false;
        }

        // CCLD_DVSN=01 누적체결도 같은 exact identity로 교차 확인한다. 30초 주기나
        // 시작 조정이 이미 조회한 스냅샷을 넘기면 재사용하고, 직접 호출일 때만 조회한다.
        let matching_fill = if let Some(fills) = today_fills {
            fills
                .iter()
                .filter(|fill| {
                    fill.order_no == watch.order_no
                        && fill.org_no == watch.org_no
                        && fill.code == watch.code
                        && fill.side == Side::Buy
                })
                .max_by_key(|fill| fill.qty)
                .cloned()
        } else {
            self.broker.today_fills().await.ok().and_then(|fills| {
                fills
                    .into_iter()
                    .filter(|fill| {
                        fill.order_no == watch.order_no
                            && fill.org_no == watch.org_no
                            && fill.code == watch.code
                            && fill.side == Side::Buy
                    })
                    .max_by_key(|fill| fill.qty)
            })
        };
        if let Some(fill) = matching_fill.as_ref() {
            if fill.qty != status.filled_qty
                || (fill.price - status.avg_fill_price).abs() > f64::EPSILON
            {
                return false;
            }
        }

        let expected_qty = watch.pending.baseline_qty.saturating_add(status.filled_qty);
        if self.cached_position_qty(&watch.code) != expected_qty {
            return false;
        }
        let event = crate::types::FillEvent {
            fill_id: format!(
                "late-exact:{}:{}:{}:{}",
                status.trading_date, status.order_no, status.filled_qty, status.avg_fill_price
            ),
            order_no: status.order_no,
            original_order_no: status.original_order_no,
            org_no: status.org_no,
            code: status.code,
            side: status.side,
            qty: status.filled_qty,
            price: status.avg_fill_price,
            // 당일 누적체결이 있으면 `infm_tmd`에서 온 체결 반영시각을 보존한다.
            // 아직 조회에 안 보일 때만 정확 주문상태의 주문시각으로 폴백한다.
            filled_at: matching_fill
                .as_ref()
                .map(|fill| fill.filled_at)
                .unwrap_or(status.ordered_at),
            status: "filled".into(),
        };
        let restored = self.restore_late_auto_entry_notice(&event);
        if restored {
            self.apply_reconciled_fill(&event);
        }
        restored
    }

    /// 진입 POST 뒤 ACK·IOC 종결·체결 조회가 일시적으로 불명확해도 pending을
    /// 방치하지 않는다. 주문 actor 뒤에서 조회만 반복하며 절대 매수를 재POST하지 않고,
    /// 목표 보호를 복구하거나 안전 청산 상태가 될 때까지 계속 조정한다.
    fn start_pending_entry_reconcile(self: &Arc<Self>) {
        if self.entry_reconcile_pending.swap(true, Ordering::SeqCst) {
            return;
        }
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                let _actor = engine.order_actor.lock().await;
                let still_pending = {
                    let runtime = engine.automation.lock().unwrap();
                    runtime.mode() == ControlMode::Auto && runtime.pending_entry().is_some()
                };
                if !still_pending {
                    break;
                }

                if engine.pending_entry_market_close_due(engine.automation_now())
                    && engine.market_is_open()
                {
                    if let Err(error) = engine
                        .cancel_known_auto_open_orders("15:15 pending 진입 주문 정리")
                        .await
                    {
                        engine.automation.lock().unwrap().suspend(error);
                    }
                }
                let account_ok = engine.refresh_account().await;
                let open_orders = engine.broker.open_orders().await;
                let today_fills = engine.broker.today_fills().await;
                if account_ok {
                    if let (Ok(open_orders), Ok(mut today_fills)) = (open_orders, today_fills) {
                        let exact_state =
                            match engine
                                .merge_exact_pending_entry_terminal(
                                    &engine.account_snapshot(),
                                    &mut today_fills,
                                )
                                .await
                            {
                                Ok(state) => state,
                                Err(error) => {
                                    engine.automation.lock().unwrap().suspend(format!(
                                        "Auto IOC 정확 주문상태 조정 실패: {error}"
                                    ));
                                    engine.persist_automation();
                                    engine.emit_automation_state();
                                    drop(_actor);
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                    continue;
                                }
                            };
                        if exact_state == Some(false)
                            && !engine.pending_entry_market_close_due(engine.automation_now())
                        {
                            engine.automation.lock().unwrap().suspend(
                                "Auto IOC의 정확한 terminal 상태를 기다리며 pending 조정을 계속합니다",
                            );
                            engine.persist_automation();
                            engine.emit_automation_state();
                            drop(_actor);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                        engine.apply_today_fill_snapshot(&mut today_fills);
                        match engine.reconcile_pending_auto_entry_startup(
                            &engine.account_snapshot(),
                            &open_orders,
                            &today_fills,
                        ) {
                            Ok(()) => {
                                engine.persist_automation();
                                engine.emit_automation_state();
                                let should_exit = engine.automation.lock().unwrap().phase()
                                    == crate::types::AutomationPhase::ExitPending;
                                drop(_actor);
                                if should_exit {
                                    Arc::clone(&engine)
                                        .execute_auto_exit("target_order_failed")
                                        .await;
                                }
                                break;
                            }
                            Err(error) => {
                                if engine.pending_entry_market_close_due(engine.automation_now())
                                    && open_orders.iter().all(|order| {
                                        BrokerOrderKey::from_fake_epoch(
                                            order.ordered_at,
                                            &order.org_no,
                                            &order.order_no,
                                        )
                                        .ok()
                                        .is_none_or(
                                            |key| {
                                                !engine
                                                    .ledger
                                                    .is_auto_broker_order_key(&key)
                                                    .unwrap_or(false)
                                            },
                                        )
                                    })
                                {
                                    match engine.recover_pending_entry_for_flatten(
                                        &engine.account_snapshot(),
                                    ) {
                                        Ok(true) => {
                                            engine.persist_automation();
                                            engine.emit_automation_state();
                                            drop(_actor);
                                            Arc::clone(&engine)
                                                .execute_auto_exit("market_close")
                                                .await;
                                            break;
                                        }
                                        Ok(false) => {
                                            match engine.finish_empty_pending_entry_for_flatten(
                                                &engine.account_snapshot(),
                                            ) {
                                                Ok(true) => {
                                                    engine.persist_automation();
                                                    engine.emit_automation_state();
                                                    break;
                                                }
                                                Ok(false) => {}
                                                Err(finish_error) => engine
                                                    .automation
                                                    .lock()
                                                    .unwrap()
                                                    .suspend(finish_error),
                                            }
                                        }
                                        Err(flatten_error) => {
                                            engine.automation.lock().unwrap().suspend(flatten_error)
                                        }
                                    }
                                } else {
                                    engine.automation.lock().unwrap().suspend(error);
                                }
                                engine.persist_automation();
                                engine.emit_automation_state();
                            }
                        }
                    }
                }
                drop(_actor);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            engine
                .entry_reconcile_pending
                .store(false, Ordering::SeqCst);
            let needs_restart = {
                let runtime = engine.automation.lock().unwrap();
                runtime.mode() == ControlMode::Auto && runtime.pending_entry().is_some()
            };
            if needs_restart {
                engine.start_pending_entry_reconcile();
            }
        });
    }

    /// 저장된 Auto 소유권에 포함되지 않은 양방향 ETF 보유·미체결이 있는지 확인한다.
    /// 같은 종목의 수량 불일치는 호출부의 더 엄격한 잔고 대조에서 별도로 처리한다.
    fn has_unknown_auto_exposure(
        settings: &Settings,
        owned: Option<&OwnedPosition>,
        account: &AccountSnapshot,
        open_orders: &[crate::broker::BrokerOpenOrder],
    ) -> bool {
        let auto_codes = [
            settings.auto_symbols.leverage.as_str(),
            settings.auto_symbols.inverse.as_str(),
        ];
        let unknown_position = account.positions.iter().any(|item| {
            item.qty > 0
                && auto_codes.contains(&item.code.as_str())
                && owned.is_none_or(|position| position.code != item.code)
        });
        let unknown_order = open_orders.iter().any(|order| {
            if order.cancelable_qty == 0 || !auto_codes.contains(&order.code.as_str()) {
                return false;
            }
            let owned_order = owned.is_some_and(|position| {
                position.code == order.code
                    && order.side == Side::Sell
                    && (position.target_order_no.as_deref() == Some(order.order_no.as_str())
                        || position.exit_order_no.as_deref() == Some(order.order_no.as_str()))
            });
            !owned_order
        });
        unknown_position || unknown_order
    }

    /// 시작 뒤 HTS·지연 주문으로 양방향 ETF 노출이 새로 생겨도 다음 OCO 체결까지
    /// 기다리지 않고 현재 그룹을 폐기하고 신규 진입을 중단한다.
    async fn suspend_idle_unknown_auto_exposure(&self) {
        let eligible = {
            let runtime = self.automation.lock().unwrap();
            runtime.mode() == ControlMode::Auto
                && runtime.position().is_none()
                && runtime.pending_entry().is_none()
                && matches!(
                    runtime.phase(),
                    crate::types::AutomationPhase::Idle
                        | crate::types::AutomationPhase::Analyzing
                        | crate::types::AutomationPhase::ArmedOco
                )
        };
        if !eligible {
            return;
        }
        let open_orders = match self.broker.open_orders().await {
            Ok(orders) => orders,
            Err(error) => {
                let _market_guard = self.automation_market_gate.lock().await;
                self.suspend_automation_with_group_ledger_safety(format!(
                    "자동 유휴 상태의 미체결 노출 확인 실패로 신규 진입을 중단합니다: {error}"
                ));
                return;
            }
        };
        let settings = self.settings.read().unwrap().clone();
        if Self::has_unknown_auto_exposure(&settings, None, &self.account_snapshot(), &open_orders)
        {
            let _market_guard = self.automation_market_gate.lock().await;
            self.suspend_automation_with_group_ledger_safety(
                "자동 유휴 중 양방향 ETF에 외부·지연 보유 또는 미체결이 생겨 신규 진입을 중단했습니다",
            );
        }
    }

    async fn reconcile_startup(&self) {
        if !self.refresh_account().await {
            self.automation.lock().unwrap().finish_reconcile(Some(
                "시작 시 계좌 조회 실패로 자동 진입을 중단했습니다".into(),
            ));
            self.persist_automation();
            return;
        }
        let open_orders = match self.broker.open_orders().await {
            Ok(orders) => orders,
            Err(error) => {
                self.automation
                    .lock()
                    .unwrap()
                    .finish_reconcile(Some(format!("미체결 주문 조정 실패: {error}")));
                self.persist_automation();
                return;
            }
        };
        let mut today_fills = match self.broker.today_fills().await {
            Ok(fills) => fills,
            Err(error) => {
                self.automation
                    .lock()
                    .unwrap()
                    .finish_reconcile(Some(format!("당일 체결 조정 실패: {error}")));
                self.persist_automation();
                return;
            }
        };
        // 저장된 tombstone도 WS 통보 없이 exact REST 상태와 계좌로 먼저 복원한다.
        // 이후 공통 pending-entry 재시작 조정이 15:15 market_close 청산까지 이어간다.
        self.reconcile_late_auto_entry_watch_from_rest(Some(&today_fills))
            .await;
        let exact_state = match self
            .merge_exact_pending_entry_terminal(&self.account_snapshot(), &mut today_fills)
            .await
        {
            Ok(state) => state,
            Err(error) => {
                self.automation
                    .lock()
                    .unwrap()
                    .finish_reconcile(Some(format!("진입 정확 주문상태 조정 실패: {error}")));
                self.persist_automation();
                return;
            }
        };
        if exact_state == Some(false) {
            self.automation.lock().unwrap().finish_reconcile(Some(
                "Auto IOC의 정확한 terminal 상태가 아직 확인되지 않았습니다".into(),
            ));
            self.persist_automation();
            return;
        }
        self.apply_today_fill_snapshot(&mut today_fills);

        if let Err(error) = self.reconcile_pending_auto_entry_startup(
            &self.account_snapshot(),
            &open_orders,
            &today_fills,
        ) {
            self.automation
                .lock()
                .unwrap()
                .finish_reconcile(Some(format!("진입 주문 재시작 조정 실패: {error}")));
            self.persist_automation();
            return;
        }

        let settings = self.settings.read().unwrap().clone();
        let account = self.account_snapshot();
        let mut runtime = self.automation.lock().unwrap();
        let mode = runtime.mode();
        let mut suspended = None;

        if runtime.session_id().is_none() && mode != ControlMode::Manual {
            let session_id = unique_id(if mode == ControlMode::Auto {
                "auto"
            } else {
                "shadow"
            });
            let session = NewSession {
                session_id: session_id.clone(),
                execution_kind: if mode == ControlMode::Shadow {
                    LedgerExecutionKind::Shadow
                } else {
                    LedgerExecutionKind::Real
                },
                control_mode: ledger_control_mode(mode),
                starting_cash: Some(account.cash),
                started_at: now_kst_fake_epoch(),
            };
            if let Err(error) = self.ledger.start_session(&session) {
                suspended = Some(format!("자동 세션 장부 생성 실패: {error}"));
            } else {
                runtime.set_session_id(Some(session_id));
            }
        }

        if let Some(position) = runtime.position() {
            if position.shadow {
                // 섀도 상태는 KIS 보유와 독립적이다. 세부 주문은 아래 모드 초기화에서 복원한다.
            } else {
                let actual = account
                    .positions
                    .iter()
                    .find(|item| item.code == position.code);
                let target_exists = position.target_order_no.as_ref().is_some_and(|order_no| {
                    open_orders.iter().any(|order| {
                        &order.order_no == order_no
                            && order.code == position.code
                            && order.side == Side::Sell
                            && order.ordered_qty == position.entry_qty
                            && order.filled_qty == position.entry_qty.saturating_sub(position.qty)
                            && order.cancelable_qty == position.qty
                            && order.price.round() as u64 == position.target_price
                            && position.target_org_no.as_deref() == Some(order.org_no.as_str())
                            && order.cancelable_qty > 0
                    })
                });
                let recovering_exit = position.exit_intent_id.is_some()
                    || runtime.phase() == crate::types::AutomationPhase::ExitPending;
                if Self::has_unknown_auto_exposure(
                    &settings,
                    Some(position),
                    &account,
                    &open_orders,
                ) {
                    suspended = Some(
                        "저장된 Auto 포지션 외의 자동 종목 보유·미체결 주문이 있습니다".into(),
                    );
                } else if actual.is_none_or(|actual| actual.qty != position.qty)
                    || (!recovering_exit && !target_exists)
                {
                    suspended =
                        Some("저장된 Auto 포지션과 KIS 잔고·목표주문이 일치하지 않습니다".into());
                }
            }
        } else if mode == ControlMode::Auto {
            if Self::has_unknown_auto_exposure(&settings, None, &account, &open_orders) {
                suspended = Some(
                    "소유권을 확인할 수 없는 자동 종목 보유·주문이 있어 신규 진입을 중단했습니다"
                        .into(),
                );
            }
        }

        if mode == ControlMode::Shadow && self.shadow.lock().unwrap().is_none() {
            let shadow_cash = runtime.shadow_cash().unwrap_or(account.cash);
            let session = runtime
                .position()
                .filter(|position| position.shadow)
                .map_or_else(
                    || ShadowSession::start(shadow_cash),
                    |position| {
                        ShadowSession::restore(
                            shadow_cash,
                            shadow_cash,
                            crate::automation::shadow::ShadowPosition {
                                product: shadow_product(position.product),
                                code: position.code.clone(),
                                entry_qty: position.entry_qty.max(position.qty),
                                qty: position.qty,
                                entry_vwap: position.avg_price,
                                target_return_pct: position.target_return_pct,
                                target_price: position.target_price,
                                first_fill_time: position.first_fill_at,
                                last_price: position.last_price.round() as u64,
                                profit_guard_armed: position.profit_guard_armed,
                            },
                        )
                    },
                );
            *self.shadow.lock().unwrap() = Some(session);
            runtime.set_shadow_cash(Some(shadow_cash));
        }
        runtime.finish_reconcile(suspended);
        drop(runtime);
        self.persist_automation();
    }

    fn auto_quotes_freshness_error(&self, now: i64) -> Option<String> {
        if !self.connected.load(Ordering::SeqCst) {
            return Some("자동매매 시세 신선도 검사 실패: 실시간 웹소켓 연결 끊김".into());
        }
        let settings = self.settings.read().unwrap();
        let symbols = [
            settings.auto_symbols.underlying.as_str(),
            settings.auto_symbols.leverage.as_str(),
            settings.auto_symbols.inverse.as_str(),
        ];
        let quotes = self.quotes.read().unwrap();
        let mut issues = Vec::new();
        for code in symbols {
            let Some(quote) = quotes.get(code) else {
                issues.push(format!("{code}[시세 없음]"));
                continue;
            };
            let mut quote_issues = Vec::new();
            if !(quote.price > 0.0) {
                quote_issues.push("현재가 오류".to_string());
            }
            if !(quote.ask1 > 0.0) {
                quote_issues.push("매도1호가 오류".to_string());
            }
            if !(quote.bid1 > 0.0) {
                quote_issues.push("매수1호가 오류".to_string());
            }
            if quote.trade_ts <= 0 {
                quote_issues.push("체결 미수신".to_string());
            } else if quote.trade_ts > now {
                quote_issues.push(format!(
                    "체결 시각이 {}초 미래",
                    quote.trade_ts.saturating_sub(now)
                ));
            } else {
                let age = now.saturating_sub(quote.trade_ts);
                if age > QUOTE_FRESH_SECS {
                    quote_issues.push(format!("체결 {age}초 경과"));
                }
            }
            if quote.book_ts <= 0 {
                quote_issues.push("호가 미수신".to_string());
            } else if quote.book_ts > now {
                quote_issues.push(format!(
                    "호가 시각이 {}초 미래",
                    quote.book_ts.saturating_sub(now)
                ));
            } else {
                let age = now.saturating_sub(quote.book_ts);
                if age > QUOTE_FRESH_SECS {
                    quote_issues.push(format!("호가 {age}초 경과"));
                }
            }
            if !quote_issues.is_empty() {
                issues.push(format!("{code}[{}]", quote_issues.join(", ")));
            }
        }
        (!issues.is_empty())
            .then(|| format!("자동매매 시세 신선도 검사 실패: {}", issues.join("; ")))
    }

    fn auto_quotes_fresh(&self, now: i64) -> bool {
        self.auto_quotes_freshness_error(now).is_none()
    }

    fn mark_automation_feed_reset(&self) {
        let now = now_kst_fake_epoch();
        self.automation_trade_journal.lock().unwrap().mark_reset();
        self.automation_feed_reset_epoch
            .fetch_max(now, Ordering::SeqCst);
        self.automation_feed_generation
            .fetch_add(1, Ordering::SeqCst);
    }

    /// 자동매매 종목 중 체결·호가 한쪽 스트림만 완전히 죽은(미수신) 종목을 찾는다.
    /// 반대쪽 스트림이 신선할 때만 half-dead로 본다 — 시장이 조용한 것과
    /// 구독이 죽은 것을 구분하는 기준이다. (예: 000660 체결은 흐르는데 호가는
    /// ack만 성공하고 데이터가 전혀 안 오는 상태)
    fn automation_feed_half_dead(&self, now: i64) -> Option<String> {
        if !self.connected.load(Ordering::SeqCst) {
            return None;
        }
        let settings = self.settings.read().unwrap();
        let symbols = [
            settings.auto_symbols.underlying.as_str(),
            settings.auto_symbols.leverage.as_str(),
            settings.auto_symbols.inverse.as_str(),
        ];
        let quotes = self.quotes.read().unwrap();
        for code in symbols {
            let Some(quote) = quotes.get(code) else {
                continue;
            };
            let trade_fresh =
                quote.trade_ts > 0 && now.saturating_sub(quote.trade_ts) <= QUOTE_FRESH_SECS;
            let book_fresh =
                quote.book_ts > 0 && now.saturating_sub(quote.book_ts) <= QUOTE_FRESH_SECS;
            if trade_fresh && quote.book_ts <= 0 {
                return Some(format!("{code} 체결은 수신 중이나 호가 미수신"));
            }
            if book_fresh && quote.trade_ts <= 0 {
                return Some(format!("{code} 호가는 수신 중이나 체결 미수신"));
            }
        }
        None
    }

    /// 피드 감시견 1초 판정. half-dead가 지속되면 WS 태스크에 전체 재접속
    /// (새 접속키·전체 재구독)을 요청한다. 재구독 30초 루프로도 회복되지 않는
    /// "ack 성공·데이터 무송신" 구독의 유일한 회복 경로다.
    fn feed_watchdog_tick(&self, now: i64) {
        let Some(reason) = self.automation_feed_half_dead(now) else {
            self.feed_half_dead_since.store(0, Ordering::SeqCst);
            return;
        };
        let since = self.feed_half_dead_since.load(Ordering::SeqCst);
        if since == 0 {
            self.feed_half_dead_since.store(now, Ordering::SeqCst);
            return;
        }
        if now.saturating_sub(since) < FEED_HALF_DEAD_RECONNECT_SECS {
            return;
        }
        let last_fired = self.feed_watchdog_last_fired.load(Ordering::SeqCst);
        if last_fired > 0 && now.saturating_sub(last_fired) < FEED_WATCHDOG_MIN_GAP_SECS {
            return;
        }
        self.feed_watchdog_last_fired.store(now, Ordering::SeqCst);
        self.feed_half_dead_since.store(0, Ordering::SeqCst);
        tracing::warn!(
            "피드 감시견: {reason} 상태가 {FEED_HALF_DEAD_RECONNECT_SECS}초 지속 — 웹소켓 전체 재접속(새 접속키·전체 재구독)을 요청합니다"
        );
        self.feed_reconnect.notify_one();
    }

    /// 재연결 직후 이전 연결의 10초 이내 시세가 우연히 신선해 보이는 것을 막는다.
    /// 가격 표시는 보존하되 세 종목 모두 새 체결·호가를 받은 뒤에만 LLM 입력을 허용한다.
    fn invalidate_automation_quote_freshness(&self) {
        let settings = self.settings.read().unwrap();
        let codes = [
            settings.auto_symbols.underlying.as_str(),
            settings.auto_symbols.leverage.as_str(),
            settings.auto_symbols.inverse.as_str(),
        ];
        let mut quotes = self.quotes.write().unwrap();
        for code in codes {
            if let Some(quote) = quotes.get_mut(code) {
                quote.volume = 0.0;
                quote.trade_sequence = 0;
                quote.received_at_micros = 0;
                quote.trade_ts = 0;
                quote.book_ts = 0;
            }
        }
    }

    fn reset_confirmation_for_market_gap(&self, code: &str, stream: &str) {
        let settings = self.settings.read().unwrap();
        let is_auto_symbol = code == settings.auto_symbols.underlying
            || code == settings.auto_symbols.leverage
            || code == settings.auto_symbols.inverse;
        drop(settings);
        if !is_auto_symbol {
            return;
        }
        self.mark_automation_feed_reset();
        if self
            .automation
            .lock()
            .unwrap()
            .reset_confirmation(self.monotonic_now())
        {
            tracing::info!("자동 OCO 확인 초기화: {code} {stream} 시세 공백");
            self.sync_scenario_ledger();
            self.emit_automation_state();
        }
    }

    fn handle_automation_quote(self: &Arc<Self>, quote: &Quote) {
        let now_epoch = now_kst_fake_epoch();
        let settings = self.settings.read().unwrap().clone();

        // 실제 체결로 식별되는 틱만 수익 보호의 무장·발동에 사용한다.
        // 가격은 유한·양수까지 확인한다. 프레임 필드가 비거나 깨져 0이 되면 손절
        // 판정이 -100%로 뒤집히고, "inf"로 파싱되면(f64::parse는 이를 받는다)
        // 호가 반올림이 포화해 목표가 계산까지 오염된다.
        let valid_trade_tick = quote.volume > 0.0
            && quote.price.is_finite()
            && quote.price > 0.0
            && quote.trade_sequence > 0
            && quote.received_at_micros > 0;
        let owned = valid_trade_tick
            .then(|| {
                let mut runtime = self.automation.lock().unwrap();
                runtime
                    .update_position_price(&quote.code, quote.price)
                    .map(|update| (runtime.mode(), runtime.position().cloned(), update))
            })
            .flatten();
        if let Some((mode, Some(position), update)) = owned {
            if mode == ControlMode::Auto && update.guard_armed_changed {
                self.persist_automation();
            }
            self.emit_automation_state();
            if mode == ControlMode::Shadow {
                self.handle_shadow_trade(quote);
            } else if mode == ControlMode::Auto {
                let reason =
                    if schedule::has_reached_position_flatten(position.first_fill_at, now_epoch) {
                        Some("market_close")
                    } else if update.rate <= crate::util::AUTO_STOP_LOSS_PCT {
                        Some("stop_loss")
                    } else if update.profit_guard_triggered {
                        Some("profit_guard")
                    } else if now_epoch >= position.exit_deadline {
                        Some("max_holding")
                    } else {
                        None
                    };
                if let Some(reason) = reason {
                    let (should_exit, state_changed) = {
                        let mut runtime = self.automation.lock().unwrap();
                        let revision = runtime.revision();
                        let should_exit = if reason == "market_close" {
                            runtime.begin_market_close_exit().is_some()
                        } else {
                            runtime.begin_exit(reason).is_some()
                        };
                        (should_exit, runtime.revision() != revision)
                    };
                    if state_changed {
                        self.persist_automation();
                        self.emit_automation_state();
                    }
                    if should_exit {
                        let engine = Arc::clone(self);
                        tokio::spawn(async move { engine.execute_auto_exit(reason).await });
                    }
                }
            }
        }

        if quote.code != settings.auto_symbols.underlying || !valid_trade_tick {
            // 포지션 수익률과 같은 기준을 OCO에도 적용한다. 가격이 0·음수·NaN·무한대인
            // 틱은 확인 횟수나 무효화 판정을 바꾸지 않는다.
            return;
        }
        // 휴장 전 장부 종결이 일시 실패하면 runtime 그룹은 다음 tick의 재시도를
        // 위해 보존한다. 그 사이 장외 틱이 신규 진입을 확정하지 못하게 실행만 막는다.
        if !self.market_is_open() {
            return;
        }
        if !self.auto_quotes_fresh(now_epoch) {
            self.mark_automation_feed_reset();
            if self
                .automation
                .lock()
                .unwrap()
                .reset_confirmation(self.monotonic_now())
            {
                self.sync_scenario_ledger();
                self.emit_automation_state();
            }
            return;
        }

        let triggered = self.automation.lock().unwrap().on_trade_tick(TradeTick {
            sequence: quote.trade_sequence,
            price: quote.price.round() as u64,
            volume: quote.volume.max(0.0).round() as u64,
            at: Duration::from_micros(quote.received_at_micros),
            epoch: quote.trade_ts,
        });
        self.sync_scenario_ledger();
        self.emit_automation_state();
        if let Some(triggered) = triggered {
            let engine = Arc::clone(self);
            tokio::spawn(async move { engine.execute_triggered_entry(triggered).await });
        }
    }

    fn handle_automation_book(self: &Arc<Self>, quote: &Quote) {
        let mode = self.automation.lock().unwrap().mode();
        if mode == ControlMode::Shadow {
            self.handle_shadow_book(quote);
        }
    }

    fn record_feed_fill(&self, fill: &crate::types::FillEvent) -> bool {
        if fill.qty == 0 || fill.price <= 0.0 {
            return false;
        }
        let order_key =
            match BrokerOrderKey::from_fake_epoch(fill.filled_at, &fill.org_no, &fill.order_no) {
                Ok(key) => key,
                Err(error) => {
                    tracing::error!("체결 주문 식별자 생성 실패({}): {error}", fill.code);
                    return false;
                }
            };
        let settings = self.settings.read().unwrap();
        let runtime = self.automation.lock().unwrap();
        let pending_entry = runtime.pending_entry().cloned();
        let runtime_order_owned = runtime.position().is_some_and(|position| {
            !position.shadow
                && position.code == fill.code
                && (position.target_order_no.as_deref() == Some(fill.order_no.as_str())
                    || position.exit_order_no.as_deref() == Some(fill.order_no.as_str()))
        });
        let ledger_order_owned = self
            .ledger
            .is_auto_broker_order_key(&order_key)
            .unwrap_or(false);
        let pending_order_owned = pending_entry.is_some_and(|pending| {
            let pending_code = pending.product.code(&settings.auto_symbols);
            self.ledger
                .get_order(&pending.intent_id)
                .ok()
                .flatten()
                .is_some_and(|order| {
                    fill.side == Side::Buy
                        && fill.code == pending_code
                        && order
                            .broker_order_id
                            .as_deref()
                            .is_none_or(|order_no| order_no == fill.order_no)
                        && fill.filled_at >= order.requested_at
                        && fill.qty <= order.qty
                        && order.price.is_none_or(|limit| fill.price <= limit as f64)
                })
        });
        let auto_owned = runtime.mode() == ControlMode::Auto
            && (ledger_order_owned || runtime_order_owned || pending_order_owned);
        drop(runtime);
        drop(settings);
        let origin = if auto_owned {
            LedgerOrigin::Auto
        } else {
            LedgerOrigin::Manual
        };
        let fill_key = if fill.fill_id.is_empty() {
            format!(
                "{}:{}:{}:{}:{}",
                fill.order_no, fill.code, fill.filled_at, fill.qty, fill.price
            )
        } else {
            fill.fill_id.clone()
        };
        let inserted = match self.ledger.record_fill_for_broker_order(
            &order_key,
            &NewFill {
                fill_key,
                intent_id: None,
                broker_order_id: (!fill.order_no.is_empty()).then(|| fill.order_no.clone()),
                broker_fill_id: (!fill.fill_id.is_empty()).then(|| fill.fill_id.clone()),
                execution_kind: LedgerExecutionKind::Real,
                origin,
                code: fill.code.clone(),
                side: ledger_side(fill.side),
                qty: fill.qty,
                price: fill.price,
                filled_at: fill.filled_at,
                raw_json: serde_json::to_string(fill).ok(),
            },
        ) {
            Ok(inserted) => inserted,
            Err(error) => {
                tracing::error!("체결 장부 기록 실패({}): {error}", fill.code);
                false
            }
        };
        if inserted && !auto_owned {
            self.record_manual_trade_fill(fill);
        }
        inserted
    }

    /// KIS REST는 주문별 누적 수량·평균가를 돌려준다. 같은 주문의 이전 누적
    /// 수량·대금을 빼서 이번 구간의 정확한 delta 수량·평균가만 장부에 기록한다.
    /// WebSocket 원문은 이 함수에 들어오지 않고 REST 조정을 깨우는 역할만 한다.
    fn record_reconciled_fill(
        &self,
        fill: &crate::types::FillEvent,
    ) -> Option<crate::types::FillEvent> {
        let _ingest = self.fill_ingest_lock.lock().unwrap();
        let order_key =
            BrokerOrderKey::from_fake_epoch(fill.filled_at, &fill.org_no, &fill.order_no).ok()?;
        let (existing_qty, existing_value) =
            match self.ledger.fill_totals_for_broker_order(&order_key) {
                Ok(existing) => existing,
                Err(error) => {
                    tracing::error!(
                        "기존 체결누적 조회 실패({}/{}): {error}",
                        fill.code,
                        fill.order_no
                    );
                    return None;
                }
            };
        if existing_qty >= fill.qty {
            return None;
        }
        let delta_qty = fill.qty - existing_qty;
        let cumulative_value = fill.price * fill.qty as f64;
        let delta_value = cumulative_value - existing_value;
        let delta_price = if delta_value.is_finite() && delta_value > 0.0 {
            delta_value / delta_qty as f64
        } else {
            tracing::warn!(
                "누적 체결대금 역산값이 유효하지 않아 REST 평균가를 사용합니다({}/{}): 누적={cumulative_value}, 기존={existing_value}",
                fill.code,
                fill.order_no
            );
            fill.price
        };
        let mut delta = fill.clone();
        delta.qty = delta_qty;
        delta.price = delta_price;
        delta.fill_id = format!("reconcile:{}:{}:{}", fill.org_no, fill.order_no, fill.qty);
        delta.status = "reconciled".into();
        self.record_feed_fill(&delta).then_some(delta)
    }

    /// REST에서 확정한 경제 체결 delta를 런타임·예약·장부에 정확히 한 번 적용한다.
    fn apply_reconciled_fill(&self, fill: &crate::types::FillEvent) -> bool {
        let Some(delta) = self.record_reconciled_fill(fill) else {
            return false;
        };
        self.handle_automation_fill(&delta);
        if delta.side == Side::Sell {
            if let Some(info) = self.on_sell_fill(&delta.code, delta.qty) {
                self.emit("reservation", &info);
            }
        }
        true
    }

    /// 한 번 조회한 당일 REST 누적체결을 시간순으로 공통 delta 경로에 흘린다.
    /// 누적 수량·평균가가 같은 스냅샷을 반복 적용해도 새 경제 체결은 생기지 않는다.
    fn apply_today_fill_snapshot(&self, fills: &mut [crate::broker::BrokerFill]) -> bool {
        fills.sort_by_key(|fill| fill.filled_at);
        let mut restored_late_entry = false;
        for fill in fills {
            let event = crate::types::FillEvent {
                fill_id: fill.fill_id.clone(),
                order_no: fill.order_no.clone(),
                original_order_no: fill.original_order_no.clone(),
                org_no: fill.org_no.clone(),
                code: fill.code.clone(),
                side: fill.side,
                qty: fill.qty,
                price: fill.price,
                filled_at: fill.filled_at,
                status: "filled".into(),
            };
            restored_late_entry |= self.restore_late_auto_entry_notice(&event);
            self.claim_pending_auto_sell_notice(&event);
            self.apply_reconciled_fill(&event);
        }
        restored_late_entry
    }

    /// 주문 응답보다 체결 알림이 먼저 올 수 있으므로, 엄격히 일치하는 Auto 매도
    /// intent 하나에 한해서 주문번호를 선결합한다. 실제 수량·가격 적용은 REST만 한다.
    fn claim_pending_auto_sell_notice(&self, fill: &crate::types::FillEvent) {
        if fill.side != Side::Sell
            || fill.order_no.trim().is_empty()
            || fill.qty == 0
            || fill.price <= 0.0
        {
            return;
        }
        let position = {
            let runtime = self.automation.lock().unwrap();
            if runtime.mode() != ControlMode::Auto {
                return;
            }
            runtime.position().cloned()
        };
        let Some(position) =
            position.filter(|position| !position.shadow && position.code == fill.code)
        else {
            return;
        };

        let mut matches = Vec::new();
        if let Some(intent_id) = position.target_intent_id.as_deref() {
            matches.push((true, intent_id.to_string()));
        }
        if let Some(intent_id) = position.exit_intent_id.as_deref() {
            matches.push((false, intent_id.to_string()));
        }
        let matches = matches
            .into_iter()
            .filter_map(|(target, intent_id)| {
                let order = self.ledger.get_order(&intent_id).ok().flatten()?;
                let status_ok = matches!(
                    order.status,
                    LedgerOrderStatus::Dispatching
                        | LedgerOrderStatus::Submitted
                        | LedgerOrderStatus::PartiallyFilled
                        | LedgerOrderStatus::Unknown
                );
                let identity_ok = order
                    .broker_order_id
                    .as_deref()
                    .is_none_or(|order_no| order_no == fill.order_no);
                let org_identity_ok = order
                    .broker_org_no
                    .as_deref()
                    .is_none_or(|org_no| fill.org_no.trim().is_empty() || org_no == fill.org_no);
                let shape_ok = order.origin == LedgerOrigin::Auto
                    && order.execution_kind == LedgerExecutionKind::Real
                    && order.side == LedgerSide::Sell
                    && order.code == fill.code
                    && order.requested_at <= fill.filled_at
                    && order.qty >= fill.qty
                    && status_ok
                    && identity_ok
                    && org_identity_ok;
                let price_ok = if target {
                    order.order_type == LedgerOrderType::Limit
                        && order.price.is_some_and(|price| fill.price >= price as f64)
                } else {
                    order.order_type == LedgerOrderType::Market
                };
                (shape_ok && price_ok).then_some((target, order))
            })
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            return;
        }
        let (target, order) = &matches[0];
        let org_no = (!fill.org_no.trim().is_empty()).then(|| fill.org_no.clone());
        let ack_status = match order.status {
            LedgerOrderStatus::Dispatching | LedgerOrderStatus::Unknown => {
                LedgerOrderStatus::Submitted
            }
            // 주기 REST 스냅샷이 같은 누적체결을 다시 읽어도 PartiallyFilled를
            // Submitted로 되돌리지 않는다.
            status => status,
        };
        if self
            .ledger
            .record_order_ack(
                &order.intent_id,
                &OrderAcknowledgement {
                    broker_order_id: Some(fill.order_no.clone()),
                    broker_org_no: org_no.clone(),
                    original_order_id: None,
                    status: ack_status,
                    message: Some("WebSocket 체결 통보로 주문번호 선결합".into()),
                    acknowledged_at: now_kst_fake_epoch(),
                },
            )
            .is_err()
        {
            return;
        }
        let bound = {
            let mut runtime = self.automation.lock().unwrap();
            let current_matches = runtime.position().is_some_and(|current| {
                current.code == fill.code
                    && if *target {
                        current.target_intent_id.as_deref() == Some(order.intent_id.as_str())
                    } else {
                        current.exit_intent_id.as_deref() == Some(order.intent_id.as_str())
                    }
            });
            if !current_matches {
                false
            } else if *target {
                runtime.bind_target_order(fill.order_no.clone(), org_no.clone().unwrap_or_default())
            } else {
                let current = runtime.position().cloned().unwrap();
                if current
                    .exit_order_no
                    .as_deref()
                    .is_some_and(|order_no| order_no != fill.order_no)
                {
                    false
                } else {
                    runtime.set_active_exit_order(
                        order.intent_id.clone(),
                        Some(fill.order_no.clone()),
                        org_no,
                        order.requested_at,
                    )
                }
            }
        };
        if bound {
            self.persist_automation();
            self.emit_automation_state();
        }
    }

    /// WebSocket 체결 통보를 주문별 단일비행 REST 조정으로 바꾼다.
    ///
    /// 통보에는 체결 건별 고유 ID와 조직번호가 없으므로 통보 수량을 누적 목표로
    /// 더하지 않는다. 모든 수신 건은 감사 장부에 별도로 남기고, 경제 체결은
    /// 거래일·조직번호·주문번호가 확정된 REST 누적값의 delta만 반영한다.
    fn start_fill_notice_reconcile(self: &Arc<Self>, notice: crate::types::FillEvent) {
        if notice.order_no.trim().is_empty()
            || notice.code.trim().is_empty()
            || notice.qty == 0
            || !notice.price.is_finite()
            || notice.price <= 0.0
        {
            return;
        }
        let trading_date = match crate::util::trading_date_from_fake_epoch(notice.filled_at) {
            Ok(date) => date,
            Err(error) => {
                tracing::warn!(
                    "체결 통보 거래일 변환 실패({}/{}): {error}",
                    notice.code,
                    notice.order_no
                );
                return;
            }
        };
        // 15:15 무노출 종결과 통보 등록 사이에 틈이 생기지 않게 REST delta 기록과
        // 같은 잠금을 사용한다. 잠금 순서는 항상 ingest -> ledger -> pending이다.
        let _ingest = self.fill_ingest_lock.lock().unwrap();

        let ledger_side = ledger_side(notice.side);
        let mut initial_org_no = (!notice.org_no.trim().is_empty()).then(|| notice.org_no.clone());
        if initial_org_no.is_none() {
            match self.ledger.broker_order_org_candidates(
                &trading_date,
                &notice.order_no,
                &notice.code,
                ledger_side,
            ) {
                Ok(candidates) if candidates.len() == 1 => {
                    initial_org_no = candidates.into_iter().next();
                }
                Ok(candidates) if candidates.len() > 1 => tracing::warn!(
                    "체결 통보 주문 조직번호 후보가 여러 개라 REST 유일 후보를 기다립니다({}/{}/{}): {:?}",
                    trading_date,
                    notice.code,
                    notice.order_no,
                    candidates
                ),
                Ok(_) => {}
                Err(error) => tracing::warn!(
                    "체결 통보 주문 조직번호 후보 조회 실패({}/{}/{}): {error}",
                    trading_date,
                    notice.code,
                    notice.order_no
                ),
            }
        }

        // source_notice_id가 같아도 별도 수신은 별도 행이다. H0STCNI0의 동일초
        // 동일수량·가격 복수 체결과 전송 중복을 여기서 임의로 구분하지 않는다.
        if let Err(error) = self.ledger.record_fill_notice(&NewFillNotice {
            notice_id: unique_id("fill-notice"),
            source_notice_id: (!notice.fill_id.trim().is_empty()).then(|| notice.fill_id.clone()),
            broker_order_id: notice.order_no.clone(),
            broker_org_no: initial_org_no.clone(),
            code: notice.code.clone(),
            side: ledger_side,
            qty: notice.qty,
            price: notice.price,
            notified_at: notice.filled_at,
            received_at: now_kst_fake_epoch(),
            raw_json: serde_json::to_string(&notice).ok(),
        }) {
            tracing::error!(
                "체결 통보 감사 장부 기록 실패({}/{}): {error}",
                notice.code,
                notice.order_no
            );
        }

        let unresolved_key = format!(
            "{}:?:{}:{}:{}",
            trading_date,
            notice.order_no,
            notice.code,
            if notice.side == Side::Buy {
                "buy"
            } else {
                "sell"
            }
        );
        let exact_key = initial_org_no.as_ref().map(|org_no| {
            format!(
                "{}:{}:{}:{}:{}",
                trading_date,
                org_no,
                notice.order_no,
                notice.code,
                if notice.side == Side::Buy {
                    "buy"
                } else {
                    "sell"
                }
            )
        });
        let (key, should_start) = {
            let mut pending = self.fill_reconcile_pending.lock().unwrap();
            // 조직번호를 결합하기 전에 시작된 단일비행이 있으면 그 상태에 합류한다.
            let key = if pending.contains_key(&unresolved_key) {
                unresolved_key
            } else {
                exact_key.unwrap_or(unresolved_key)
            };
            if let Some(state) = pending.get_mut(&key) {
                if let Some(org_no) = initial_org_no {
                    if state.org_no.as_deref().is_some_and(|bound| bound != org_no) {
                        tracing::error!(
                            "같은 체결 조정 상태에 다른 주문조직번호가 들어왔습니다({key}): {:?} != {org_no}",
                            state.org_no
                        );
                        return;
                    }
                    state.org_no = Some(org_no);
                }
                state.generation = state.generation.saturating_add(1);
                if state.running {
                    (key, false)
                } else {
                    state.running = true;
                    (key, true)
                }
            } else {
                pending.insert(
                    key.clone(),
                    FillReconcileState {
                        generation: 1,
                        running: true,
                        trading_date: trading_date.clone(),
                        order_no: notice.order_no.clone(),
                        org_no: initial_org_no,
                        code: notice.code.clone(),
                        side: notice.side,
                    },
                );
                (key, true)
            }
        };
        if !should_start {
            return;
        }
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            let mut last_snapshot: Option<(String, u64, u64, i64)> = None;
            let mut stable_reads = 0_u8;
            let mut observed_generation = 0_u64;
            let mut generation_seen_at = tokio::time::Instant::now();
            let mut attempts = 0_u64;
            let mut warned = false;
            loop {
                if attempts > 0 {
                    let delay = if attempts < 8 {
                        Duration::from_millis(250)
                    } else if attempts < 30 {
                        Duration::from_secs(1)
                    } else {
                        Duration::from_secs(2)
                    };
                    tokio::time::sleep(delay).await;
                }
                attempts = attempts.saturating_add(1);

                let (generation, state_date, state_order_no, bound_org_no, state_code, state_side) = {
                    let pending = engine.fill_reconcile_pending.lock().unwrap();
                    let Some(state) = pending.get(&key) else {
                        return;
                    };
                    (
                        state.generation,
                        state.trading_date.clone(),
                        state.order_no.clone(),
                        state.org_no.clone(),
                        state.code.clone(),
                        state.side,
                    )
                };
                if generation != observed_generation {
                    observed_generation = generation;
                    generation_seen_at = tokio::time::Instant::now();
                    attempts = 1;
                    stable_reads = 0;
                    last_snapshot = None;
                    warned = false;
                }
                if attempts >= FILL_RECONCILE_MAX_ATTEMPTS {
                    let message = {
                        let mut pending = engine.fill_reconcile_pending.lock().unwrap();
                        let Some(state) = pending.get_mut(&key) else {
                            return;
                        };
                        if state.generation != observed_generation {
                            continue;
                        }
                        state.running = false;
                        format!(
                            "WebSocket 체결({}/{})의 정확한 REST 누적체결을 제한 시간 안에 확인하지 못했습니다",
                            state.code, state.order_no
                        )
                    };
                    *engine.last_error.lock().unwrap() = message.clone();
                    engine.emit("engine-error", &message);
                    return;
                }

                let matching = match engine.broker.today_fills().await {
                    Ok(fills) => {
                        let candidates = fills
                            .into_iter()
                            .filter(|fill| {
                                fill.order_no == state_order_no
                                    && fill.code == state_code
                                    && fill.side == state_side
                                    && !fill.org_no.trim().is_empty()
                                    && crate::util::trading_date_from_fake_epoch(fill.filled_at)
                                        .ok()
                                        .as_deref()
                                        == Some(state_date.as_str())
                            })
                            .collect::<Vec<_>>();
                        let selected_org = if let Some(org_no) = bound_org_no.as_deref() {
                            Some(org_no.to_string())
                        } else {
                            let orgs = candidates
                                .iter()
                                .map(|fill| fill.org_no.clone())
                                .collect::<HashSet<_>>();
                            if orgs.len() == 1 {
                                orgs.into_iter().next()
                            } else {
                                if orgs.len() > 1 && !warned {
                                    warned = true;
                                    tracing::error!(
                                        "체결 통보 REST 조정의 주문조직번호가 모호합니다({key}): {:?}",
                                        orgs
                                    );
                                }
                                None
                            }
                        };
                        selected_org.and_then(|org_no| {
                            candidates
                                .into_iter()
                                .filter(|fill| fill.org_no == org_no)
                                .max_by_key(|fill| fill.qty)
                        })
                    }
                    Err(error) => {
                        tracing::warn!("체결 통보 REST 조정 재시도({key}): {error}");
                        None
                    }
                };
                let Some(fill) = matching else {
                    continue;
                };

                {
                    let mut pending = engine.fill_reconcile_pending.lock().unwrap();
                    let Some(state) = pending.get_mut(&key) else {
                        return;
                    };
                    match state.org_no.as_deref() {
                        Some(org_no) if org_no != fill.org_no => {
                            tracing::error!(
                                "체결 통보 조정 중 주문조직번호가 바뀌었습니다({key}): {org_no} != {}",
                                fill.org_no
                            );
                            continue;
                        }
                        None => state.org_no = Some(fill.org_no.clone()),
                        _ => {}
                    }
                }
                let event = crate::types::FillEvent {
                    fill_id: fill.fill_id,
                    order_no: fill.order_no,
                    original_order_no: fill.original_order_no,
                    org_no: fill.org_no,
                    code: fill.code,
                    side: fill.side,
                    qty: fill.qty,
                    price: fill.price,
                    filled_at: fill.filled_at,
                    status: "filled".into(),
                };
                // 원 WebSocket 통보에는 조직번호가 없다. REST 누적체결에서 정확한
                // 거래일·조직번호·주문번호를 얻은 뒤에만 15:15 tombstone을 복원한다.
                let restored_late_entry = engine.restore_late_auto_entry_notice(&event);
                engine.apply_reconciled_fill(&event);
                if restored_late_entry {
                    engine.start_pending_entry_reconcile();
                }
                let order_key =
                    match BrokerOrderKey::new(&state_date, &event.org_no, &event.order_no) {
                        Ok(key) => key,
                        Err(error) => {
                            tracing::warn!("체결 통보 정확 주문키 생성 실패({key}): {error}");
                            continue;
                        }
                    };
                let applied_qty = match engine.ledger.filled_qty_for_broker_order(&order_key) {
                    Ok(qty) => qty,
                    Err(error) => {
                        tracing::warn!("체결 통보 반영수량 재조회 실패({key}): {error}");
                        continue;
                    }
                };
                if applied_qty != event.qty {
                    stable_reads = 0;
                    last_snapshot = None;
                    continue;
                }

                let snapshot = (
                    event.org_no.clone(),
                    event.qty,
                    event.price.to_bits(),
                    event.filled_at,
                );
                if last_snapshot.as_ref() == Some(&snapshot) {
                    stable_reads = stable_reads.saturating_add(1);
                    if stable_reads >= 2
                        && generation_seen_at.elapsed()
                            >= Duration::from_secs(FILL_RECONCILE_SETTLE_SECS)
                    {
                        let mut pending = engine.fill_reconcile_pending.lock().unwrap();
                        let Some(state) = pending.get_mut(&key) else {
                            return;
                        };
                        if state.generation != observed_generation {
                            stable_reads = 0;
                            last_snapshot = None;
                            continue;
                        }
                        state.running = false;
                        return;
                    }
                } else {
                    last_snapshot = Some(snapshot);
                    stable_reads = 1;
                }
            }
        });
    }

    fn record_manual_trade_fill(&self, fill: &crate::types::FillEvent) {
        let key = format!("manual_trade:{}", fill.code);
        let mut accumulator = self
            .ledger
            .get_runtime_state::<ManualTradeAccumulator>(&key)
            .ok()
            .flatten();
        match fill.side {
            Side::Buy => {
                if accumulator
                    .as_ref()
                    .is_none_or(|trade| trade.remaining() == 0)
                {
                    accumulator = Some(ManualTradeAccumulator {
                        trade_id: unique_id("manual-trade"),
                        code: fill.code.clone(),
                        origin: LedgerOrigin::Manual,
                        entry_qty: 0,
                        entry_value: 0.0,
                        exit_qty: 0,
                        exit_value: 0.0,
                        entered_at: fill.filled_at,
                    });
                }
                let trade = accumulator.as_mut().expect("위에서 수동 거래 생성");
                trade.entry_qty = trade.entry_qty.saturating_add(fill.qty);
                trade.entry_value += fill.price * fill.qty as f64;
            }
            Side::Sell => {
                let Some(trade) = accumulator.as_mut() else {
                    // 앱 밖에서 시작된 보유의 원가를 확정할 수 없으므로 체결 장부만 남긴다.
                    return;
                };
                let applied = fill.qty.min(trade.remaining());
                trade.exit_qty = trade.exit_qty.saturating_add(applied);
                trade.exit_value += fill.price * applied as f64;
            }
        }
        let Some(trade) = accumulator else { return };
        let entry_avg = if trade.entry_qty > 0 {
            trade.entry_value / trade.entry_qty as f64
        } else {
            return;
        };
        let closed = trade.remaining() == 0;
        let exit_avg = (trade.exit_qty > 0).then(|| trade.exit_value / trade.exit_qty as f64);
        let pnl_rate = closed.then(|| (exit_avg.unwrap_or(entry_avg) / entry_avg - 1.0) * 100.0);
        let _ = self.ledger.upsert_trade(&NewTrade {
            trade_id: trade.trade_id.clone(),
            session_id: None,
            execution_kind: LedgerExecutionKind::Real,
            origin: trade.origin,
            code: trade.code.clone(),
            entry_qty: trade.entry_qty,
            exit_qty: trade.exit_qty,
            entry_avg_price: entry_avg,
            exit_avg_price: if closed { exit_avg } else { None },
            pnl_rate,
            entered_at: trade.entered_at,
            exited_at: closed.then_some(fill.filled_at),
            status: if closed {
                LedgerTradeStatus::Closed
            } else {
                LedgerTradeStatus::Open
            },
            exit_reason: closed.then(|| "manual".into()),
            updated_at: fill.filled_at,
        });
        let _ = self.ledger.set_runtime_state(&key, &trade, fill.filled_at);
        self.emit("trade-recorded", &serde_json::json!({ "kind": "real" }));
    }

    fn record_manual_order_intent(
        &self,
        code: &str,
        side: LedgerSide,
        order_type: LedgerOrderType,
        qty: u64,
        price: Option<u64>,
    ) -> Result<(String, i64), String> {
        let intent_id = unique_id("manual-order");
        let local_id = self
            .ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: intent_id.clone(),
                session_id: None,
                trade_id: None,
                decision_id: None,
                scenario_product: None,
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Manual,
                code: code.to_string(),
                side,
                order_type,
                qty,
                price,
                requested_at: now_kst_fake_epoch(),
            })
            .map_err(|error| error.to_string())?;
        Ok((intent_id, local_id))
    }

    fn record_broker_ack(
        &self,
        intent_id: &str,
        ack: &crate::broker::OrderAck,
        status: LedgerOrderStatus,
    ) {
        if let Err(error) = self.ledger.record_order_ack(
            intent_id,
            &OrderAcknowledgement {
                broker_order_id: Some(ack.order_no.clone()),
                broker_org_no: Some(ack.org_no.clone()),
                original_order_id: None,
                status,
                message: Some(ack.message.clone()),
                acknowledged_at: now_kst_fake_epoch(),
            },
        ) {
            tracing::error!("주문 ACK 장부 기록 실패: {error}");
        }
    }

    fn record_broker_error(&self, intent_id: &str, message: &str, ambiguous: bool) {
        let _ = self.ledger.record_order_ack(
            intent_id,
            &OrderAcknowledgement {
                broker_order_id: None,
                broker_org_no: None,
                original_order_id: None,
                status: if ambiguous {
                    LedgerOrderStatus::Unknown
                } else {
                    LedgerOrderStatus::Rejected
                },
                message: Some(message.to_string()),
                acknowledged_at: now_kst_fake_epoch(),
            },
        );
    }

    fn record_auto_cancel_intent(
        &self,
        position: &OwnedPosition,
        reason: &str,
    ) -> Result<String, String> {
        let intent_id = unique_id("auto-cancel");
        let runtime = self.automation.lock().unwrap();
        self.ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: intent_id.clone(),
                session_id: runtime.session_id().map(str::to_owned),
                trade_id: Some(position.trade_id.clone()),
                decision_id: runtime.decision_key().map(str::to_owned),
                scenario_product: Some(ledger_product(position.product)),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: position.code.clone(),
                side: LedgerSide::Sell,
                order_type: LedgerOrderType::Cancel,
                qty: position.qty.max(1),
                price: Some(position.target_price),
                requested_at: now_kst_fake_epoch(),
            })
            .map_err(|error| format!("{reason}: {error}"))?;
        Ok(intent_id)
    }

    fn sync_scenario_ledger(&self) {
        let market_day = self.effective_market_day_state();
        let (decision_key, snapshot) = {
            let settings = self.settings.read().unwrap();
            let runtime = self.automation.lock().unwrap();
            (
                runtime.decision_key().map(str::to_owned),
                runtime.snapshot(
                    &self.runtime_id,
                    self.runtime_generation,
                    &settings.auto_symbols,
                    self.monotonic_now(),
                    market_day.status,
                    market_day.message.clone(),
                ),
            )
        };
        let Some(decision_key) = decision_key else {
            return;
        };
        let now = now_kst_fake_epoch();
        let mut fingerprint = decision_key.clone();
        for scenario in &snapshot.scenarios {
            write!(
                &mut fingerprint,
                "|{:?}:{:?}:{}:{:?}",
                scenario.product,
                scenario.status,
                scenario.confirming_ticks,
                scenario.reference_observed_at
            )
            .expect("String 쓰기는 실패하지 않음");
        }
        let decision_status = if snapshot
            .scenarios
            .iter()
            .any(|scenario| scenario.status == crate::types::ScenarioStatus::Triggered)
        {
            Some(LedgerDecisionStatus::Triggered)
        } else if !snapshot.scenarios.is_empty() {
            let no_active = snapshot.scenarios.iter().all(|scenario| {
                !matches!(
                    scenario.status,
                    crate::types::ScenarioStatus::Armed | crate::types::ScenarioStatus::Confirming
                )
            });
            if no_active {
                let status =
                    if snapshot.scenarios.iter().any(|scenario| {
                        scenario.status == crate::types::ScenarioStatus::Invalidated
                    }) {
                        Some(LedgerDecisionStatus::Invalidated)
                    } else if snapshot
                        .scenarios
                        .iter()
                        .all(|scenario| scenario.status == crate::types::ScenarioStatus::Missed)
                    {
                        Some(LedgerDecisionStatus::Missed)
                    } else if snapshot
                        .scenarios
                        .iter()
                        .any(|scenario| scenario.status == crate::types::ScenarioStatus::Expired)
                    {
                        Some(LedgerDecisionStatus::Expired)
                    } else {
                        None
                    };
                status
            } else {
                None
            }
        } else {
            None
        };
        let updates: Vec<_> = snapshot
            .scenarios
            .iter()
            .map(|scenario| ScenarioStatusUpdate {
                product: ledger_product(scenario.product),
                status: ledger_scenario_status(scenario.status),
                confirmation_started_at: (scenario.status
                    == crate::types::ScenarioStatus::Confirming)
                    .then(|| now.saturating_sub((scenario.confirming_elapsed_ms / 1_000) as i64)),
                confirmation_tick_count: scenario.confirming_ticks,
                updated_at: now,
                terminal_reason: scenario_terminal_reason_ko(scenario.status),
                reference_observed_at: scenario.reference_observed_at,
            })
            .collect();
        if let Err(error) = self.ledger.update_decision_and_scenarios(
            &decision_key,
            decision_status,
            None,
            &updates,
        ) {
            tracing::warn!("OCO 시나리오·판단 장부 원자 갱신 실패: {error}");
            return;
        }
        let history_changed = {
            let mut previous = self.scenario_history_fingerprint.lock().unwrap();
            if previous.as_deref() == Some(fingerprint.as_str()) {
                false
            } else {
                *previous = Some(fingerprint);
                true
            }
        };
        if history_changed {
            self.emit(
                "llm-decision-recorded",
                &serde_json::json!({ "decisionId": decision_key }),
            );
        }
    }

    /// 이미 기록한 판단을 실행 불가능 상태로 폐기하면서 아직 활성일 수 있는
    /// 자식 시나리오도 같은 트랜잭션에서 종결한다.
    fn discard_recorded_decision(
        &self,
        decision_id: &str,
        decision: &ValidatedDecision,
        seeds: &[ScenarioSeed],
        reason: &str,
    ) {
        let now = now_kst_fake_epoch();
        let updates: Vec<_> = decision
            .scenarios
            .iter()
            .map(|scenario| {
                let seed = seeds
                    .iter()
                    .find(|seed| seed.product == scenario.product)
                    .copied()
                    .unwrap_or_else(|| ScenarioSeed::armed(scenario.product, None));
                let status = match seed.status {
                    crate::types::ScenarioStatus::Missed => LedgerScenarioStatus::Missed,
                    crate::types::ScenarioStatus::Invalidated => LedgerScenarioStatus::Invalidated,
                    _ => LedgerScenarioStatus::Replaced,
                };
                ScenarioStatusUpdate {
                    product: ledger_product(scenario.product),
                    status,
                    confirmation_started_at: None,
                    confirmation_tick_count: 0,
                    updated_at: now,
                    terminal_reason: scenario_terminal_reason_ko(seed.status).or(Some(reason)),
                    reference_observed_at: seed.reference_observed_at,
                }
            })
            .collect();
        let result = {
            let _retirement = self.retirement_gate.lock().unwrap();
            if !self.is_active() {
                return;
            }
            self.ledger.update_decision_and_scenarios(
                decision_id,
                Some(LedgerDecisionStatus::Discarded),
                Some(reason),
                &updates,
            )
        };
        if let Err(error) = result {
            tracing::error!("폐기된 LLM 판단·시나리오 장부 원자 갱신 실패: {error}");
            return;
        }
        self.emit(
            "llm-decision-recorded",
            &serde_json::json!({
                "decisionId": decision_id,
                "status": LedgerDecisionStatus::Discarded.as_str(),
            }),
        );
    }

    fn mark_current_group_replaced(&self) -> Result<(), String> {
        let market_day = self.effective_market_day_state();
        let (decision_key, snapshot) = {
            let settings = self.settings.read().unwrap();
            let runtime = self.automation.lock().unwrap();
            (
                runtime.decision_key().map(str::to_owned),
                runtime.snapshot(
                    &self.runtime_id,
                    self.runtime_generation,
                    &settings.auto_symbols,
                    self.monotonic_now(),
                    market_day.status,
                    market_day.message.clone(),
                ),
            )
        };
        let Some(decision_key) = decision_key else {
            return Ok(());
        };
        let now = now_kst_fake_epoch();
        let active: Vec<_> = snapshot
            .scenarios
            .iter()
            .filter(|scenario| {
                matches!(
                    scenario.status,
                    crate::types::ScenarioStatus::Armed | crate::types::ScenarioStatus::Confirming
                )
            })
            .collect();
        if active.is_empty() {
            return Ok(());
        }
        let updates: Vec<_> = active
            .into_iter()
            .map(|scenario| ScenarioStatusUpdate {
                product: ledger_product(scenario.product),
                status: LedgerScenarioStatus::Replaced,
                confirmation_started_at: None,
                confirmation_tick_count: 0,
                updated_at: now,
                terminal_reason: Some("새 LLM 판단으로 교체"),
                reference_observed_at: scenario.reference_observed_at,
            })
            .collect();
        self.ledger
            .update_decision_and_scenarios(
                &decision_key,
                Some(LedgerDecisionStatus::Replaced),
                None,
                &updates,
            )
            .map_err(|error| format!("기존 LLM 판단 교체 장부 원자 갱신 실패: {error}"))?;
        self.emit(
            "llm-decision-recorded",
            &serde_json::json!({
                "decisionId": decision_key,
                "status": LedgerDecisionStatus::Replaced.as_str(),
            }),
        );
        Ok(())
    }

    /// 모드 전환이 주문 actor를 선점한 시점의 OCO를 장부와 메모리에서 함께 종결한다.
    ///
    /// 트리거 전이면 활성 시나리오를 Replaced로 바꾸고, 트리거 직후이지만 주문
    /// 복구 표식이 생기기 전이면 Triggered 사실은 보존하면서 미주문 사유를 남긴다.
    /// 장부 갱신이 실패하면 메모리 그룹과 revision을 건드리지 않는다.
    fn close_current_group_for_mode_transition(&self) -> Result<(), String> {
        const ACTIVE_REASON: &str = "제어 모드 전환으로 폐기";
        const TRIGGERED_REASON: &str = "제어 모드 전환으로 주문 전 진입 폐기";

        let market_day = self.effective_market_day_state();
        let settings = self.settings.read().unwrap();
        let mut runtime = self.automation.lock().unwrap();
        let snapshot = runtime.snapshot(
            &self.runtime_id,
            self.runtime_generation,
            &settings.auto_symbols,
            self.monotonic_now(),
            market_day.status,
            market_day.message,
        );
        if snapshot.scenarios.is_empty() {
            return Ok(());
        }
        let decision_key = runtime
            .decision_key()
            .map(str::to_owned)
            .ok_or_else(|| "모드 전환으로 종결할 OCO 판단 식별자가 없습니다".to_string())?;
        let pre_dispatch_trigger = runtime.phase() == crate::types::AutomationPhase::EntryPending
            && runtime.pending_entry().is_none()
            && runtime.position().is_none();
        let now = now_kst_fake_epoch();
        let (decision_status, updates): (Option<LedgerDecisionStatus>, Vec<_>) =
            if pre_dispatch_trigger {
                (
                    Some(LedgerDecisionStatus::Triggered),
                    snapshot
                        .scenarios
                        .iter()
                        .map(|scenario| ScenarioStatusUpdate {
                            product: ledger_product(scenario.product),
                            status: ledger_scenario_status(scenario.status),
                            confirmation_started_at: None,
                            confirmation_tick_count: 0,
                            updated_at: now,
                            terminal_reason: if scenario.status
                                == crate::types::ScenarioStatus::Triggered
                            {
                                Some(TRIGGERED_REASON)
                            } else {
                                scenario_terminal_reason_ko(scenario.status)
                            },
                            reference_observed_at: scenario.reference_observed_at,
                        })
                        .collect(),
                )
            } else {
                let updates: Vec<_> = snapshot
                    .scenarios
                    .iter()
                    .filter(|scenario| {
                        matches!(
                            scenario.status,
                            crate::types::ScenarioStatus::Armed
                                | crate::types::ScenarioStatus::Confirming
                        )
                    })
                    .map(|scenario| ScenarioStatusUpdate {
                        product: ledger_product(scenario.product),
                        status: LedgerScenarioStatus::Replaced,
                        confirmation_started_at: None,
                        confirmation_tick_count: 0,
                        updated_at: now,
                        terminal_reason: Some(ACTIVE_REASON),
                        reference_observed_at: scenario.reference_observed_at,
                    })
                    .collect();
                (
                    (!updates.is_empty()).then_some(LedgerDecisionStatus::Replaced),
                    updates,
                )
            };

        if let Some(decision_status) = decision_status {
            self.ledger
                .update_decision_and_scenarios(&decision_key, Some(decision_status), None, &updates)
                .map_err(|error| format!("모드 전환 OCO 장부 종결 실패: {error}"))?;
        }
        runtime.clear_group_for_mode_transition();
        drop(runtime);
        drop(settings);
        if let Some(decision_status) = decision_status {
            self.emit(
                "llm-decision-recorded",
                &serde_json::json!({
                    "decisionId": decision_key,
                    "status": decision_status.as_str(),
                }),
            );
        }
        Ok(())
    }

    /// 신규 진입은 즉시 막되, 장부 종결이 실패하면 원본 그룹을 Suspended 안에
    /// 보존해 다음 scheduler tick에서 부모·자식을 다시 원자 갱신할 수 있게 한다.
    fn suspend_automation_with_group_ledger_safety(&self, reason: impl Into<String>) {
        let reason = reason.into();
        match self.mark_current_group_replaced() {
            Ok(()) => self.automation.lock().unwrap().suspend(reason),
            Err(error) => {
                tracing::error!("자동 시나리오 장부 종결을 보존 상태에서 재시도: {error}");
                self.automation
                    .lock()
                    .unwrap()
                    .suspend_preserving_group(format!(
                        "{reason} (시나리오 장부 종결 재시도 중: {error})"
                    ));
            }
        }
        self.persist_automation();
        self.emit_automation_state();
    }

    fn retry_suspended_group_ledger_repair(&self) {
        if !self
            .automation
            .lock()
            .unwrap()
            .has_group_pending_ledger_repair()
        {
            return;
        }
        match self.mark_current_group_replaced() {
            Ok(()) => {
                if self.automation.lock().unwrap().finish_group_ledger_repair() {
                    self.persist_automation();
                    self.emit_automation_state();
                }
            }
            Err(error) => {
                tracing::error!("Suspended OCO 장부 종결 재시도 실패: {error}");
            }
        }
    }

    async fn automation_tick(self: &Arc<Self>) {
        if !self.is_active() {
            return;
        }
        self.retry_suspended_group_ledger_repair();
        let now = self.automation_now();
        let monotonic = self.monotonic_now();
        let market_open = self.market_is_open();
        if market_open {
            self.feed_watchdog_tick(now);
        }
        let schedule_changed = if !market_open {
            // 장부 Replaced와 runtime 그룹 제거 사이에 체결 확정이 끼지 않게
            // feed consumer와 같은 gate에서 두 전이를 연속 수행한다.
            let _market_guard = self.automation_market_gate.lock().await;
            let can_pause_for_market_day = match self.mark_current_group_replaced() {
                Ok(()) => true,
                Err(error) => {
                    tracing::error!("휴장 중 OCO 장부 종결 재시도 실패: {error}");
                    false
                }
            };
            if can_pause_for_market_day {
                self.automation.lock().unwrap().pause_for_market_day()
            } else {
                false
            }
        } else {
            let mut runtime = self.automation.lock().unwrap();
            if matches!(runtime.mode(), ControlMode::Auto | ControlMode::Shadow)
                && runtime.position().is_none()
            {
                let recovered = schedule::recover_decision_slot(runtime.next_decision_at(), now);
                if recovered != runtime.next_decision_at() {
                    runtime.set_next_decision_at(recovered);
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };
        if schedule_changed {
            self.persist_automation();
            self.emit_automation_state();
        }
        let expired = self.automation.lock().unwrap().expire_group(monotonic);
        if expired {
            self.sync_scenario_ledger();
            self.emit_automation_state();
        }

        let mode = self.automation.lock().unwrap().mode();
        if mode == ControlMode::Shadow {
            self.poll_shadow(now);
        } else if mode == ControlMode::Auto {
            let position_market_close_due = {
                let runtime = self.automation.lock().unwrap();
                runtime.position().is_some_and(|position| {
                    !position.shadow
                        && schedule::has_reached_position_flatten(position.first_fill_at, now)
                })
            };
            if position_market_close_due {
                let (should_exit, state_changed) = {
                    let mut runtime = self.automation.lock().unwrap();
                    let revision = runtime.revision();
                    let should_exit = runtime.begin_market_close_exit().is_some();
                    (should_exit, runtime.revision() != revision)
                };
                if state_changed {
                    self.persist_automation();
                    self.emit_automation_state();
                }
                if should_exit {
                    let engine = Arc::clone(self);
                    tokio::spawn(async move { engine.execute_auto_exit("market_close").await });
                }
            }
            let exit_reason = {
                let runtime = self.automation.lock().unwrap();
                runtime.position().and_then(|position| {
                    if runtime.phase() != crate::types::AutomationPhase::Holding {
                        return None;
                    }
                    // 수익률을 계산할 수 없으면 0%로 대체하지 않는다. 무장된 수익
                    // 보호선에서 0%는 "되밀렸다"로 읽혀 근거 없는 청산이 나간다.
                    // 가격에 의존하지 않는 최대보유 청산만 그대로 유지한다.
                    let rate = (position.avg_price > 0.0
                        && position.last_price.is_finite()
                        && position.last_price > 0.0)
                        .then(|| (position.last_price / position.avg_price - 1.0) * 100.0);
                    if rate.is_some_and(|rate| rate <= crate::util::AUTO_STOP_LOSS_PCT) {
                        Some("stop_loss")
                    } else if position.profit_guard_armed
                        && rate.is_some_and(|rate| rate <= crate::util::AUTO_PROFIT_GUARD_PCT)
                    {
                        Some("profit_guard")
                    } else if now >= position.exit_deadline {
                        Some("max_holding")
                    } else {
                        None
                    }
                })
            };
            if let Some(reason) = exit_reason {
                if self.automation.lock().unwrap().begin_exit(reason).is_some() {
                    self.persist_automation();
                    self.emit_automation_state();
                    let engine = Arc::clone(self);
                    tokio::spawn(async move { engine.execute_auto_exit(reason).await });
                }
            }
            let no_position = self.automation.lock().unwrap().position().is_none();
            let day = now.div_euclid(86_400);
            if no_position
                && market_open
                && schedule::is_at_or_after_flatten(now)
                && self.last_flatten_day.load(Ordering::SeqCst) != day
                && !self.auto_flatten_pending.swap(true, Ordering::SeqCst)
            {
                let engine = Arc::clone(self);
                tokio::spawn(async move {
                    let _actor = engine.order_actor.lock().await;
                    if let Err(error) = engine
                        .cancel_known_auto_open_orders("15:15 자동 미체결 정리")
                        .await
                    {
                        engine.automation.lock().unwrap().suspend(error);
                        engine.emit_automation_state();
                    } else {
                        engine.last_flatten_day.store(day, Ordering::SeqCst);
                    }
                    engine.auto_flatten_pending.store(false, Ordering::SeqCst);
                });
            }
        }

        if !market_open {
            return;
        }
        let (slot, mode, revision) = {
            // 이전 OCO의 장부 종결과 Analyzing 전환을 시세 처리와 직렬화한다.
            let _market_guard = self.automation_market_gate.lock().await;
            let due = {
                let runtime = self.automation.lock().unwrap();
                runtime
                    .next_decision_at()
                    .filter(|slot| *slot <= now)
                    .map(|slot| (slot, runtime.mode()))
            };
            let Some((slot, mode)) = due else {
                return;
            };
            if !matches!(mode, ControlMode::Auto | ControlMode::Shadow)
                || !self.automation.lock().unwrap().can_begin_analysis()
            {
                return;
            }
            let next = schedule::following_slot(slot);
            if let Err(error) = self.mark_current_group_replaced() {
                self.emit(
                    "engine-error",
                    &format!("기존 자동 시나리오 장부 종결 실패로 새 판단을 보류합니다: {error}"),
                );
                return;
            }
            let Some(revision) = self.automation.lock().unwrap().begin_analysis(slot, next) else {
                return;
            };
            (slot, mode, revision)
        };
        // OpenAI 요청을 시작하기 전에 이번 슬롯 소비와 다음 슬롯을 반드시
        // 내구화한다. 정확한 5분 경계에서 앱이 재시작돼도 같은 슬롯을 재호출하지 않는다.
        if let Err(error) = self.persist_automation_required() {
            let message = format!("LLM 판단 슬롯 저장 실패: {error}");
            self.automation
                .lock()
                .unwrap()
                .fail_analysis(revision, message);
            self.emit_automation_state();
            return;
        }
        self.emit_automation_state();
        let expiry = schedule::scenario_expiry(slot).unwrap_or(slot + 300);
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            engine.run_llm_decision(slot, expiry, revision, mode).await;
        });
    }

    async fn build_llm_input(&self) -> Result<(DecisionInput, AutomationMarketMarker), String> {
        let settings = self.settings.read().unwrap().clone();
        // 초기화 또는 실제 연결 공백 뒤의 REST 백필은 피드 잠금 밖에서 끝낸다.
        // 이 반환값은 준비용일 뿐이며 실제 LLM 입력에는 아래 임계구역에서 다시
        // 복사한 정상 캐시 스냅샷만 사용한다.
        self.candles(&settings.auto_symbols.underlying)
            .await
            .map_err(|error| error.to_string())?;

        // 봉·세 종목 호가·journal cursor·세대를 하나의 피드 임계구역에서
        // 캡처한다. 이후 PNG 렌더 중 들어오는 틱은 marker 뒤 journal에 남으므로
        // 응답 적용 때 정확히 재생되고, 입력 봉과 시세가 서로 다른 시점이 되지 않는다.
        let (bars, as_of_epoch, underlying, leverage, inverse, marker) = {
            let _market_guard = self.automation_market_gate.lock().await;
            let now = now_kst_fake_epoch();
            if let Some(error) = self.auto_quotes_freshness_error(now) {
                return Err(error);
            }
            let bars = self
                .market_history
                .healthy_snapshot(&settings.auto_symbols.underlying)
                .await
                .ok_or_else(|| {
                    "LLM 입력 시점의 정상 1분봉 스냅샷을 확보할 수 없습니다".to_string()
                })?;
            let quotes = self.quotes.read().unwrap();
            let get = |code: &str| {
                quotes
                    .get(code)
                    .cloned()
                    .ok_or_else(|| format!("시세 없음: {code}"))
            };
            let (underlying, leverage, inverse) = (
                get(&settings.auto_symbols.underlying)?,
                get(&settings.auto_symbols.leverage)?,
                get(&settings.auto_symbols.inverse)?,
            );
            let marker = self.automation_trade_journal.lock().unwrap().marker(
                self.automation_feed_generation.load(Ordering::SeqCst),
                underlying.trade_sequence,
            );
            (bars, now, underlying, leverage, inverse, marker)
        };

        let as_of_kst = chrono::DateTime::from_timestamp(as_of_epoch, 0)
            .ok_or_else(|| "LLM 판단 시각을 변환할 수 없습니다".to_string())?
            .format("%Y-%m-%dT%H:%M:%S+09:00")
            .to_string();
        let indicators = chart_image::indicator_payload(&bars, as_of_epoch);
        let chart_png = chart_image::render_composite_png(&bars, as_of_epoch)
            .map_err(|error| error.to_string())?;
        Ok((
            DecisionInput {
                as_of_kst,
                underlying: MarketQuoteInput::from(&underlying),
                leverage: MarketQuoteInput::from(&leverage),
                inverse: MarketQuoteInput::from(&inverse),
                indicators,
                chart_png,
            },
            marker,
        ))
    }

    /// 모델 응답을 적용하기 직전에 최신 본주 체결과 캐시된 1분봉을 다시 읽는다.
    /// 첫 입력에서 이미 백필했으므로 정상 연결 중 추가 KIS REST 호출은 발생하지 않는다.
    async fn latest_scenario_seeds(
        &self,
        decision: &ValidatedDecision,
        input_indicators: &chart_image::IndicatorPayload,
        market_marker: AutomationMarketMarker,
    ) -> Result<Vec<ScenarioSeed>, String> {
        if self.automation_feed_generation.load(Ordering::SeqCst) != market_marker.feed_generation {
            return Err("LLM 분석 중 시세 연결이 초기화되어 판단을 폐기합니다".into());
        }
        let underlying_code = self
            .settings
            .read()
            .unwrap()
            .auto_symbols
            .underlying
            .clone();
        let bars = self
            .candles(&underlying_code)
            .await
            .map_err(|error| format!("응답 적용용 최근 1분봉 확인 실패: {error}"))?;
        let now = now_kst_fake_epoch();
        if let Some(error) = self.auto_quotes_freshness_error(now) {
            return Err(format!("응답 적용 시 {error}"));
        }
        let quote = self
            .quotes
            .read()
            .unwrap()
            .get(&underlying_code)
            .cloned()
            .ok_or_else(|| "응답 적용 시 본주 시세가 없습니다".to_string())?;
        let current_price = quote.price.round() as u64;
        let minimum_reference_epoch = self.automation_feed_reset_epoch.load(Ordering::SeqCst);
        let (pre_input_reference_ticks, replay_ticks) = {
            let journal = self.automation_trade_journal.lock().unwrap();
            let pre_input_reference_ticks = if needs_reset_minute_tick_recovery(
                &input_indicators.one_minute,
                minimum_reference_epoch,
            ) {
                journal.since_reset_through(market_marker)?
            } else {
                Vec::new()
            };
            (pre_input_reference_ticks, journal.after(market_marker)?)
        };
        Ok(decision
            .scenarios
            .iter()
            .map(|scenario| {
                seed_scenario_from_latest_market(
                    scenario,
                    current_price,
                    quote.trade_ts,
                    &bars,
                    &input_indicators.one_minute,
                    minimum_reference_epoch,
                    &pre_input_reference_ticks,
                    &replay_ticks,
                )
            })
            .collect())
    }

    /// 실패·거부·타임아웃도 다음 슬롯과 Idle 전이를 함께 저장한다. 슬롯 소비는
    /// 요청 전에 이미 저장되지만, 이 저장으로 재시작 UI와 실행 상태도 일치시킨다.
    fn finish_llm_analysis_failure(&self, revision: u64, message: impl Into<String>) {
        if !self.is_active() {
            return;
        }
        if self
            .automation
            .lock()
            .unwrap()
            .fail_analysis(revision, message)
        {
            self.persist_automation();
            self.emit_automation_state();
        }
    }

    async fn run_llm_decision(
        self: Arc<Self>,
        slot: i64,
        expiry: i64,
        revision: u64,
        mode: ControlMode,
    ) {
        if !self.is_active() {
            return;
        }
        let input_feed_generation = self.automation_feed_generation.load(Ordering::SeqCst);
        let (input, market_marker) = match self.build_llm_input().await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.record_failed_decision(slot, expiry, revision, mode, &error);
                self.finish_llm_analysis_failure(revision, error);
                return;
            }
        };
        if !self.is_active() {
            return;
        }
        let input_hash = match serialized_dynamic_input(&input) {
            Ok(serialized) => sha256_hex(serialized.as_bytes()),
            Err(error) => {
                let message = error.to_string();
                self.record_failed_decision(slot, expiry, revision, mode, &message);
                self.finish_llm_analysis_failure(revision, message);
                return;
            }
        };
        let chart_hash = sha256_hex(&input.chart_png);
        if market_marker.feed_generation != input_feed_generation
            || self.automation_feed_generation.load(Ordering::SeqCst) != input_feed_generation
        {
            let message = "LLM 입력 준비 중 시세 연결이 초기화되어 판단 요청을 생략했습니다";
            self.record_failed_decision_with_telemetry(
                slot,
                expiry,
                revision,
                mode,
                message,
                None,
                TokenUsage::default(),
                0,
                Some(&input_hash),
                Some(&chart_hash),
            );
            self.finish_llm_analysis_failure(revision, message);
            return;
        }
        let snapshot_price = input.underlying.price.round() as u64;
        let api_key = self.settings.read().unwrap().openai_api_key.clone();
        let client = match OpenAiClient::new(api_key) {
            Ok(client) => client,
            Err(error) => {
                let message = error.to_string();
                self.record_failed_decision_with_telemetry(
                    slot,
                    expiry,
                    revision,
                    mode,
                    &message,
                    None,
                    TokenUsage::default(),
                    0,
                    Some(&input_hash),
                    Some(&chart_hash),
                );
                self.finish_llm_analysis_failure(revision, message);
                return;
            }
        };
        let seconds = expiry.saturating_sub(now_kst_fake_epoch());
        if seconds <= 0 {
            let message = "다음 5분 경계 전에 판단 입력 준비가 끝나지 않았습니다";
            self.record_failed_decision_with_telemetry(
                slot,
                expiry,
                revision,
                mode,
                message,
                None,
                TokenUsage::default(),
                0,
                Some(&input_hash),
                Some(&chart_hash),
            );
            self.finish_llm_analysis_failure(revision, message);
            return;
        }
        let request_started = std::time::Instant::now();
        if !self.is_active() || !self.market_is_open() {
            let message = "OpenAI 요청 직전 장중 상태가 해제되어 판단 요청을 생략했습니다";
            self.record_failed_decision_with_telemetry(
                slot,
                expiry,
                revision,
                mode,
                message,
                None,
                TokenUsage::default(),
                0,
                Some(&input_hash),
                Some(&chart_hash),
            );
            self.finish_llm_analysis_failure(revision, message);
            return;
        }
        let response =
            tokio::time::timeout(Duration::from_secs(seconds as u64), client.decide(&input)).await;
        if !self.is_active() {
            return;
        }
        let result = match response {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                let message = error.to_string();
                self.record_failed_decision_with_telemetry(
                    slot,
                    expiry,
                    revision,
                    mode,
                    &message,
                    error.response_id.as_deref(),
                    error.usage,
                    error.latency_ms,
                    Some(&input_hash),
                    Some(&chart_hash),
                );
                self.finish_llm_analysis_failure(revision, message);
                return;
            }
            Err(_) => {
                let message = "OpenAI 판단이 다음 5분 경계를 넘겨 폐기되었습니다";
                let latency_ms = request_started
                    .elapsed()
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX);
                self.record_failed_decision_with_telemetry(
                    slot,
                    expiry,
                    revision,
                    mode,
                    message,
                    None,
                    TokenUsage::default(),
                    latency_ms,
                    Some(&input_hash),
                    Some(&chart_hash),
                );
                self.finish_llm_analysis_failure(revision, message);
                return;
            }
        };

        let feed_unchanged =
            self.automation_feed_generation.load(Ordering::SeqCst) == input_feed_generation;
        let mut current_matches = self.market_is_open() && feed_unchanged && {
            let runtime = self.automation.lock().unwrap();
            runtime.revision() == revision
                && runtime.phase() == crate::types::AutomationPhase::Analyzing
                && runtime.mode() == mode
                && now_kst_fake_epoch() < expiry
        };
        let validated = match validate_decision(snapshot_price, &result.decision) {
            Ok(validated) => validated,
            Err(error) => {
                let message = format!("LLM 결정 의미 검증 실패: {error:?}");
                let status = if current_matches {
                    LedgerDecisionStatus::Invalid
                } else {
                    LedgerDecisionStatus::Discarded
                };
                let decision = NewDecision {
                    decision_id: result.response_id.clone(),
                    session_id: self
                        .automation
                        .lock()
                        .unwrap()
                        .session_id()
                        .map(str::to_owned),
                    control_mode: ledger_control_mode(mode),
                    revision: revision.try_into().unwrap_or(i64::MAX),
                    as_of_ts: slot,
                    expires_at: expiry,
                    underlying_price: snapshot_price as f64,
                    status,
                    model: MODEL.into(),
                    prompt_version: PROMPT_VERSION.into(),
                    input_tokens: result.usage.input_tokens,
                    cached_input_tokens: result.usage.cached_input_tokens,
                    cache_write_tokens: result.usage.cache_write_tokens,
                    output_tokens: result.usage.output_tokens,
                    reasoning_tokens: result.usage.reasoning_tokens,
                    latency_ms: result.latency_ms,
                    input_hash: Some(input_hash),
                    chart_hash: Some(chart_hash),
                    market_regime: Some(ledger_market_regime(result.decision.market_regime)),
                    decision_summary_ko: Some(result.decision.decision_summary_ko.clone()),
                    error: Some(message.clone()),
                    created_at: now_kst_fake_epoch(),
                };
                let record_result = {
                    let _retirement = self.retirement_gate.lock().unwrap();
                    if !self.is_active() {
                        return;
                    }
                    self.ledger.record_decision(&decision, &[])
                };
                if let Err(ledger_error) = record_result {
                    tracing::error!("무효 LLM 판단 장부 기록 실패: {ledger_error}");
                } else {
                    self.emit(
                        "llm-decision-recorded",
                        &serde_json::json!({
                            "decisionId": decision.decision_id,
                            "status": decision.status.as_str(),
                        }),
                    );
                }
                self.finish_llm_analysis_failure(revision, message);
                return;
            }
        };
        let mut discard_reason = (!current_matches).then(|| {
            if !feed_unchanged {
                "LLM 분석 중 시세 연결이 초기화되어 이전 기준선 판단을 폐기".to_string()
            } else {
                "상태 revision 또는 판단 슬롯 만료로 폐기".to_string()
            }
        });
        let mut seeds = Vec::new();
        if current_matches && !validated.scenarios.is_empty() {
            match self
                .latest_scenario_seeds(&validated, &input.indicators, market_marker)
                .await
            {
                Ok(latest) => seeds = latest,
                Err(error) => {
                    current_matches = false;
                    discard_reason = Some(error);
                }
            }
        }
        if current_matches {
            let active_market = self.is_active() && self.market_is_open();
            let runtime = self.automation.lock().unwrap();
            current_matches = runtime.revision() == revision
                && runtime.phase() == crate::types::AutomationPhase::Analyzing
                && runtime.mode() == mode
                && now_kst_fake_epoch() < expiry
                && active_market
                && self.automation_feed_generation.load(Ordering::SeqCst) == input_feed_generation;
            if !current_matches {
                discard_reason = Some("최신 시세 재검증 중 상태 또는 판단 슬롯이 변경됨".into());
            }
        }
        let decision_status = initial_decision_status(current_matches, &validated, &seeds);
        // 실행 가능한 판단은 장부에 먼저 안전한 비활성 상태로 준비한 뒤, 최종
        // 피드 배리어·seed·revision 검사를 통과한 트랜잭션에서만 Armed로 바꾼다.
        // 중간 DB 오류나 프로세스 종료가 있어도 장부에 유령 Armed가 남지 않는다.
        let staged_activation = current_matches && decision_status == LedgerDecisionStatus::Armed;
        let scenarios: Vec<NewDecisionScenario> = validated
            .scenarios
            .iter()
            .map(|scenario| {
                let seed = seeds
                    .iter()
                    .find(|seed| seed.product == scenario.product)
                    .copied()
                    .unwrap_or_else(|| ScenarioSeed::armed(scenario.product, None));
                let scenario_status = if staged_activation
                    && matches!(
                        seed.status,
                        crate::types::ScenarioStatus::Armed
                            | crate::types::ScenarioStatus::Confirming
                    ) {
                    LedgerScenarioStatus::Replaced
                } else if current_matches {
                    ledger_scenario_status(seed.status)
                } else {
                    LedgerScenarioStatus::Replaced
                };
                let terminal_reason = if current_matches {
                    scenario_terminal_reason_ko(seed.status).map(str::to_owned)
                } else {
                    discard_reason.clone()
                };
                NewDecisionScenario {
                    product: ledger_product(scenario.product),
                    setup_type: Some(ledger_setup_type(scenario.setup_type)),
                    reference_price: Some(scenario.reference_price),
                    confirmation_price: Some(scenario.confirmation_price),
                    invalidation_price: Some(scenario.invalidation_price),
                    trigger_price: scenario.trigger_price,
                    target_return_pct: scenario.target_return_pct,
                    rationale_ko: Some(scenario.rationale_ko.clone()),
                    status: scenario_status,
                    reference_observed_at: seed.reference_observed_at,
                    terminal_reason,
                }
            })
            .chain(
                validated
                    .rejected
                    .iter()
                    .map(|scenario| NewDecisionScenario {
                        product: ledger_product(scenario.product),
                        setup_type: Some(ledger_setup_type(scenario.setup_type)),
                        reference_price: Some(scenario.reference_price),
                        confirmation_price: Some(scenario.confirmation_price),
                        invalidation_price: Some(scenario.invalidation_price),
                        trigger_price: scenario.trigger_price,
                        target_return_pct: scenario.target_return_pct,
                        rationale_ko: Some(scenario.rationale_ko.clone()),
                        status: LedgerScenarioStatus::Invalid,
                        reference_observed_at: None,
                        terminal_reason: Some(format!(
                            "의미 검증 실패: {}",
                            scenario_invalid_reason_ko(scenario.reason)
                        )),
                    }),
            )
            .collect();
        let session_id = self
            .automation
            .lock()
            .unwrap()
            .session_id()
            .map(str::to_owned);
        let decision = NewDecision {
            decision_id: result.response_id.clone(),
            session_id,
            control_mode: ledger_control_mode(mode),
            revision: revision.try_into().unwrap_or(i64::MAX),
            as_of_ts: slot,
            expires_at: expiry,
            underlying_price: snapshot_price as f64,
            status: if staged_activation {
                LedgerDecisionStatus::Discarded
            } else {
                decision_status
            },
            model: MODEL.into(),
            prompt_version: PROMPT_VERSION.into(),
            input_tokens: result.usage.input_tokens,
            cached_input_tokens: result.usage.cached_input_tokens,
            cache_write_tokens: result.usage.cache_write_tokens,
            output_tokens: result.usage.output_tokens,
            reasoning_tokens: result.usage.reasoning_tokens,
            latency_ms: result.latency_ms,
            input_hash: Some(input_hash),
            chart_hash: Some(chart_hash),
            market_regime: Some(ledger_market_regime(validated.market_regime)),
            decision_summary_ko: Some(validated.decision_summary_ko.clone()),
            error: discard_reason.clone().or_else(|| {
                (decision_status == LedgerDecisionStatus::Invalid)
                    .then(|| "모든 시나리오가 의미 검증에 실패함".into())
            }),
            created_at: now_kst_fake_epoch(),
        };
        let row_id = {
            let _retirement = self.retirement_gate.lock().unwrap();
            if !self.is_active() {
                return;
            }
            match self.ledger.record_decision(&decision, &scenarios) {
                Ok(row_id) => row_id,
                Err(error) => {
                    let message = format!("LLM 판단 장부 저장 실패: {error}");
                    drop(_retirement);
                    self.finish_llm_analysis_failure(revision, message);
                    return;
                }
            }
        };
        if !current_matches {
            self.emit(
                "llm-decision-recorded",
                &serde_json::json!({
                    "decisionId": decision.decision_id,
                    "status": decision.status.as_str(),
                }),
            );
        }
        if !current_matches {
            self.finish_llm_analysis_failure(
                revision,
                discard_reason.unwrap_or_else(|| "LLM 판단 적용 조건이 변경되었습니다".into()),
            );
            return;
        }

        if !validated.scenarios.is_empty() {
            if let Err(error) = self.drain_automation_feed_queue(expiry).await {
                self.discard_recorded_decision(&decision.decision_id, &validated, &seeds, &error);
                self.finish_llm_analysis_failure(revision, error);
                return;
            }
        }

        // 이 잠금 뒤에는 새 체결·연결 이벤트가 끼어들 수 없다. 입력 cursor 이후
        // 틱을 전부 재생한 상태에서 generation/expiry/revision 확인과 OCO 무장을
        // 연속 수행해 응답 지연 구간의 C/I 재통과와 TOCTOU를 모두 차단한다.
        let market_guard = self.automation_market_gate.lock().await;
        if self.automation_feed_generation.load(Ordering::SeqCst) != market_marker.feed_generation {
            let message = "LLM 판단 무장 직전 시세 연결이 초기화되어 폐기되었습니다";
            self.discard_recorded_decision(&decision.decision_id, &validated, &seeds, message);
            drop(market_guard);
            self.finish_llm_analysis_failure(revision, message);
            return;
        }

        if !validated.scenarios.is_empty() {
            let latest = match self
                .latest_scenario_seeds(&validated, &input.indicators, market_marker)
                .await
            {
                Ok(latest) => latest,
                Err(error) => {
                    self.discard_recorded_decision(
                        &decision.decision_id,
                        &validated,
                        &seeds,
                        &error,
                    );
                    drop(market_guard);
                    self.finish_llm_analysis_failure(revision, error);
                    return;
                }
            };
            for latest_seed in latest {
                if let Some(previous) = seeds
                    .iter_mut()
                    .find(|seed| seed.product == latest_seed.product)
                {
                    *previous = merge_scenario_seed(*previous, latest_seed);
                }
            }
        }

        let last_trade_sequence = if validated.scenarios.is_empty() {
            None
        } else {
            match self
                .automation_trade_journal
                .lock()
                .unwrap()
                .after(market_marker)
            {
                Ok(replayed) => replayed
                    .last()
                    .map(|tick| tick.sequence)
                    .or((market_marker.trade_sequence > 0).then_some(market_marker.trade_sequence)),
                Err(error) => {
                    self.discard_recorded_decision(
                        &decision.decision_id,
                        &validated,
                        &seeds,
                        &error,
                    );
                    drop(market_guard);
                    self.finish_llm_analysis_failure(revision, error);
                    return;
                }
            }
        };

        // runtime mutex를 얻은 뒤의 밀리초 벽시각 하나만 만료 검사와 남은 수명
        // 계산에 함께 쓴다. 단조 시각을 먼저 찍어 두 샘플 사이 지연은 수명을
        // 보수적으로 줄이게 하고, mutex 대기나 초 단위 절삭으로 연장되지 않게 한다.
        let final_market_open = self.is_active() && self.market_is_open();
        let mut runtime = self.automation.lock().unwrap();
        let armed_at = self.monotonic_now();
        let final_accepted_at_millis = now_kst_fake_epoch_millis();
        let final_accepted_at_epoch = final_accepted_at_millis.div_euclid(1_000);
        let expiry_millis = expiry.saturating_mul(1_000);
        let state_still_matches = runtime.revision() == revision
            && runtime.phase() == crate::types::AutomationPhase::Analyzing
            && runtime.mode() == mode
            && final_accepted_at_millis < expiry_millis
            && final_market_open
            && self.automation_feed_generation.load(Ordering::SeqCst)
                == market_marker.feed_generation;
        if !state_still_matches {
            drop(runtime);
            let message = "LLM 판단 무장 직전 상태·시세 세대 또는 판단 슬롯이 변경되었습니다";
            self.discard_recorded_decision(&decision.decision_id, &validated, &seeds, message);
            drop(market_guard);
            self.finish_llm_analysis_failure(revision, message);
            return;
        }
        let remaining_millis = expiry_millis.saturating_sub(final_accepted_at_millis);
        let expires_at = armed_at + Duration::from_millis(remaining_millis as u64);

        // 장부를 Armed로 바꾸기 전에 동일 인자로 OCO 생성 가능성을 확인한다.
        // 이후 runtime.accept_decision은 같은 mutex 아래 같은 입력을 사용한다.
        if !validated.scenarios.is_empty()
            && OcoGroup::arm(
                row_id,
                revision,
                &validated,
                &seeds,
                last_trade_sequence,
                armed_at,
                expires_at,
            )
            .is_err()
        {
            drop(runtime);
            let message = "응답 적용 시 OCO 그룹 사전 검증 실패";
            self.discard_recorded_decision(&decision.decision_id, &validated, &seeds, message);
            drop(market_guard);
            self.finish_llm_analysis_failure(revision, message);
            return;
        }

        let refreshed_status = initial_decision_status(true, &validated, &seeds);
        let updates: Vec<_> = seeds
            .iter()
            .map(|seed| ScenarioStatusUpdate {
                product: ledger_product(seed.product),
                status: ledger_scenario_status(seed.status),
                confirmation_started_at: None,
                confirmation_tick_count: 0,
                updated_at: final_accepted_at_epoch,
                terminal_reason: scenario_terminal_reason_ko(seed.status),
                reference_observed_at: seed.reference_observed_at,
            })
            .collect();
        let final_ledger_update = {
            let _retirement = self.retirement_gate.lock().unwrap();
            if !self.is_active() {
                drop(runtime);
                return;
            }
            self.ledger.update_decision_and_scenarios(
                &decision.decision_id,
                Some(refreshed_status),
                None,
                &updates,
            )
        };
        if let Err(error) = final_ledger_update {
            drop(runtime);
            let message = format!("최종 LLM 시나리오 장부 갱신 실패: {error}");
            self.discard_recorded_decision(&decision.decision_id, &validated, &seeds, &message);
            drop(market_guard);
            self.finish_llm_analysis_failure(revision, message);
            return;
        }
        if self.monotonic_now() >= expires_at {
            drop(runtime);
            let message = "최종 장부 갱신 중 판단 슬롯이 만료되어 OCO를 무장하지 않습니다";
            self.discard_recorded_decision(&decision.decision_id, &validated, &seeds, message);
            drop(market_guard);
            self.finish_llm_analysis_failure(revision, message);
            return;
        }

        let accepted = runtime.accept_decision(
            revision,
            row_id,
            result.response_id.clone(),
            &validated,
            automation_decision_status(refreshed_status),
            &seeds,
            last_trade_sequence,
            row_id,
            armed_at,
            expires_at,
            expiry,
        );
        drop(runtime);
        if !accepted {
            let message = "응답 적용 시 OCO 그룹 생성 또는 revision 검증 실패";
            self.discard_recorded_decision(&decision.decision_id, &validated, &seeds, message);
            drop(market_guard);
            self.finish_llm_analysis_failure(revision, message);
            return;
        }
        drop(market_guard);
        self.emit(
            "llm-decision-recorded",
            &serde_json::json!({
                "decisionId": decision.decision_id,
                "status": refreshed_status.as_str(),
            }),
        );
        self.persist_automation();
        self.emit_automation_state();
    }

    fn record_failed_decision(
        &self,
        slot: i64,
        expiry: i64,
        revision: u64,
        mode: ControlMode,
        error: &str,
    ) {
        self.record_failed_decision_with_telemetry(
            slot,
            expiry,
            revision,
            mode,
            error,
            None,
            TokenUsage::default(),
            0,
            None,
            None,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn record_failed_decision_with_telemetry(
        &self,
        slot: i64,
        expiry: i64,
        revision: u64,
        mode: ControlMode,
        error: &str,
        response_id: Option<&str>,
        usage: TokenUsage,
        latency_ms: u64,
        input_hash: Option<&str>,
        chart_hash: Option<&str>,
    ) {
        if !self.is_active() {
            return;
        }
        let underlying_price = self
            .settings
            .read()
            .ok()
            .and_then(|settings| {
                self.quotes.read().ok().and_then(|quotes| {
                    quotes
                        .get(&settings.auto_symbols.underlying)
                        .map(|q| q.price)
                })
            })
            .unwrap_or(0.0);
        let decision = NewDecision {
            decision_id: response_id
                .map(str::to_owned)
                .unwrap_or_else(|| unique_id("llm-error")),
            session_id: self
                .automation
                .lock()
                .unwrap()
                .session_id()
                .map(str::to_owned),
            control_mode: ledger_control_mode(mode),
            revision: revision.try_into().unwrap_or(i64::MAX),
            as_of_ts: slot,
            expires_at: expiry,
            underlying_price,
            status: LedgerDecisionStatus::Error,
            model: MODEL.into(),
            prompt_version: PROMPT_VERSION.into(),
            input_tokens: usage.input_tokens,
            cached_input_tokens: usage.cached_input_tokens,
            cache_write_tokens: usage.cache_write_tokens,
            output_tokens: usage.output_tokens,
            reasoning_tokens: usage.reasoning_tokens,
            latency_ms,
            input_hash: input_hash.map(str::to_owned),
            chart_hash: chart_hash.map(str::to_owned),
            market_regime: None,
            decision_summary_ko: None,
            error: Some(error.to_string()),
            created_at: now_kst_fake_epoch(),
        };
        let _retirement = self.retirement_gate.lock().unwrap();
        if !self.is_active() {
            return;
        }
        if let Err(ledger_error) = self.ledger.record_decision(&decision, &[]) {
            tracing::error!("LLM 실패 장부 기록 실패: {ledger_error}");
        } else {
            self.emit(
                "llm-decision-recorded",
                &serde_json::json!({
                    "decisionId": decision.decision_id,
                    "status": decision.status.as_str(),
                }),
            );
        }
    }

    async fn execute_triggered_entry(self: Arc<Self>, triggered: TriggeredScenario) {
        let mode = self.automation.lock().unwrap().mode();
        match mode {
            ControlMode::Auto => self.execute_real_entry(triggered).await,
            ControlMode::Shadow => self.execute_shadow_entry(triggered).await,
            ControlMode::Manual => {
                let changed = {
                    let mut runtime = self.automation.lock().unwrap();
                    if runtime.pending_matches(&triggered) {
                        runtime.entry_failed("수동 모드로 전환되어 진입을 폐기했습니다");
                        true
                    } else {
                        false
                    }
                };
                if changed {
                    self.emit_automation_state();
                }
            }
        }
    }

    async fn execute_shadow_entry(&self, triggered: TriggeredScenario) {
        // 모드 전환과 같은 actor에서 직렬화한다. 전환이 먼저 끝났다면 아래의
        // 상태 재검증에서 폐기되어 Manual 전환 뒤 섀도 포지션이 생기지 않는다.
        let _actor = self.order_actor.lock().await;
        if !self.validate_triggered_entry_window(ControlMode::Shadow, &triggered) {
            return;
        }
        let now = self.automation_now();
        if !self.auto_quotes_fresh(now) {
            self.automation
                .lock()
                .unwrap()
                .entry_failed("섀도 진입 시세가 10초 기준을 벗어났습니다");
            self.emit_automation_state();
            return;
        }
        let settings = self.settings.read().unwrap().clone();
        let product = triggered.scenario.product;
        let code = product.code(&settings.auto_symbols).to_string();
        let unused_shadow_session = self
            .shadow
            .lock()
            .unwrap()
            .as_ref()
            .is_none_or(|session| session.position().is_none() && session.orders().is_empty());
        let entry_now = self.automation_now();
        let quote = self.quotes.read().unwrap().get(&code).cloned();
        let Some(quote) = quote else {
            self.automation
                .lock()
                .unwrap()
                .entry_failed("섀도 ETF 시세 없음");
            self.emit_automation_state();
            return;
        };
        let book = ShadowBook {
            product: shadow_product(product),
            sequence: self.trade_sequence.fetch_add(1, Ordering::SeqCst) + 1,
            ask_price: quote.ask1.round() as u64,
            ask_qty: quote.ask1_qty,
            bid_price: quote.bid1.round() as u64,
            bid_qty: quote.bid1_qty,
            observed_at: quote.book_ts,
        };
        // Shadow 진입 전까지 수동 실주문이 허용되므로 첫 진입이면 선택 ETF 기준
        // 미수없는매수금액을 읽기 전용 capability로 복사한다. 전용 executor에는
        // place_buy/place_sell/cancel 같은 주문 메서드가 존재하지 않는다.
        let existing_session = { self.shadow.lock().unwrap().clone() };
        let fallback_cash = { self.account.read().unwrap().cash };
        let prepared = self
            .shadow_entry_executor
            .prepare(ShadowEntryRequest {
                existing_session,
                copy_real_cash: unused_shadow_session,
                fallback_cash,
                product: shadow_product(product),
                code: code.clone(),
                target_return_pct: triggered.scenario.target_return_pct,
                now: entry_now,
                book,
            })
            .await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(ShadowExecutionError::Cash(error)) => {
                self.automation.lock().unwrap().entry_failed(format!(
                    "섀도 첫 진입 전 선택 ETF 주문가능현금 조회 실패: {error}"
                ));
                self.emit_automation_state();
                return;
            }
            Err(ShadowExecutionError::Simulation(error)) => {
                self.automation
                    .lock()
                    .unwrap()
                    .entry_failed(format!("섀도 진입 실패: {error:?}"));
                self.emit_automation_state();
                return;
            }
        };
        if !self.auto_quotes_fresh(self.automation_now()) {
            self.automation
                .lock()
                .unwrap()
                .entry_failed("섀도 주문가능현금 조회 중 세 종목 시세가 10초 기준을 벗어났습니다");
            self.emit_automation_state();
            return;
        }
        // 실제 세션은 최종 상태 검증이 끝날 때까지 건드리지 않는다.
        let mut candidate = prepared.session;
        let copied_cash = prepared.copied_cash;
        let outcome = prepared.outcome;
        let trade_id = unique_id("shadow-trade");
        let session = &mut candidate;
        // actor 외부의 OCO 교체나 revision 변경도 커밋 직전에 다시 확인한다.
        if !self.validate_triggered_entry_window(ControlMode::Shadow, &triggered) {
            return;
        }
        if outcome.filled_qty == 0 {
            if let Some(cash) = copied_cash {
                *self.shadow.lock().unwrap() = Some(ShadowSession::start(cash));
                self.automation.lock().unwrap().set_shadow_cash(Some(cash));
                self.persist_automation();
            }
            self.record_shadow_empty_entry(
                &trade_id,
                product,
                &code,
                quote.ask1.round() as u64,
                &outcome,
                entry_now,
            );
            self.automation
                .lock()
                .unwrap()
                .entry_failed("섀도 매도1호가 수량이 없어 IOC가 무체결됐습니다");
            self.emit_automation_state();
            return;
        }
        let position = session.position().expect("체결 뒤 섀도 포지션").clone();
        let cash = session.cash();

        let target_intent_id = outcome
            .target_order_id
            .map(|id| shadow_order_key(&trade_id, ShadowOrderKind::Target, id));
        let owned = OwnedPosition {
            product,
            code: code.clone(),
            entry_qty: position.entry_qty,
            qty: position.qty,
            avg_price: position.entry_vwap,
            last_price: position.last_price as f64,
            target_return_pct: position.target_return_pct,
            target_price: position.target_price,
            first_fill_at: position.first_fill_time,
            exit_deadline: position.first_fill_time + AUTO_MAX_HOLD_SECS,
            target_order_no: target_intent_id.clone(),
            target_org_no: None,
            target_intent_id,
            exit_order_no: None,
            exit_order_org_no: None,
            exit_intent_id: None,
            exit_requested_at: None,
            pending_exit_reason: None,
            exit_qty: 0,
            exit_value: 0.0,
            trade_id: trade_id.clone(),
            shadow: true,
            profit_guard_armed: position.profit_guard_armed,
        };
        {
            let mut runtime = self.automation.lock().unwrap();
            if runtime.mode() != ControlMode::Shadow || !runtime.pending_matches(&triggered) {
                return;
            }
            *self.shadow.lock().unwrap() = Some(candidate);
            runtime.set_shadow_cash(Some(cash));
            runtime.mark_holding(owned.clone());
        }
        self.record_shadow_entry(&owned, &outcome);
        self.persist_automation();
        self.emit_automation_state();
    }

    fn record_shadow_empty_entry(
        &self,
        trade_id: &str,
        product: ProductKind,
        code: &str,
        ask_price: u64,
        outcome: &ShadowEntryOutcome,
        now: i64,
    ) {
        let intent_id = shadow_order_key(trade_id, ShadowOrderKind::EntryIoc, outcome.order_id);
        let runtime = self.automation.lock().unwrap();
        let session_id = runtime.session_id().map(str::to_owned);
        let decision_id = runtime.decision_key().map(str::to_owned);
        drop(runtime);
        let _ = self.ledger.record_order_intent(&NewOrderIntent {
            intent_id: intent_id.clone(),
            session_id,
            trade_id: None,
            decision_id,
            scenario_product: Some(ledger_product(product)),
            execution_kind: LedgerExecutionKind::Shadow,
            origin: LedgerOrigin::Shadow,
            code: code.into(),
            side: LedgerSide::Buy,
            order_type: LedgerOrderType::Virtual,
            qty: outcome.requested_qty,
            price: Some(ask_price),
            requested_at: now,
        });
        let _ = self.ledger.record_order_ack(
            &intent_id,
            &OrderAcknowledgement {
                broker_order_id: Some(intent_id.clone()),
                broker_org_no: None,
                original_order_id: None,
                status: LedgerOrderStatus::Cancelled,
                message: Some("섀도 IOC 무체결".into()),
                acknowledged_at: now,
            },
        );
    }

    fn record_shadow_entry(&self, position: &OwnedPosition, outcome: &ShadowEntryOutcome) {
        let now = position.first_fill_at;
        let session_id = self
            .automation
            .lock()
            .unwrap()
            .session_id()
            .map(str::to_owned);
        let entry_intent = shadow_order_key(
            &position.trade_id,
            ShadowOrderKind::EntryIoc,
            outcome.order_id,
        );
        let _ = self.ledger.record_order_intent(&NewOrderIntent {
            intent_id: entry_intent.clone(),
            session_id: session_id.clone(),
            trade_id: Some(position.trade_id.clone()),
            decision_id: self
                .automation
                .lock()
                .unwrap()
                .decision_key()
                .map(str::to_owned),
            scenario_product: Some(ledger_product(position.product)),
            execution_kind: LedgerExecutionKind::Shadow,
            origin: LedgerOrigin::Shadow,
            code: position.code.clone(),
            side: LedgerSide::Buy,
            order_type: LedgerOrderType::Virtual,
            qty: outcome.requested_qty,
            price: Some(position.avg_price.round() as u64),
            requested_at: now,
        });
        let _ = self.ledger.record_order_ack(
            &entry_intent,
            &OrderAcknowledgement {
                broker_order_id: Some(entry_intent.clone()),
                broker_org_no: None,
                original_order_id: None,
                status: if outcome.filled_qty < outcome.requested_qty {
                    LedgerOrderStatus::PartiallyFilled
                } else {
                    LedgerOrderStatus::Filled
                },
                message: Some("섀도 매도1호가 가상 체결".into()),
                acknowledged_at: now,
            },
        );
        let _ = self.ledger.record_fill(&NewFill {
            fill_key: format!("{entry_intent}-fill"),
            intent_id: Some(entry_intent.clone()),
            broker_order_id: Some(entry_intent),
            broker_fill_id: None,
            execution_kind: LedgerExecutionKind::Shadow,
            origin: LedgerOrigin::Shadow,
            code: position.code.clone(),
            side: LedgerSide::Buy,
            qty: position.qty,
            price: position.avg_price,
            filled_at: now,
            raw_json: None,
        });
        if outcome.target_order_id.is_some() {
            let intent_id = position
                .target_intent_id
                .clone()
                .expect("체결된 섬도 진입은 목표 주문을 보유");
            let _ = self.ledger.record_order_intent(&NewOrderIntent {
                intent_id: intent_id.clone(),
                session_id,
                trade_id: Some(position.trade_id.clone()),
                decision_id: self
                    .automation
                    .lock()
                    .unwrap()
                    .decision_key()
                    .map(str::to_owned),
                scenario_product: Some(ledger_product(position.product)),
                execution_kind: LedgerExecutionKind::Shadow,
                origin: LedgerOrigin::Shadow,
                code: position.code.clone(),
                side: LedgerSide::Sell,
                order_type: LedgerOrderType::Virtual,
                qty: position.qty,
                price: Some(position.target_price),
                requested_at: now,
            });
            let _ = self.ledger.record_order_ack(
                &intent_id,
                &OrderAcknowledgement {
                    broker_order_id: Some(intent_id.clone()),
                    broker_org_no: None,
                    original_order_id: None,
                    status: LedgerOrderStatus::Submitted,
                    message: Some("섀도 목표 주문".into()),
                    acknowledged_at: now,
                },
            );
        }
        let _ = self.ledger.upsert_trade(&NewTrade {
            trade_id: position.trade_id.clone(),
            session_id: self
                .automation
                .lock()
                .unwrap()
                .session_id()
                .map(str::to_owned),
            execution_kind: LedgerExecutionKind::Shadow,
            origin: LedgerOrigin::Shadow,
            code: position.code.clone(),
            entry_qty: position.qty,
            exit_qty: 0,
            entry_avg_price: position.avg_price,
            exit_avg_price: None,
            pnl_rate: None,
            entered_at: position.first_fill_at,
            exited_at: None,
            status: LedgerTradeStatus::Open,
            exit_reason: None,
            updated_at: now,
        });
    }

    fn handle_shadow_trade(&self, quote: &Quote) {
        let settings = self.settings.read().unwrap().clone();
        let product = if quote.code == settings.auto_symbols.leverage {
            Some(ShadowProduct::Leverage)
        } else if quote.code == settings.auto_symbols.inverse {
            Some(ShadowProduct::Inverse)
        } else {
            None
        };
        let Some(product) = product else { return };
        if quote.trade_sequence == 0 {
            return;
        }
        let update = self.shadow.lock().unwrap().as_mut().and_then(|session| {
            session
                .on_trade_tick(ShadowTradeTick {
                    product,
                    sequence: quote.trade_sequence,
                    price: quote.price.round() as u64,
                    volume: quote.volume.max(0.0).round() as u64,
                    at: quote.trade_ts,
                })
                .ok()
        });
        if let Some(update) = update {
            self.apply_shadow_update(update);
        }
    }

    fn handle_shadow_book(&self, quote: &Quote) {
        let settings = self.settings.read().unwrap().clone();
        let product = if quote.code == settings.auto_symbols.leverage {
            Some(ShadowProduct::Leverage)
        } else if quote.code == settings.auto_symbols.inverse {
            Some(ShadowProduct::Inverse)
        } else {
            None
        };
        let Some(product) = product else { return };
        let update = self.shadow.lock().unwrap().as_mut().and_then(|session| {
            session
                .on_book(ShadowBook {
                    product,
                    sequence: self.trade_sequence.fetch_add(1, Ordering::SeqCst) + 1,
                    ask_price: quote.ask1.round() as u64,
                    ask_qty: quote.ask1_qty,
                    bid_price: quote.bid1.round() as u64,
                    bid_qty: quote.bid1_qty,
                    observed_at: quote.book_ts,
                })
                .ok()
        });
        if let Some(update) = update {
            self.apply_shadow_update(update);
        }
    }

    fn poll_shadow(&self, now: i64) {
        let update = self
            .shadow
            .lock()
            .unwrap()
            .as_mut()
            .and_then(|session| session.poll(now).ok());
        if let Some(update) = update {
            self.apply_shadow_update(update);
        }
    }

    fn apply_shadow_update(&self, update: crate::automation::shadow::ShadowUpdate) {
        let previous = self.automation.lock().unwrap().position().cloned();
        let (position, cash, exit_vwap, orders) = {
            let shadow = self.shadow.lock().unwrap();
            let Some(session) = shadow.as_ref() else {
                return;
            };
            let latest_orders = session.latest_trade_orders();
            let sell_orders: Vec<_> = latest_orders
                .iter()
                .filter(|order| order.side == ShadowSide::Sell && order.filled_qty > 0)
                .collect();
            let total_qty: u64 = sell_orders.iter().map(|order| order.filled_qty).sum();
            let total_value: f64 = sell_orders
                .iter()
                .map(|order| order.avg_fill_price.unwrap_or_default() * order.filled_qty as f64)
                .sum();
            (
                session.position().cloned(),
                session.cash(),
                (total_qty > 0).then(|| total_value / total_qty as f64),
                latest_orders.to_vec(),
            )
        };
        if let Some(previous) = previous.as_ref() {
            // 새 강제청산 주문은 체결 장부보다 intent/ack를 반드시 먼저 남긴다.
            for order in orders.iter().filter(|order| order.side == ShadowSide::Sell) {
                self.ensure_shadow_sell_order(previous, order);
            }
            if let Some(fill) = &update.fill {
                if let Some(order) = orders.iter().find(|order| order.id == fill.order_id) {
                    let intent_id = shadow_intent_for_order(previous, order);
                    let _ = self.ledger.record_fill(&NewFill {
                        fill_key: format!(
                            "{intent_id}:fill:{}:{}:{}",
                            fill.at, fill.qty, order.filled_qty
                        ),
                        intent_id: Some(intent_id.clone()),
                        broker_order_id: Some(intent_id),
                        broker_fill_id: None,
                        execution_kind: LedgerExecutionKind::Shadow,
                        origin: LedgerOrigin::Shadow,
                        code: previous.code.clone(),
                        side: match fill.side {
                            ShadowSide::Buy => LedgerSide::Buy,
                            ShadowSide::Sell => LedgerSide::Sell,
                        },
                        qty: fill.qty,
                        price: fill.price as f64,
                        filled_at: fill.at,
                        raw_json: None,
                    });
                }
            }
            for order in orders.iter().filter(|order| order.side == ShadowSide::Sell) {
                self.sync_shadow_order_status(previous, order);
            }
        }
        {
            let mut runtime = self.automation.lock().unwrap();
            runtime.set_shadow_cash(Some(cash));
            if let Some(position) = &position {
                runtime.sync_position(position.qty, position.last_price as f64);
            } else if update.position_closed {
                runtime.complete_exit();
                runtime.set_next_decision_at(self.next_automation_slot(now_kst_fake_epoch() + 1));
            }
        }
        if update.position_closed {
            if let Some(previous) = previous {
                let exit_price = exit_vwap.unwrap_or(previous.last_price);
                let pnl = (exit_price / previous.avg_price - 1.0) * 100.0;
                let _ = self.ledger.upsert_trade(&NewTrade {
                    trade_id: previous.trade_id,
                    session_id: self
                        .automation
                        .lock()
                        .unwrap()
                        .session_id()
                        .map(str::to_owned),
                    execution_kind: LedgerExecutionKind::Shadow,
                    origin: LedgerOrigin::Shadow,
                    code: previous.code,
                    entry_qty: previous.entry_qty.max(previous.qty),
                    exit_qty: previous.entry_qty.max(previous.qty),
                    entry_avg_price: previous.avg_price,
                    exit_avg_price: Some(exit_price),
                    pnl_rate: Some(pnl),
                    entered_at: previous.first_fill_at,
                    exited_at: Some(now_kst_fake_epoch()),
                    status: LedgerTradeStatus::Closed,
                    exit_reason: update
                        .exit_reason
                        .map(shadow_exit_reason)
                        .map(str::to_owned),
                    updated_at: now_kst_fake_epoch(),
                });
                self.emit("trade-recorded", &serde_json::json!({ "kind": "shadow" }));
            }
        }
        self.persist_automation();
        self.emit_automation_state();
    }

    fn ensure_shadow_sell_order(&self, position: &OwnedPosition, order: &ShadowOrder) {
        let intent_id = shadow_intent_for_order(position, order);
        if self.ledger.get_order(&intent_id).ok().flatten().is_some() {
            return;
        }
        let session_id = self
            .automation
            .lock()
            .unwrap()
            .session_id()
            .map(str::to_owned);
        let decision_id = self
            .automation
            .lock()
            .unwrap()
            .decision_key()
            .map(str::to_owned);
        if let Err(error) = self.ledger.record_order_intent(&NewOrderIntent {
            intent_id: intent_id.clone(),
            session_id,
            trade_id: Some(position.trade_id.clone()),
            decision_id,
            scenario_product: Some(ledger_product(position.product)),
            execution_kind: LedgerExecutionKind::Shadow,
            origin: LedgerOrigin::Shadow,
            code: position.code.clone(),
            side: LedgerSide::Sell,
            order_type: LedgerOrderType::Virtual,
            qty: order.requested_qty,
            price: order.limit_price,
            requested_at: order.created_at,
        }) {
            tracing::error!("섀도 매도 intent 기록 실패({intent_id}): {error}");
            return;
        }
        let message = match order.kind {
            ShadowOrderKind::Target => "섀도 목표 주문",
            ShadowOrderKind::ForcedExit(_) => "섀도 강제청산 주문",
            ShadowOrderKind::EntryIoc => "섀도 진입 주문",
        };
        if let Err(error) = self.ledger.record_order_ack(
            &intent_id,
            &OrderAcknowledgement {
                broker_order_id: Some(intent_id.clone()),
                broker_org_no: None,
                original_order_id: None,
                // 체결이 이미 계산된 update라도 먼저 접수 상태를 기록한 뒤 fill을 쓴다.
                status: LedgerOrderStatus::Submitted,
                message: Some(message.into()),
                acknowledged_at: order.created_at,
            },
        ) {
            tracing::error!("섀도 매도 ack 기록 실패({intent_id}): {error}");
        }
    }

    fn sync_shadow_order_status(&self, position: &OwnedPosition, order: &ShadowOrder) {
        let intent_id = shadow_intent_for_order(position, order);
        let (status, message) = match order.status {
            ShadowOrderStatus::Pending => (LedgerOrderStatus::Submitted, None),
            ShadowOrderStatus::PartiallyFilled => (LedgerOrderStatus::PartiallyFilled, None),
            ShadowOrderStatus::Filled => (LedgerOrderStatus::Filled, None),
            ShadowOrderStatus::Cancelled => (LedgerOrderStatus::Cancelled, Some("가상 주문 취소")),
            ShadowOrderStatus::PartiallyFilledCancelled => (
                LedgerOrderStatus::Cancelled,
                Some("가상 주문 부분체결 후 잔량 취소"),
            ),
        };
        if let Err(error) =
            self.ledger
                .update_order_status(&intent_id, status, message, order.updated_at)
        {
            tracing::error!("섀도 주문 상태 갱신 실패({intent_id}): {error}");
        }
    }

    /// Auto IOC 진입 주문의 정확 조회가 저장한 intent와 같은 경제 주문인지 검증한다.
    /// 미체결 목록에서 사라졌다는 사실은 이 검증과 종결 판정을 대신할 수 없다.
    fn validate_entry_ioc_status(
        intent: &crate::ledger::OrderRecord,
        status: &crate::broker::BrokerOrderStatus,
    ) -> Result<(), String> {
        let Some(order_no) = intent.broker_order_id.as_deref() else {
            return Err("정확 조회할 Auto 진입 주문번호가 장부에 없습니다".into());
        };
        if status.trading_date != intent.trading_date
            || status.order_no != order_no
            || status.code != intent.code
            || status.side != Side::Buy
            || status.ordered_qty != intent.qty
            || status.ordered_at < intent.requested_at
            || (status.filled_qty > 0
                && !status.org_no.trim().is_empty()
                && intent
                    .broker_org_no
                    .as_deref()
                    .is_some_and(|org_no| !org_no.trim().is_empty() && org_no != status.org_no))
            || intent
                .price
                .is_some_and(|limit| status.filled_qty > 0 && status.avg_fill_price > limit as f64)
        {
            return Err("정확 Auto IOC 조회 결과가 저장된 진입 intent와 일치하지 않습니다".into());
        }
        Ok(())
    }

    /// 주문번호가 확정된 Auto IOC는 KIS 일별 주문체결의 정확한 누적 상태가
    /// terminal이 될 때까지 기다린다. `None`과 조회 지연은 주문 부재가 아니다.
    async fn wait_entry_ioc_terminal(
        &self,
        intent: &crate::ledger::OrderRecord,
    ) -> Result<Option<crate::broker::BrokerOrderStatus>, String> {
        let order_no = intent
            .broker_order_id
            .as_deref()
            .ok_or_else(|| "Auto IOC 종결 조회에 주문번호가 없습니다".to_string())?;
        let mut had_success = false;
        let mut last_error = None;
        for attempt in 0..8 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            match self
                .broker
                .order_status(&intent.trading_date, order_no)
                .await
            {
                Ok(Some(status)) => {
                    had_success = true;
                    Self::validate_entry_ioc_status(intent, &status)?;
                    if status.is_terminal() {
                        return Ok(Some(status));
                    }
                }
                Ok(None) => had_success = true,
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        if had_success {
            Ok(None)
        } else {
            Err(last_error.unwrap_or_else(|| "Auto IOC 정확 주문상태 조회 실패".into()))
        }
    }

    /// pending 조정 중 정확한 terminal 상태가 보이면 누적 체결을 authoritative
    /// 입력으로 병합한다. 0체결은 계좌 기준수량까지 같을 때만 pending을 해제한다.
    async fn merge_exact_pending_entry_terminal(
        &self,
        account: &AccountSnapshot,
        today_fills: &mut Vec<crate::broker::BrokerFill>,
    ) -> Result<Option<bool>, String> {
        let Some(pending) = self.automation.lock().unwrap().pending_entry().cloned() else {
            return Ok(Some(true));
        };
        let Some(intent) = self
            .ledger
            .get_order(&pending.intent_id)
            .map_err(|error| error.to_string())?
        else {
            return Ok(None);
        };
        let Some(order_no) = intent.broker_order_id.as_deref() else {
            // 주문번호 없는 불명확 POST는 빈 조회만으로 부재를 추론하지 않는다.
            return Ok(None);
        };
        let Some(status) = self
            .broker
            .order_status(&intent.trading_date, order_no)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(Some(false));
        };
        Self::validate_entry_ioc_status(&intent, &status)?;
        if !status.is_terminal() {
            return Ok(Some(false));
        }

        if status.filled_qty == 0 {
            let actual_qty = account
                .positions
                .iter()
                .find(|position| position.code == intent.code)
                .map(|position| position.qty)
                .unwrap_or(0);
            if actual_qty != pending.baseline_qty {
                return Err(format!(
                    "정확 종결된 Auto IOC는 0체결이지만 계좌 수량이 다릅니다: 기준 {}주 / 현재 {actual_qty}주",
                    pending.baseline_qty
                ));
            }
            let ledger_status = if status.rejected_qty >= status.ordered_qty {
                LedgerOrderStatus::Rejected
            } else {
                LedgerOrderStatus::Cancelled
            };
            self.ledger
                .update_order_status(
                    &pending.intent_id,
                    ledger_status,
                    Some("정확 주문상태로 Auto IOC 0체결 종결 확인"),
                    now_kst_fake_epoch(),
                )
                .map_err(|error| error.to_string())?;
            self.automation
                .lock()
                .unwrap()
                .entry_failed("자동 IOC 진입 무체결");
            return Ok(Some(true));
        }

        self.ledger
            .update_order_status(
                &pending.intent_id,
                if status.filled_qty < status.ordered_qty {
                    LedgerOrderStatus::PartiallyFilled
                } else {
                    LedgerOrderStatus::Filled
                },
                Some("정확 주문상태로 Auto IOC 종결 확인"),
                now_kst_fake_epoch(),
            )
            .map_err(|error| error.to_string())?;

        let org_no = if status.org_no.trim().is_empty() {
            intent.broker_org_no.clone().unwrap_or_default()
        } else {
            status.org_no.clone()
        };
        today_fills.retain(|fill| {
            !(fill.order_no == status.order_no
                && fill.code == status.code
                && fill.side == Side::Buy)
        });
        today_fills.push(crate::broker::BrokerFill {
            fill_id: format!(
                "exact-entry:{}:{}:{}:{}",
                status.trading_date, status.order_no, status.filled_qty, status.avg_fill_price
            ),
            order_no: status.order_no,
            original_order_no: status.original_order_no,
            org_no,
            code: status.code,
            side: status.side,
            qty: status.filled_qty,
            price: status.avg_fill_price,
            filled_at: status.ordered_at,
        });
        Ok(Some(true))
    }

    /// 저장된 실주문 intent와 일별 주문체결의 정확 조회 결과가 같은 경제 주문인지
    /// 검증한다. 주문번호만 같거나 미체결 목록에서 사라진 사실은 식별 근거가 아니다.
    fn validate_exact_order_status(
        intent: &OrderRecord,
        status: &crate::broker::BrokerOrderStatus,
    ) -> Result<(), String> {
        let order_no = intent
            .broker_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "정확 조회할 브로커 주문번호가 장부에 없습니다".to_string())?;
        let org_no = intent
            .broker_org_no
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "정확 조회할 주문조직번호가 장부에 없습니다".to_string())?;
        let expected_side = match intent.side {
            LedgerSide::Buy => Side::Buy,
            LedgerSide::Sell => Side::Sell,
        };
        let original_matches = intent.original_order_id.as_deref().is_none_or(|original| {
            original.trim().is_empty() || status.original_order_no == original
        });
        let price_matches = if status.filled_qty == 0 || status.avg_fill_price <= 0.0 {
            true
        } else {
            match (intent.order_type, intent.price, intent.side) {
                (
                    LedgerOrderType::Limit | LedgerOrderType::IocLimit,
                    Some(limit),
                    LedgerSide::Buy,
                ) => status.avg_fill_price <= limit as f64,
                (
                    LedgerOrderType::Limit | LedgerOrderType::IocLimit,
                    Some(limit),
                    LedgerSide::Sell,
                ) => status.avg_fill_price >= limit as f64,
                (LedgerOrderType::Market, _, _) => true,
                _ => false,
            }
        };
        if status.trading_date != intent.trading_date
            || status.order_no != order_no
            || status.org_no != org_no
            || status.code != intent.code
            || status.side != expected_side
            || status.ordered_qty != intent.qty
            || status.ordered_at < intent.requested_at
            || !original_matches
            || !price_matches
        {
            return Err(format!(
                "정확 주문상태가 저장된 intent identity와 일치하지 않습니다: {}/{order_no}",
                intent.trading_date
            ));
        }
        Ok(())
    }

    fn terminal_ledger_order_status(
        status: &crate::broker::BrokerOrderStatus,
    ) -> LedgerOrderStatus {
        if status.filled_qty >= status.ordered_qty {
            LedgerOrderStatus::Filled
        } else if status.filled_qty > 0 {
            LedgerOrderStatus::PartiallyFilled
        } else if status.rejected_qty >= status.ordered_qty {
            LedgerOrderStatus::Rejected
        } else {
            LedgerOrderStatus::Cancelled
        }
    }

    /// 주문번호·거래일·조직번호와 주문 형태를 모두 대조한 일별 누적상태가 terminal일
    /// 때만 반환한다. 조회 부재와 일시적인 미체결 목록 부재는 종결로 해석하지 않는다.
    async fn wait_exact_order_terminal(
        &self,
        intent: &OrderRecord,
    ) -> Result<crate::broker::BrokerOrderStatus, String> {
        if intent.order_type == LedgerOrderType::Cancel {
            return Err("취소 요청 intent로 원주문의 경제 종결을 추론할 수 없습니다".into());
        }
        let order_no = intent
            .broker_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "정확 종결을 조회할 주문번호가 없습니다".to_string())?;
        let mut had_success = false;
        let mut last_error = None;
        for attempt in 0..8 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            match self
                .broker
                .order_status(&intent.trading_date, order_no)
                .await
            {
                Ok(Some(status)) => {
                    had_success = true;
                    Self::validate_exact_order_status(intent, &status)?;
                    if status.is_terminal() {
                        return Ok(status);
                    }
                }
                Ok(None) => had_success = true,
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        if had_success {
            Err(format!(
                "정확 주문상태가 아직 terminal이 아닙니다: {}/{}",
                intent.trading_date, order_no
            ))
        } else {
            Err(last_error.unwrap_or_else(|| "정확 주문상태 조회 실패".into()))
        }
    }

    /// 주문 응답에서 주문번호를 얻지 못한 경우 같은 종목·방향·요청시각 이후 체결을
    /// 여러 번 합쳐 조회한다. 불명확 응답 뒤 동일 POST는 절대 반복하지 않는다.
    async fn wait_recent_fills(
        &self,
        code: &str,
        side: Side,
        requested_at: i64,
    ) -> Result<Vec<crate::broker::BrokerFill>, String> {
        let mut by_order = HashMap::<String, crate::broker::BrokerFill>::new();
        let mut had_success = false;
        let mut last_error = None;
        for attempt in 0..8 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            match self.broker.today_fills().await {
                Ok(fills) => {
                    had_success = true;
                    for fill in fills.into_iter().filter(|fill| {
                        fill.code == code && fill.side == side && fill.filled_at >= requested_at
                    }) {
                        let key = format!("{}:{}", fill.org_no, fill.order_no);
                        let replace = by_order.get(&key).is_none_or(|current| {
                            fill.qty > current.qty
                                || (fill.qty == current.qty && fill.filled_at >= current.filled_at)
                        });
                        if replace {
                            by_order.insert(key, fill);
                        }
                    }
                }
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        if !had_success {
            return Err(last_error.unwrap_or_else(|| "당일 체결 조회 실패".into()));
        }
        let mut fills: Vec<_> = by_order.into_values().collect();
        fills.sort_by_key(|fill| fill.filled_at);
        Ok(fills)
    }

    /// IOC 체결분이 실포지션으로 생긴 뒤 목표 주문 보호를 완성하지 못하면
    /// `Suspended`에 멈추지 않고 시장가 안전청산 상태로 전환한다.
    fn protect_staged_auto_position(&self, message: impl Into<String>) -> bool {
        let message = message.into();
        let should_exit = {
            let mut runtime = self.automation.lock().unwrap();
            if runtime.mode() != ControlMode::Auto || runtime.position().is_none() {
                runtime.suspend(message);
                false
            } else {
                runtime.clear_pending_entry();
                if runtime.phase() == crate::types::AutomationPhase::Holding {
                    runtime.begin_exit("target_order_failed");
                }
                if runtime.phase() == crate::types::AutomationPhase::ExitPending {
                    runtime.exit_waiting(message);
                    true
                } else {
                    runtime.suspend(message);
                    false
                }
            }
        };
        self.persist_automation();
        self.emit_automation_state();
        should_exit
    }

    async fn execute_real_entry(self: Arc<Self>, triggered: TriggeredScenario) {
        let _actor = self.order_actor.lock().await;
        if !self.validate_triggered_entry_window(ControlMode::Auto, &triggered) {
            return;
        }
        let now = self.automation_now();
        if !self.auto_quotes_fresh(now) {
            self.automation
                .lock()
                .unwrap()
                .entry_failed("자동 진입 직전 세 종목 시세가 10초 기준을 벗어났습니다");
            self.emit_automation_state();
            return;
        }
        let settings = self.settings.read().unwrap().clone();
        let product = triggered.scenario.product;
        let code = product.code(&settings.auto_symbols).to_string();
        // Auto POST 직전에는 캐시 신선도와 무관하게 계좌·미체결을 다시 읽는다.
        // Idle 중 HTS/지연 주문으로 생긴 노출 위에 자동 수량을 겹쳐 사지 않는다.
        if !self.refresh_account().await {
            self.automation
                .lock()
                .unwrap()
                .suspend("자동 진입 전 계좌 조회 실패");
            self.persist_automation();
            self.emit_automation_state();
            return;
        }
        let open_orders = match self.broker.open_orders().await {
            Ok(orders) => orders,
            Err(error) => {
                self.automation.lock().unwrap().suspend(format!(
                    "자동 진입 전 미체결 조회 실패로 주문을 중단합니다: {error}"
                ));
                self.persist_automation();
                self.emit_automation_state();
                return;
            }
        };
        let account = self.account_snapshot();
        if Self::has_unknown_auto_exposure(&settings, None, &account, &open_orders) {
            self.automation.lock().unwrap().suspend(
                "자동 진입 직전 양방향 ETF에 기존 보유·미체결이 확인되어 주문을 중단했습니다",
            );
            self.persist_automation();
            self.emit_automation_state();
            return;
        }
        let cash = match self.broker.orderable_cash(&code).await {
            Ok(cash) => cash,
            Err(error) => {
                self.automation.lock().unwrap().suspend(format!(
                    "자동 진입 전 선택 ETF 주문가능현금 조회 실패로 주문을 중단합니다: {error}"
                ));
                self.persist_automation();
                self.emit_automation_state();
                return;
            }
        };
        // 계좌·미체결·선택종목 현금 조회가 KIS 백오프로 오래 걸릴 수 있다.
        // 주문 가격과 수량은 모든 안전 조회가 끝난 마지막 순간의 신선한 ETF
        // 호가로 다시 계산한다.
        if !self.auto_quotes_fresh(self.automation_now()) {
            self.automation
                .lock()
                .unwrap()
                .entry_failed("자동 안전 조회 중 세 종목 시세가 10초 기준을 벗어났습니다");
            self.emit_automation_state();
            return;
        }
        let quote = self.quotes.read().unwrap().get(&code).cloned();
        let Some(quote) = quote else {
            self.automation
                .lock()
                .unwrap()
                .entry_failed("자동 ETF 시세 없음");
            self.emit_automation_state();
            return;
        };
        let ask = if quote.ask1 > 0.0 {
            quote.ask1
        } else {
            quote.price
        };
        let limit = buy_limit_price(ask.round() as u64, true);
        let baseline_qty = self.cached_position_qty(&code);
        let qty = max_buy_qty(cash, ask.round() as u64);
        if qty == 0 {
            self.automation
                .lock()
                .unwrap()
                .entry_failed("자동 진입 주문가능금액 부족");
            self.emit_automation_state();
            return;
        }
        let entry_requested_at = self.automation_now();
        let trade_id = unique_id("auto-trade");
        let intent_id = unique_id("auto-entry");
        let (session_id, decision_id) = {
            let runtime = self.automation.lock().unwrap();
            (
                runtime.session_id().map(str::to_owned),
                runtime.decision_key().map(str::to_owned),
            )
        };
        let pending_entry = PendingAutoEntry {
            intent_id: intent_id.clone(),
            trade_id: trade_id.clone(),
            baseline_qty,
            product,
            target_return_pct: triggered.scenario.target_return_pct,
        };
        if !self
            .automation
            .lock()
            .unwrap()
            .set_pending_entry(pending_entry)
        {
            self.automation
                .lock()
                .unwrap()
                .suspend("자동 진입 복구 표식을 설정하지 못했습니다");
            self.emit_automation_state();
            return;
        }
        if let Err(error) = self.persist_automation_required() {
            self.automation
                .lock()
                .unwrap()
                .entry_failed(format!("진입 전 상태 저장 실패: {error}"));
            self.persist_automation();
            self.emit_automation_state();
            return;
        }
        let local_order_id = match self.ledger.record_order_intent(&NewOrderIntent {
            intent_id: intent_id.clone(),
            session_id: session_id.clone(),
            trade_id: Some(trade_id.clone()),
            decision_id: decision_id.clone(),
            scenario_product: Some(ledger_product(product)),
            execution_kind: LedgerExecutionKind::Real,
            origin: LedgerOrigin::Auto,
            code: code.clone(),
            side: LedgerSide::Buy,
            order_type: LedgerOrderType::IocLimit,
            qty,
            price: Some(limit),
            requested_at: entry_requested_at,
        }) {
            Ok(id) => id,
            Err(error) => {
                self.automation
                    .lock()
                    .unwrap()
                    .entry_failed(format!("진입 의도 장부 기록 실패: {error}"));
                self.persist_automation();
                self.emit_automation_state();
                return;
            }
        };

        // 계좌 동기화와 장부 기록 중 다음 슬롯 또는 15:15 경계를 넘을 수 있다.
        // 주문 상태를 Dispatching으로 바꾸기 직전에 동일 winner와 시각을 다시 확인한다.
        if !self.validate_triggered_entry_window(ControlMode::Auto, &triggered) {
            let _ = self.ledger.update_order_status(
                &intent_id,
                LedgerOrderStatus::Cancelled,
                Some("진입 슬롯 만료"),
                self.automation_now(),
            );
            return;
        }

        match self
            .ledger
            .begin_order_dispatch(&intent_id, self.automation_now())
        {
            Ok(true) => {}
            Ok(false) => {
                self.automation
                    .lock()
                    .unwrap()
                    .suspend("진입 주문이 이미 제출 중이어서 중복 POST를 차단했습니다");
                self.persist_automation();
                self.emit_automation_state();
                return;
            }
            Err(error) => {
                self.automation
                    .lock()
                    .unwrap()
                    .suspend(format!("진입 주문 제출 상태 기록 실패: {error}"));
                self.persist_automation();
                self.emit_automation_state();
                return;
            }
        }

        // 동기식 dispatch CAS가 경계 직전에 시작된 경우도 실제 POST 전 마지막으로 막는다.
        if !self.validate_triggered_entry_window(ControlMode::Auto, &triggered) {
            let _ = self.ledger.update_order_status(
                &intent_id,
                LedgerOrderStatus::Cancelled,
                Some("진입 POST 직전 슬롯 만료"),
                self.automation_now(),
            );
            return;
        }
        if !self.auto_quotes_fresh(self.automation_now()) {
            let _ = self.ledger.update_order_status(
                &intent_id,
                LedgerOrderStatus::Cancelled,
                Some("진입 POST 직전 시세 신선도 상실"),
                self.automation_now(),
            );
            self.automation
                .lock()
                .unwrap()
                .entry_failed("진입 POST 직전 세 종목 시세가 10초 기준을 벗어났습니다");
            self.persist_automation();
            self.emit_automation_state();
            return;
        }

        // 이 시점부터 앱이 종료되거나 ACK가 불명확해도 별도 조정 태스크가 동일 매수를
        // 재POST하지 않고 pending intent를 체결·계좌와 끝까지 맞춘다.
        self.start_pending_entry_reconcile();
        let ack = self.broker.place_buy(&code, qty, limit, true).await;
        let (entry_order_no, entry_org_no) = match ack {
            Ok(ack) => {
                if let Err(error) = self.ledger.record_order_ack(
                    &intent_id,
                    &OrderAcknowledgement {
                        broker_order_id: Some(ack.order_no.clone()),
                        broker_org_no: Some(ack.org_no.clone()),
                        original_order_id: None,
                        status: LedgerOrderStatus::Submitted,
                        message: Some(ack.message.clone()),
                        acknowledged_at: now_kst_fake_epoch(),
                    },
                ) {
                    self.automation.lock().unwrap().suspend(format!(
                        "자동 IOC ACK 장부 기록 실패로 조정을 계속합니다: {error}"
                    ));
                    self.persist_automation();
                    self.emit_automation_state();
                    return;
                }
                (ack.order_no, ack.org_no)
            }
            Err(error) if error.is_confirmed_order_rejection() => {
                // check_order_rt가 반환한 업무거부는 POST 결과가 확정됐으므로 같은
                // 주문의 지연 체결 가능성이 없다. 이 경우에만 pending을 해제한다.
                if let Err(ledger_error) = self.ledger.record_order_ack(
                    &intent_id,
                    &OrderAcknowledgement {
                        broker_order_id: None,
                        broker_org_no: None,
                        original_order_id: None,
                        status: LedgerOrderStatus::Rejected,
                        message: Some(error.to_string()),
                        acknowledged_at: now_kst_fake_epoch(),
                    },
                ) {
                    self.automation.lock().unwrap().suspend(format!(
                        "확정 거부된 Auto 진입 장부 기록 실패로 pending 조정을 유지합니다: {ledger_error}"
                    ));
                    self.persist_automation();
                    self.emit_automation_state();
                    return;
                }
                self.automation
                    .lock()
                    .unwrap()
                    .entry_failed(format!("자동 진입 확정 거부: {error}"));
                self.persist_automation();
                self.emit_automation_state();
                return;
            }
            Err(error) => {
                // 응답 불명확 가능성을 전제로 같은 POST를 되풀이하지 않고 조회로 먼저 조정한다.
                if let Err(ledger_error) = self.ledger.record_order_ack(
                    &intent_id,
                    &OrderAcknowledgement {
                        broker_order_id: None,
                        broker_org_no: None,
                        original_order_id: None,
                        status: LedgerOrderStatus::Unknown,
                        message: Some(error.to_string()),
                        acknowledged_at: now_kst_fake_epoch(),
                    },
                ) {
                    self.automation.lock().unwrap().suspend(format!(
                        "자동 진입 응답과 ACK 장부 기록이 모두 불명확합니다: {error}; {ledger_error}"
                    ));
                    self.persist_automation();
                    self.emit_automation_state();
                    return;
                }
                let recent_fills = match self
                    .wait_recent_fills(&code, Side::Buy, entry_requested_at)
                    .await
                {
                    Ok(fills) => fills,
                    Err(query_error) => {
                        self.automation.lock().unwrap().suspend(format!(
                            "자동 진입 응답과 체결 조회가 모두 불명확합니다: {error}; {query_error}"
                        ));
                        self.persist_automation();
                        self.emit_automation_state();
                        return;
                    }
                };
                let matching_fills = recent_fills
                    .into_iter()
                    .filter(|fill| {
                        !fill.order_no.trim().is_empty()
                            && fill.qty > 0
                            && fill.qty <= qty
                            && fill.price > 0.0
                            && fill.price <= limit as f64
                    })
                    .collect::<Vec<_>>();
                let candidate_orders: HashSet<_> = matching_fills
                    .iter()
                    .map(|fill| fill.order_no.as_str())
                    .collect();
                if candidate_orders.len() > 1 {
                    self.automation.lock().unwrap().suspend(
                        "자동 진입 응답 불명확 후 매수 주문 후보가 둘 이상이라 소유권을 확정할 수 없습니다",
                    );
                    self.persist_automation();
                    self.emit_automation_state();
                    return;
                }
                let recovered = if let Some(order_no) = candidate_orders.into_iter().next() {
                    matching_fills
                        .iter()
                        .filter(|fill| fill.order_no == order_no)
                        .max_by_key(|fill| (fill.qty, fill.filled_at))
                        .map(|fill| {
                            (
                                fill.order_no.clone(),
                                fill.org_no.clone(),
                                "응답 불명확 후 체결 조회로 주문 확인".to_string(),
                            )
                        })
                } else {
                    let open_orders = match self.broker.open_orders().await {
                        Ok(orders) => orders,
                        Err(query_error) => {
                            self.automation.lock().unwrap().suspend(format!(
                                "자동 진입 응답 불명확 후 미체결 조회 실패: {error}; {query_error}"
                            ));
                            self.persist_automation();
                            self.emit_automation_state();
                            return;
                        }
                    };
                    let mut matching_open = open_orders
                        .into_iter()
                        .filter(|order| {
                            order.code == code
                                && order.side == Side::Buy
                                && order.ordered_at >= entry_requested_at
                                && order.cancelable_qty > 0
                                && order.ordered_qty == qty
                                && order.price.round() as u64 == limit
                        })
                        .collect::<Vec<_>>();
                    if matching_open.len() > 1 {
                        self.automation.lock().unwrap().suspend(
                            "자동 진입 응답 불명확 후 정확히 일치하는 미체결 주문이 둘 이상입니다",
                        );
                        self.persist_automation();
                        self.emit_automation_state();
                        return;
                    }
                    matching_open.pop().map(|order| {
                        (
                            order.order_no,
                            order.org_no,
                            "응답 불명확 후 미체결 조회로 주문 확인".to_string(),
                        )
                    })
                };
                let Some((order_no, org_no, recovery_message)) = recovered else {
                    self.automation
                        .lock()
                        .unwrap()
                        .suspend(format!(
                            "자동 진입 응답이 불명확하고 주문번호를 찾지 못해 pending 조정을 계속합니다: {error}"
                        ));
                    self.persist_automation();
                    self.emit_automation_state();
                    return;
                };
                if let Err(ledger_error) = self.ledger.record_order_ack(
                    &intent_id,
                    &OrderAcknowledgement {
                        broker_order_id: Some(order_no.clone()),
                        broker_org_no: (!org_no.trim().is_empty()).then(|| org_no.clone()),
                        original_order_id: None,
                        status: LedgerOrderStatus::Submitted,
                        message: Some(recovery_message),
                        acknowledged_at: now_kst_fake_epoch(),
                    },
                ) {
                    self.automation.lock().unwrap().suspend(format!(
                        "복구한 Auto IOC 주문번호 장부 기록 실패로 조정을 계속합니다: {ledger_error}"
                    ));
                    self.persist_automation();
                    self.emit_automation_state();
                    return;
                }
                (order_no, org_no)
            }
        };

        let entry_order = match self.ledger.get_order(&intent_id) {
            Ok(Some(order)) => order,
            Ok(None) => {
                self.automation
                    .lock()
                    .unwrap()
                    .suspend("제출된 Auto IOC 진입 intent 장부가 사라져 조정을 계속합니다");
                self.persist_automation();
                self.emit_automation_state();
                return;
            }
            Err(error) => {
                self.automation
                    .lock()
                    .unwrap()
                    .suspend(format!("Auto IOC 진입 intent 조회 실패: {error}"));
                self.persist_automation();
                self.emit_automation_state();
                return;
            }
        };
        let terminal = match self.wait_entry_ioc_terminal(&entry_order).await {
            Ok(Some(status)) => status,
            Ok(None) => {
                self.automation.lock().unwrap().suspend(
                    "Auto IOC의 정확한 terminal 상태가 아직 확인되지 않아 pending 조정을 계속합니다",
                );
                self.persist_automation();
                self.emit_automation_state();
                return;
            }
            Err(error) => {
                self.automation.lock().unwrap().suspend(format!(
                    "Auto IOC 정확 주문상태 조회 실패로 pending 조정을 계속합니다: {error}"
                ));
                self.persist_automation();
                self.emit_automation_state();
                return;
            }
        };
        let terminal_rejected = terminal.rejected_qty >= terminal.ordered_qty;
        let fill_org_no = if terminal.org_no.trim().is_empty() {
            entry_org_no
        } else {
            terminal.org_no.clone()
        };
        let mut fills = (terminal.filled_qty > 0)
            .then(|| crate::broker::BrokerFill {
                fill_id: format!(
                    "exact-entry:{}:{}:{}:{}",
                    terminal.trading_date,
                    entry_order_no,
                    terminal.filled_qty,
                    terminal.avg_fill_price
                ),
                order_no: entry_order_no,
                original_order_no: terminal.original_order_no,
                org_no: fill_org_no,
                code: terminal.code,
                side: terminal.side,
                qty: terminal.filled_qty,
                price: terminal.avg_fill_price,
                filled_at: terminal.ordered_at,
            })
            .into_iter()
            .collect::<Vec<_>>();

        let account_confirmed = self.refresh_account().await;
        if !account_confirmed && fills.is_empty() {
            self.automation
                .lock()
                .unwrap()
                .suspend("IOC는 종결됐지만 계좌 조회 실패로 무체결을 확정할 수 없습니다");
            self.persist_automation();
            self.emit_automation_state();
            return;
        }
        if account_confirmed {
            let actual_qty = self.cached_position_qty(&code);
            if fills.is_empty() && actual_qty != baseline_qty {
                self.automation.lock().unwrap().suspend(format!(
                    "정확 종결된 Auto IOC는 0체결이지만 계좌 수량이 다릅니다: 기준 {baseline_qty}주 / 현재 {actual_qty}주"
                ));
                self.persist_automation();
                self.emit_automation_state();
                return;
            }
            let account_delta = self.cached_position_qty(&code).saturating_sub(baseline_qty);
            let rest_qty: u64 = fills.iter().map(|fill| fill.qty).sum();
            if account_delta > rest_qty {
                self.automation.lock().unwrap().suspend(format!(
                    "계좌 자동 진입 수량({account_delta})이 확인된 체결({rest_qty})보다 커 목표 주문을 보류합니다"
                ));
                self.persist_automation();
                self.emit_automation_state();
                return;
            }
        }

        fills.sort_by_key(|fill| fill.filled_at);
        for fill in &fills {
            let event = crate::types::FillEvent {
                fill_id: fill.fill_id.clone(),
                order_no: fill.order_no.clone(),
                original_order_no: fill.original_order_no.clone(),
                org_no: fill.org_no.clone(),
                code: fill.code.clone(),
                side: fill.side,
                qty: fill.qty,
                price: fill.price,
                filled_at: fill.filled_at,
                status: "filled".into(),
            };
            self.apply_reconciled_fill(&event);
        }
        let filled_qty: u64 = fills.iter().map(|fill| fill.qty).sum();
        if filled_qty == 0 {
            let _ = self.ledger.update_order_status(
                &intent_id,
                if terminal_rejected {
                    LedgerOrderStatus::Rejected
                } else {
                    LedgerOrderStatus::Cancelled
                },
                Some("정확 주문상태로 IOC 무체결 종결 확인"),
                now_kst_fake_epoch(),
            );
            self.automation
                .lock()
                .unwrap()
                .entry_failed("자동 IOC 진입 무체결");
            self.persist_automation();
            self.emit_automation_state();
            return;
        }
        let entry_value: f64 = fills.iter().map(|fill| fill.price * fill.qty as f64).sum();
        let avg_price = entry_value / filled_qty as f64;
        let first_fill_at = fills
            .iter()
            .map(|fill| fill.filled_at)
            .min()
            .unwrap_or(now_kst_fake_epoch());
        let _ = self.ledger.update_order_status(
            &intent_id,
            if filled_qty < qty {
                LedgerOrderStatus::PartiallyFilled
            } else {
                LedgerOrderStatus::Filled
            },
            Some("IOC 종결"),
            now_kst_fake_epoch(),
        );

        let target_price = sell_target_price(avg_price, triggered.scenario.target_return_pct, true);
        let staged_position = OwnedPosition {
            product,
            code: code.clone(),
            entry_qty: filled_qty,
            qty: filled_qty,
            avg_price,
            last_price: avg_price,
            target_return_pct: triggered.scenario.target_return_pct,
            target_price,
            first_fill_at,
            exit_deadline: first_fill_at + AUTO_MAX_HOLD_SECS,
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
            trade_id: trade_id.clone(),
            shadow: false,
            profit_guard_armed: false,
        };
        if !self
            .automation
            .lock()
            .unwrap()
            .stage_entry_position(staged_position)
        {
            self.automation
                .lock()
                .unwrap()
                .suspend("IOC 체결 포지션을 복구 상태에 올리지 못했습니다");
            self.persist_automation();
            self.emit_automation_state();
            return;
        }
        if let Err(error) = self.persist_automation_required() {
            let should_exit = self.protect_staged_auto_position(format!(
                "목표 주문 전 체결 포지션 저장 실패로 즉시 청산합니다: {error}"
            ));
            drop(_actor);
            if should_exit {
                self.execute_auto_exit("target_order_failed").await;
            }
            return;
        }
        let _ = self.ledger.upsert_trade(&NewTrade {
            trade_id: trade_id.clone(),
            session_id: session_id.clone(),
            execution_kind: LedgerExecutionKind::Real,
            origin: LedgerOrigin::Auto,
            code: code.clone(),
            entry_qty: filled_qty,
            exit_qty: 0,
            entry_avg_price: avg_price,
            exit_avg_price: None,
            pnl_rate: None,
            entered_at: first_fill_at,
            exited_at: None,
            status: LedgerTradeStatus::Open,
            exit_reason: None,
            updated_at: now_kst_fake_epoch(),
        });

        // IOC가 15:15 경계 직전에 제출되어 체결·종결 조회 중 경계를 넘을 수 있다.
        // 이 경우 목표 주문을 새로 올리지 않고 확정 체결분을 즉시 마감 청산한다.
        if schedule::has_reached_position_flatten(first_fill_at, self.automation_now()) {
            {
                let mut runtime = self.automation.lock().unwrap();
                runtime.clear_pending_entry();
                runtime.begin_exit("market_close");
            }
            self.persist_automation();
            self.emit_automation_state();
            drop(_actor);
            self.execute_auto_exit("market_close").await;
            return;
        }

        let target_intent_id = unique_id("auto-target");
        let target_requested_at = now_kst_fake_epoch();
        if let Err(error) = self.ledger.record_order_intent(&NewOrderIntent {
            intent_id: target_intent_id.clone(),
            session_id: session_id.clone(),
            trade_id: Some(trade_id.clone()),
            decision_id,
            scenario_product: Some(ledger_product(product)),
            execution_kind: LedgerExecutionKind::Real,
            origin: LedgerOrigin::Auto,
            code: code.clone(),
            side: LedgerSide::Sell,
            order_type: LedgerOrderType::Limit,
            qty: filled_qty,
            price: Some(target_price),
            requested_at: target_requested_at,
        }) {
            self.automation.lock().unwrap().clear_pending_entry();
            self.automation
                .lock()
                .unwrap()
                .begin_exit("target_order_failed");
            self.persist_automation();
            self.emit_automation_state();
            drop(_actor);
            self.execute_auto_exit("target_order_failed").await;
            tracing::error!("목표 주문 의도 기록 실패: {error}");
            return;
        }
        if !self
            .automation
            .lock()
            .unwrap()
            .set_pending_target_intent(target_intent_id.clone())
        {
            let _ = self.ledger.update_order_status(
                &target_intent_id,
                LedgerOrderStatus::Cancelled,
                Some("Auto 포지션 결합 전 실패로 미제출 목표 intent 종결"),
                now_kst_fake_epoch(),
            );
            let should_exit = self.protect_staged_auto_position(
                "목표 주문 intent를 Auto 포지션에 결합하지 못해 즉시 청산합니다",
            );
            drop(_actor);
            if should_exit {
                self.execute_auto_exit("target_order_failed").await;
            }
            return;
        }
        if let Err(error) = self.persist_automation_required() {
            let should_exit = self.protect_staged_auto_position(format!(
                "목표 주문 전 intent 상태 저장 실패로 안전청산합니다: {error}"
            ));
            drop(_actor);
            if should_exit {
                self.execute_auto_exit("target_order_failed").await;
            }
            return;
        }
        match self
            .ledger
            .begin_order_dispatch(&target_intent_id, now_kst_fake_epoch())
        {
            Ok(true) => {}
            Ok(false) => {
                let should_exit = self.protect_staged_auto_position(
                    "목표 주문이 이미 제출 중이어서 소유 주문 조정 뒤 안전청산합니다",
                );
                drop(_actor);
                if should_exit {
                    self.execute_auto_exit("target_order_failed").await;
                }
                return;
            }
            Err(error) => {
                let should_exit = self.protect_staged_auto_position(format!(
                    "목표 주문 제출 상태 기록 실패로 소유 주문 조정 뒤 안전청산합니다: {error}"
                ));
                drop(_actor);
                if should_exit {
                    self.execute_auto_exit("target_order_failed").await;
                }
                return;
            }
        }
        let target_ack = self
            .broker
            .place_sell_limit(&code, filled_qty, target_price)
            .await;
        let (target_order_no, target_org_no, target_unresolved) = match target_ack {
            Ok(ack) => {
                let _ = self.ledger.record_order_ack(
                    &target_intent_id,
                    &OrderAcknowledgement {
                        broker_order_id: Some(ack.order_no.clone()),
                        broker_org_no: Some(ack.org_no.clone()),
                        original_order_id: None,
                        status: LedgerOrderStatus::Submitted,
                        message: Some(ack.message),
                        acknowledged_at: now_kst_fake_epoch(),
                    },
                );
                (Some(ack.order_no), Some(ack.org_no), None)
            }
            Err(error) => {
                if error.is_confirmed_order_rejection() {
                    let _ = self.ledger.record_order_ack(
                        &target_intent_id,
                        &OrderAcknowledgement {
                            broker_order_id: None,
                            broker_org_no: None,
                            original_order_id: None,
                            status: LedgerOrderStatus::Rejected,
                            message: Some(error.to_string()),
                            acknowledged_at: now_kst_fake_epoch(),
                        },
                    );
                    {
                        let mut runtime = self.automation.lock().unwrap();
                        runtime.clear_target_order();
                        runtime.clear_pending_entry();
                        runtime.begin_exit("target_order_failed");
                        runtime.exit_waiting(format!(
                            "목표 주문이 확정 거부되어 즉시 시장가 청산합니다: {error}"
                        ));
                    }
                    self.persist_automation();
                    self.emit_automation_state();
                    drop(_actor);
                    self.execute_auto_exit("target_order_failed").await;
                    return;
                }
                let _ = self.ledger.record_order_ack(
                    &target_intent_id,
                    &OrderAcknowledgement {
                        broker_order_id: None,
                        broker_org_no: None,
                        original_order_id: None,
                        status: LedgerOrderStatus::Unknown,
                        message: Some(error.to_string()),
                        acknowledged_at: now_kst_fake_epoch(),
                    },
                );
                let recovered = match self.broker.open_orders().await {
                    Ok(orders) => {
                        let mut matches = orders
                            .into_iter()
                            .filter(|order| {
                                order.code == code
                                    && order.side == Side::Sell
                                    && order.cancelable_qty > 0
                                    && order.ordered_qty == filled_qty
                                    && order.price.round() as u64 == target_price
                                    && order.ordered_at >= target_requested_at
                            })
                            .collect::<Vec<_>>();
                        if matches.len() == 1 {
                            matches.pop()
                        } else {
                            if matches.len() > 1 {
                                tracing::error!(
                                    "목표 주문 응답 불명확 후 일치 후보가 둘 이상입니다"
                                );
                            }
                            None
                        }
                    }
                    Err(query_error) => {
                        tracing::error!(
                            "목표 주문 응답 불명확 후 미체결 조회 실패: {error}; {query_error}"
                        );
                        None
                    }
                };
                if let Some(order) = recovered {
                    let _ = self.ledger.record_order_ack(
                        &target_intent_id,
                        &OrderAcknowledgement {
                            broker_order_id: Some(order.order_no.clone()),
                            broker_org_no: Some(order.org_no.clone()),
                            original_order_id: None,
                            status: LedgerOrderStatus::Submitted,
                            message: Some("응답 불명확 후 미체결 조회로 목표 주문 확인".into()),
                            acknowledged_at: now_kst_fake_epoch(),
                        },
                    );
                    (Some(order.order_no), Some(order.org_no), None)
                } else {
                    (
                        None,
                        None,
                        Some(format!(
                            "목표 주문 응답이 불명확해 주문·체결 조정 전까지 자동 청산을 보류합니다: {error}"
                        )),
                    )
                }
            }
        };
        if let (Some(order_no), Some(org_no)) = (target_order_no, target_org_no) {
            // 체결 통보가 ACK보다 먼저 와 전량 청산했다면 이미 정상 종결된 상태다.
            if self.automation.lock().unwrap().position().is_none() {
                let _ = self.ledger.update_order_status(
                    &target_intent_id,
                    LedgerOrderStatus::Filled,
                    Some("ACK 전 목표 전량 체결"),
                    now_kst_fake_epoch(),
                );
                return;
            }
            let bound = self
                .automation
                .lock()
                .unwrap()
                .bind_target_order(order_no.clone(), org_no.clone());
            let completed = bound && self.automation.lock().unwrap().complete_pending_entry();
            if !completed {
                let should_exit = self.protect_staged_auto_position(
                    "목표 주문 소유권을 확정하지 못해 주문 조정 뒤 안전청산합니다",
                );
                drop(_actor);
                if should_exit {
                    self.execute_auto_exit("target_order_failed").await;
                }
                return;
            }
            let remaining_qty = self
                .automation
                .lock()
                .unwrap()
                .position()
                .map(|position| position.qty)
                .unwrap_or(0);
            self.reservations.write().unwrap().insert(
                code.clone(),
                Reservation {
                    target_pct: triggered.scenario.target_return_pct,
                    target_price,
                    qty: remaining_qty,
                    order_no,
                    org_no,
                },
            );
        }
        let position = self.automation.lock().unwrap().position().cloned();
        self.stop_loss_lock.lock().unwrap().remove(&code);
        let target_missing = self.reservations.read().unwrap().get(&code).is_none();
        self.mark_account_stale();
        self.schedule_account_refresh();
        self.persist_automation();
        self.emit_automation_state();
        tracing::info!(
            "Auto 진입 완료: {code} {filled_qty}주, VWAP {avg_price:.2}, 목표 {target_price}, local={local_order_id}"
        );
        if let Some(message) = target_unresolved {
            {
                let mut runtime = self.automation.lock().unwrap();
                runtime.clear_pending_entry();
                runtime.begin_exit("target_order_failed");
                runtime.exit_waiting(message);
            }
            self.persist_automation();
            self.emit_automation_state();
            drop(_actor);
            self.execute_auto_exit("target_order_failed").await;
        } else if target_missing
            || position
                .as_ref()
                .is_none_or(|position| position.target_order_no.is_none())
        {
            self.automation
                .lock()
                .unwrap()
                .begin_exit("target_order_failed");
            drop(_actor);
            self.execute_auto_exit("target_order_failed").await;
        }
    }

    fn handle_automation_fill(&self, fill: &crate::types::FillEvent) {
        if fill.side != Side::Sell {
            return;
        }
        let (closed, reason, target_intent_id) = {
            let mut runtime = self.automation.lock().unwrap();
            let phase = runtime.phase();
            let exit_deadline = runtime.position().map(|position| position.exit_deadline);
            let first_fill_at = runtime.position().map(|position| position.first_fill_at);
            let target_fill = runtime.position().is_some_and(|position| {
                position.target_order_no.as_deref() == Some(fill.order_no.as_str())
            });
            let target_intent_id = runtime.position().and_then(|position| {
                (matches!(
                    phase,
                    crate::types::AutomationPhase::Holding
                        | crate::types::AutomationPhase::Handoff
                        | crate::types::AutomationPhase::Reconciling
                        | crate::types::AutomationPhase::EntryPending
                ) && position.target_order_no.as_deref() == Some(fill.order_no.as_str()))
                .then(|| position.target_intent_id.clone())
                .flatten()
            });
            let reason = if first_fill_at.is_some_and(|entered_at| {
                schedule::has_reached_position_flatten(entered_at, fill.filled_at)
            }) {
                "market_close".to_string()
            } else if runtime.exit_reason() == Some("stop_loss") {
                "stop_loss".to_string()
            } else if runtime.exit_reason() == Some("profit_guard") {
                "profit_guard".to_string()
            } else if exit_deadline.is_some_and(|deadline| fill.filled_at >= deadline) {
                "max_holding".to_string()
            } else if target_fill
                && matches!(
                    phase,
                    crate::types::AutomationPhase::Holding
                        | crate::types::AutomationPhase::Handoff
                        | crate::types::AutomationPhase::Reconciling
                        | crate::types::AutomationPhase::EntryPending
                )
            {
                "target".to_string()
            } else {
                runtime
                    .exit_reason()
                    .unwrap_or("manual_reconcile")
                    .to_string()
            };
            let closed =
                runtime.apply_real_exit_fill(&fill.code, &fill.order_no, fill.qty, fill.price);
            (closed, reason, target_intent_id)
        };
        if let Some(intent_id) = target_intent_id {
            let _ = self.ledger.update_order_status(
                &intent_id,
                if closed {
                    LedgerOrderStatus::Filled
                } else {
                    LedgerOrderStatus::PartiallyFilled
                },
                Some("목표 지정가 체결"),
                fill.filled_at,
            );
        }
        if closed {
            self.complete_real_exit(&reason, fill.filled_at);
        } else {
            self.persist_automation();
            self.emit_automation_state();
        }
    }

    fn complete_real_exit(&self, reason: &str, exited_at: i64) {
        let (position, final_reason) = {
            let mut runtime = self.automation.lock().unwrap();
            let complete = runtime.position().is_some_and(|position| {
                position.qty == 0 && position.exit_qty >= position.entry_qty
            });
            if !complete {
                runtime.exit_waiting(
                    "계좌 청산 뒤 실제 체결수량·가격 장부가 완성될 때까지 조정합니다",
                );
                drop(runtime);
                self.persist_automation();
                self.emit_automation_state();
                return;
            }
            let final_reason = runtime.exit_reason().unwrap_or(reason).to_string();
            let position = runtime.complete_exit();
            runtime.set_next_decision_at(self.next_automation_slot(exited_at.saturating_add(1)));
            (position, final_reason)
        };
        let Some(position) = position else { return };
        let exit_avg = if position.exit_qty > 0 {
            position.exit_value / position.exit_qty as f64
        } else {
            return;
        };
        let pnl_rate = if position.avg_price > 0.0 {
            (exit_avg / position.avg_price - 1.0) * 100.0
        } else {
            0.0
        };
        let _ = self.ledger.upsert_trade(&NewTrade {
            trade_id: position.trade_id,
            session_id: self
                .automation
                .lock()
                .unwrap()
                .session_id()
                .map(str::to_owned),
            execution_kind: LedgerExecutionKind::Real,
            origin: LedgerOrigin::Auto,
            code: position.code.clone(),
            entry_qty: position.entry_qty,
            exit_qty: position.exit_qty,
            entry_avg_price: position.avg_price,
            exit_avg_price: Some(exit_avg),
            pnl_rate: Some(pnl_rate),
            entered_at: position.first_fill_at,
            exited_at: Some(exited_at),
            status: LedgerTradeStatus::Closed,
            exit_reason: Some(final_reason),
            updated_at: exited_at,
        });
        if let Some(reservation) = self.reservations.write().unwrap().remove(&position.code) {
            self.emit(
                "reservation",
                &reservation_info(&position.code, &reservation, "filled", None),
            );
        }
        self.persist_automation();
        self.emit_automation_state();
        self.emit("trade-recorded", &serde_json::json!({ "kind": "real" }));
    }

    fn apply_broker_sell_fills(&self, fills: Vec<crate::broker::BrokerFill>) {
        for fill in fills {
            let event = crate::types::FillEvent {
                fill_id: fill.fill_id,
                order_no: fill.order_no,
                original_order_no: fill.original_order_no,
                org_no: fill.org_no,
                code: fill.code,
                side: fill.side,
                qty: fill.qty,
                price: fill.price,
                filled_at: fill.filled_at,
                status: "filled".into(),
            };
            self.apply_reconciled_fill(&event);
        }
    }

    /// 결합된 목표 주문의 정확한 누적 상태를 반영한다. `Ok(false)`는 아직 살아 있는
    /// 주문이므로 ID를 유지한 채 취소 경로로 보내고, `Ok(true)`일 때만 종결된 목표
    /// identity를 지운다.
    async fn reconcile_bound_target_status_for_exit(
        &self,
        snapshot: &OwnedPosition,
        intent: &OrderRecord,
        order_no: &str,
        org_no: &str,
    ) -> Result<bool, String> {
        let status = self
            .broker
            .order_status(&intent.trading_date, order_no)
            .await
            .map_err(|error| format!("목표 주문 정확상태 조회 실패: {error}"))?
            .ok_or_else(|| "목표 주문의 정확한 일별 상태가 아직 조회되지 않습니다".to_string())?;
        if status.order_no != order_no
            || status.trading_date != intent.trading_date
            || status.org_no != org_no
            || status.code != snapshot.code
            || status.side != Side::Sell
            || status.ordered_qty != intent.qty
            || status.ordered_at < intent.requested_at
            || intent
                .price
                .is_some_and(|price| status.filled_qty > 0 && status.avg_fill_price < price as f64)
        {
            return Err("목표 주문 정확상태가 저장된 intent identity와 일치하지 않습니다".into());
        }

        if status.filled_qty > 0 {
            self.apply_broker_sell_fills(vec![crate::broker::BrokerFill {
                fill_id: format!(
                    "exact-target:{}:{}:{}:{}",
                    status.trading_date, status.order_no, status.filled_qty, status.avg_fill_price
                ),
                order_no: status.order_no.clone(),
                original_order_no: status.original_order_no.clone(),
                org_no: status.org_no.clone(),
                code: status.code.clone(),
                side: status.side,
                qty: status.filled_qty,
                price: status.avg_fill_price,
                filled_at: status.ordered_at,
            }]);
        }
        if self.automation.lock().unwrap().position().is_none() {
            self.ledger
                .update_order_status(
                    &intent.intent_id,
                    LedgerOrderStatus::Filled,
                    Some("정확상태로 목표 전량 체결 확인"),
                    now_kst_fake_epoch(),
                )
                .map_err(|error| error.to_string())?;
            return Ok(true);
        }
        if !status.is_terminal() {
            return Ok(false);
        }
        if !self.refresh_account().await {
            return Err("목표 주문 종결 뒤 계좌 조회에 실패했습니다".into());
        }
        let actual_qty = self.cached_position_qty(&snapshot.code);
        let runtime_qty = self
            .automation
            .lock()
            .unwrap()
            .position()
            .map(|position| position.qty)
            .unwrap_or(0);
        if actual_qty != runtime_qty {
            return Err(format!(
                "목표 종결 조정 수량 불일치: 계좌 {actual_qty}주 / 장부 {runtime_qty}주"
            ));
        }
        let terminal_status = if status.filled_qty >= status.ordered_qty {
            LedgerOrderStatus::Filled
        } else if status.filled_qty > 0 {
            LedgerOrderStatus::PartiallyFilled
        } else if status.rejected_qty >= status.ordered_qty {
            LedgerOrderStatus::Rejected
        } else {
            LedgerOrderStatus::Cancelled
        };
        self.ledger
            .update_order_status(
                &intent.intent_id,
                terminal_status,
                Some("정확상태로 목표 주문 잔량 종결 확인"),
                now_kst_fake_epoch(),
            )
            .map_err(|error| error.to_string())?;
        self.automation.lock().unwrap().clear_target_order();
        if let Err(error) = self.persist_automation_required() {
            let mut runtime = self.automation.lock().unwrap();
            runtime.restore_target_order(
                intent.intent_id.clone(),
                order_no.to_string(),
                org_no.to_string(),
            );
            runtime.exit_waiting(format!(
                "목표 종결 상태 저장 실패로 시장가 청산을 보류합니다: {error}"
            ));
            drop(runtime);
            self.persist_automation();
            return Err(error);
        }
        self.reservations.write().unwrap().remove(&snapshot.code);
        Ok(true)
    }

    /// 목표 지정가 POST 응답에서 주문번호를 받지 못한 경우, 체결·미체결과 정확
    /// 일별상태를 대조해 주문 소유권을 먼저 확정한다. 정확한 종결 전에는 target
    /// identity를 지우거나 시장가 청산을 제출해서는 안 된다.
    async fn reconcile_unknown_target_for_exit(
        &self,
        snapshot: &OwnedPosition,
    ) -> Result<(), String> {
        let intent_id = snapshot
            .target_intent_id
            .as_deref()
            .ok_or_else(|| "불명확 목표 주문의 intent가 없습니다".to_string())?;
        let intent = self
            .ledger
            .get_order(intent_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "불명확 목표 주문 intent를 장부에서 찾지 못했습니다".to_string())?;

        if intent.broker_order_id.is_none()
            && matches!(
                intent.status,
                LedgerOrderStatus::Intent
                    | LedgerOrderStatus::Cancelled
                    | LedgerOrderStatus::Rejected
            )
        {
            if intent.status == LedgerOrderStatus::Intent {
                self.ledger
                    .update_order_status(
                        intent_id,
                        LedgerOrderStatus::Cancelled,
                        Some("POST 미제출 목표 intent 종결"),
                        now_kst_fake_epoch(),
                    )
                    .map_err(|error| error.to_string())?;
            }
            {
                let mut runtime = self.automation.lock().unwrap();
                if !runtime.position().is_some_and(|position| {
                    position.target_intent_id.as_deref() == Some(intent_id)
                        && position.target_order_no.is_none()
                }) {
                    return Err("미제출 목표 intent와 현재 Auto 포지션이 일치하지 않습니다".into());
                }
                runtime.clear_target_order();
            }
            if let Err(error) = self.persist_automation_required() {
                self.automation
                    .lock()
                    .unwrap()
                    .restore_target_intent(intent.intent_id.clone());
                self.persist_automation();
                return Err(format!(
                    "미제출 목표 intent 종결 상태 저장 실패로 청산을 보류합니다: {error}"
                ));
            }
            self.reservations.write().unwrap().remove(&snapshot.code);
            return Ok(());
        }

        if let Some(order_no) = intent.broker_order_id.clone() {
            let org_no = intent
                .broker_org_no
                .clone()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "장부의 목표 주문 조직번호가 비어 있습니다".to_string())?;
            if !self
                .automation
                .lock()
                .unwrap()
                .bind_target_order(order_no.clone(), org_no.clone())
            {
                return Err("장부 목표 주문을 현재 Auto 포지션에 결합하지 못했습니다".into());
            }
            self.persist_automation_required()?;
            self.reconcile_bound_target_status_for_exit(snapshot, &intent, &order_no, &org_no)
                .await?;
            return Ok(());
        }

        let fills = self
            .wait_recent_fills(&snapshot.code, Side::Sell, intent.requested_at)
            .await?
            .into_iter()
            .filter(|fill| {
                fill.qty <= snapshot.entry_qty && fill.price.round() as u64 >= snapshot.target_price
            })
            .collect::<Vec<_>>();
        let open_orders = self
            .broker
            .open_orders()
            .await
            .map_err(|error| format!("불명확 목표 주문 미체결 조회 실패: {error}"))?;
        let matching_open = open_orders
            .into_iter()
            .filter(|order| {
                order.code == snapshot.code
                    && order.side == Side::Sell
                    && order.ordered_at >= intent.requested_at
                    && order.ordered_qty == snapshot.entry_qty
                    && order.price.round() as u64 == snapshot.target_price
            })
            .collect::<Vec<_>>();

        let mut candidates = HashSet::<(String, String)>::new();
        candidates.extend(
            fills
                .iter()
                .filter(|fill| !fill.org_no.trim().is_empty())
                .map(|fill| (fill.order_no.clone(), fill.org_no.clone())),
        );
        candidates.extend(
            matching_open
                .iter()
                .filter(|order| !order.org_no.trim().is_empty())
                .map(|order| (order.order_no.clone(), order.org_no.clone())),
        );
        if candidates.len() > 1 {
            return Err(
                "불명확 목표 주문 후보가 둘 이상이라 자동으로 소유권을 정할 수 없습니다".into(),
            );
        }

        if let Some((order_no, org_no)) = candidates.into_iter().next() {
            if !self
                .automation
                .lock()
                .unwrap()
                .bind_target_order(order_no.clone(), org_no.clone())
            {
                return Err("불명확 목표 주문을 현재 Auto 포지션에 결합하지 못했습니다".into());
            }
            if let Err(error) = self.ledger.record_order_ack(
                intent_id,
                &OrderAcknowledgement {
                    broker_order_id: Some(order_no.clone()),
                    broker_org_no: Some(org_no.clone()),
                    original_order_id: None,
                    status: LedgerOrderStatus::Submitted,
                    message: Some("체결·미체결 조회로 불명확 목표 주문 확인".into()),
                    acknowledged_at: now_kst_fake_epoch(),
                },
            ) {
                let mut runtime = self.automation.lock().unwrap();
                runtime.clear_target_order();
                runtime.restore_target_intent(intent.intent_id.clone());
                drop(runtime);
                self.persist_automation();
                return Err(error.to_string());
            }
            self.persist_automation_required()?;
            self.reconcile_bound_target_status_for_exit(snapshot, &intent, &order_no, &org_no)
                .await?;
            return Ok(());
        }

        // 주문번호 없는 POST 결과는 빈 체결·미체결·불변 계좌만으로 부재를 증명할
        // 수 없다. 목표가 실제로 살아 있을 수 있으므로 시장가 청산을 절대 겹치지 않는다.
        Err("불명확 목표 주문번호를 아직 찾지 못해 시장가 청산을 보류합니다".into())
    }

    fn start_target_cancel_reconcile(self: &Arc<Self>, reason: &'static str, order_no: String) {
        if self.defer_auto_exit_for_market_day() {
            return;
        }
        if self.exit_reconcile_pending.swap(true, Ordering::SeqCst) {
            return;
        }
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            engine.reconcile_target_cancel(reason, order_no).await;
        });
    }

    fn start_active_exit_reconcile(self: &Arc<Self>, reason: &'static str, order_no: String) {
        if self.defer_auto_exit_for_market_day() {
            return;
        }
        if self.exit_reconcile_pending.swap(true, Ordering::SeqCst) {
            return;
        }
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            engine.reconcile_active_exit(reason, order_no).await;
        });
    }

    fn start_delayed_exit(self: &Arc<Self>, reason: &'static str) {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            engine.execute_auto_exit(reason).await;
        });
    }

    async fn execute_auto_exit(self: Arc<Self>, reason: &'static str) {
        if self.defer_auto_exit_for_market_day() {
            return;
        }
        let _actor = self.order_actor.lock().await;
        if self.defer_auto_exit_for_market_day() {
            return;
        }
        let position = {
            let runtime = self.automation.lock().unwrap();
            if runtime.mode() != ControlMode::Auto
                || runtime.phase() != crate::types::AutomationPhase::ExitPending
            {
                return;
            }
            runtime.position().cloned()
        };
        let Some(mut position) = position else { return };

        if position.target_intent_id.is_some() && position.target_order_no.is_none() {
            if let Err(error) = self.reconcile_unknown_target_for_exit(&position).await {
                self.automation.lock().unwrap().exit_waiting(format!(
                    "불명확 목표 주문을 조정할 때까지 시장가 청산을 보류합니다: {error}"
                ));
                self.persist_automation();
                self.emit_automation_state();
                drop(_actor);
                self.start_delayed_exit(reason);
                return;
            }
            let Some(resolved) = self.automation.lock().unwrap().position().cloned() else {
                return;
            };
            position = resolved;
        }

        // 청산 intent가 이미 Auto 포지션에 결합됐다면 새 intent를 만들지 않는다.
        // 주문번호가 없는 상태는 POST 전 실패일 수도, 응답 유실일 수도 있으므로
        // 장부 상태를 먼저 조정해 안전하게 제출 여부를 판정한다.
        if let Some(intent_id) = position.exit_intent_id.clone() {
            if position.exit_order_no.is_none() {
                let requested_at = position
                    .exit_requested_at
                    .unwrap_or_else(now_kst_fake_epoch);
                drop(_actor);
                self.start_unknown_exit_reconcile(reason, intent_id, requested_at);
                return;
            }
        }

        // 이미 제출한 시장가 주문이 있으면 해당 주문 종결만 조회한다. 절대 재POST하지 않는다.
        if let Some(order_no) = position.exit_order_no.clone() {
            drop(_actor);
            self.start_active_exit_reconcile(reason, order_no);
            return;
        }

        if let (Some(order_no), Some(org_no)) = (
            position.target_order_no.as_deref(),
            position.target_org_no.as_deref(),
        ) {
            if self.defer_auto_exit_for_market_day() {
                return;
            }
            let cancel_intent = match self
                .record_auto_cancel_intent(&position, "자동 목표 취소 의도 기록 실패")
            {
                Ok(intent) => intent,
                Err(error) => {
                    self.automation.lock().unwrap().exit_waiting(error);
                    self.persist_automation();
                    self.emit_automation_state();
                    drop(_actor);
                    self.start_delayed_exit(reason);
                    return;
                }
            };
            if let Some(intent_id) = &position.target_intent_id {
                let _ = self.ledger.update_order_status(
                    intent_id,
                    LedgerOrderStatus::CancelPending,
                    Some(reason),
                    now_kst_fake_epoch(),
                );
            }
            match self
                .broker
                .cancel_order(&position.code, order_no, org_no)
                .await
            {
                Ok(ack) => {
                    // 취소 요청 자체의 ACK는 끝났고, 원 목표 주문의 경제 종결은
                    // 아래 exact order_status 조정으로 별도로 확인한다.
                    self.record_broker_ack(&cancel_intent, &ack, LedgerOrderStatus::Filled)
                }
                Err(error) => self.record_broker_error(
                    &cancel_intent,
                    &error.to_string(),
                    !error.is_confirmed_order_rejection(),
                ),
            }
            self.automation
                .lock()
                .unwrap()
                .exit_waiting("목표 주문 취소의 정확한 일별 종결 상태를 확인 중입니다");
            self.persist_automation();
            self.emit_automation_state();
            let target_order_no = order_no.to_string();
            drop(_actor);
            self.start_target_cancel_reconcile(reason, target_order_no);
            return;
        }

        if !self.refresh_account().await {
            self.automation
                .lock()
                .unwrap()
                .exit_waiting("시장가 청산 전 계좌 조회 실패");
            self.persist_automation();
            self.emit_automation_state();
            drop(_actor);
            self.start_delayed_exit(reason);
            return;
        }
        let qty = self.cached_position_qty(&position.code);
        let runtime_qty = self
            .automation
            .lock()
            .unwrap()
            .position()
            .map(|item| item.qty)
            .unwrap_or(0);
        if qty != runtime_qty {
            self.automation.lock().unwrap().exit_waiting(format!(
                "시장가 청산 전 수량 조정 중: 계좌 {qty}주 / 장부 {runtime_qty}주"
            ));
            self.persist_automation();
            self.emit_automation_state();
            drop(_actor);
            self.start_delayed_exit(reason);
            return;
        }
        if qty == 0 {
            self.complete_real_exit(reason, now_kst_fake_epoch());
            return;
        }
        if self.defer_auto_exit_for_market_day() {
            return;
        }

        let intent_id = unique_id("auto-exit");
        let (session_id, decision_id) = {
            let runtime = self.automation.lock().unwrap();
            (
                runtime.session_id().map(str::to_owned),
                runtime.decision_key().map(str::to_owned),
            )
        };
        let requested_at = now_kst_fake_epoch();
        if let Err(error) = self.ledger.record_order_intent(&NewOrderIntent {
            intent_id: intent_id.clone(),
            session_id,
            trade_id: Some(position.trade_id.clone()),
            decision_id,
            scenario_product: Some(ledger_product(position.product)),
            execution_kind: LedgerExecutionKind::Real,
            origin: LedgerOrigin::Auto,
            code: position.code.clone(),
            side: LedgerSide::Sell,
            order_type: LedgerOrderType::Market,
            qty,
            price: None,
            requested_at,
        }) {
            self.automation
                .lock()
                .unwrap()
                .exit_waiting(format!("청산 의도 장부 기록 실패: {error}"));
            self.persist_automation();
            self.emit_automation_state();
            drop(_actor);
            self.start_delayed_exit(reason);
            return;
        }
        if !self.automation.lock().unwrap().set_active_exit_order(
            intent_id.clone(),
            None,
            None,
            requested_at,
        ) {
            self.automation
                .lock()
                .unwrap()
                .exit_waiting("청산 intent를 Auto 포지션에 결합하지 못했습니다");
            let _ = self.ledger.update_order_status(
                &intent_id,
                LedgerOrderStatus::Cancelled,
                Some("Auto 포지션 결합 전 실패로 미제출 intent 종결"),
                now_kst_fake_epoch(),
            );
            self.persist_automation();
            self.emit_automation_state();
            drop(_actor);
            self.start_delayed_exit(reason);
            return;
        }
        if let Err(error) = self.persist_automation_required() {
            self.automation
                .lock()
                .unwrap()
                .exit_waiting(format!("청산 주문 전 상태 저장 실패: {error}"));
            self.emit_automation_state();
            drop(_actor);
            self.start_unknown_exit_reconcile(reason, intent_id, requested_at);
            return;
        }
        match self
            .ledger
            .begin_order_dispatch(&intent_id, now_kst_fake_epoch())
        {
            Ok(true) => {}
            Ok(false) => {
                self.automation
                    .lock()
                    .unwrap()
                    .exit_waiting("청산 주문이 이미 제출 중이어서 중복 POST를 차단했습니다");
                self.persist_automation();
                self.emit_automation_state();
                drop(_actor);
                self.start_unknown_exit_reconcile(reason, intent_id, requested_at);
                return;
            }
            Err(error) => {
                self.automation
                    .lock()
                    .unwrap()
                    .exit_waiting(format!("청산 주문 제출 상태 기록 실패: {error}"));
                self.persist_automation();
                self.emit_automation_state();
                drop(_actor);
                self.start_unknown_exit_reconcile(reason, intent_id, requested_at);
                return;
            }
        }
        let ack = self.broker.place_sell_market(&position.code, qty).await;
        let ack = match ack {
            Ok(ack) => ack,
            Err(error) => {
                let confirmed_rejection = error.is_confirmed_order_rejection();
                let _ = self.ledger.record_order_ack(
                    &intent_id,
                    &OrderAcknowledgement {
                        broker_order_id: None,
                        broker_org_no: None,
                        original_order_id: None,
                        status: if confirmed_rejection {
                            LedgerOrderStatus::Rejected
                        } else {
                            LedgerOrderStatus::Unknown
                        },
                        message: Some(error.to_string()),
                        acknowledged_at: now_kst_fake_epoch(),
                    },
                );
                self.automation
                    .lock()
                    .unwrap()
                    .exit_waiting(if confirmed_rejection {
                        format!("청산 주문이 확정 거부되어 안전하게 재시도합니다: {error}")
                    } else {
                        format!("청산 응답 불명확, 체결·미체결 조정 중: {error}")
                    });
                self.emit_automation_state();
                drop(_actor);
                self.start_unknown_exit_reconcile(reason, intent_id, requested_at);
                return;
            }
        };
        let _ = self.ledger.record_order_ack(
            &intent_id,
            &OrderAcknowledgement {
                broker_order_id: Some(ack.order_no.clone()),
                broker_org_no: Some(ack.org_no.clone()),
                original_order_id: None,
                status: LedgerOrderStatus::Submitted,
                message: Some(ack.message.clone()),
                acknowledged_at: now_kst_fake_epoch(),
            },
        );
        self.automation.lock().unwrap().set_active_exit_order(
            intent_id.clone(),
            Some(ack.order_no.clone()),
            Some(ack.org_no.clone()),
            requested_at,
        );
        self.automation
            .lock()
            .unwrap()
            .exit_waiting("청산 주문의 정확한 일별 종결 상태를 확인 중이며 재주문하지 않습니다");
        self.persist_automation();
        self.emit_automation_state();
        let order_no = ack.order_no;
        drop(_actor);
        self.start_active_exit_reconcile(reason, order_no);
    }

    fn start_unknown_exit_reconcile(
        self: &Arc<Self>,
        reason: &'static str,
        intent_id: String,
        requested_at: i64,
    ) {
        if self.defer_auto_exit_for_market_day() {
            return;
        }
        if self.exit_reconcile_pending.swap(true, Ordering::SeqCst) {
            return;
        }
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            engine
                .reconcile_unknown_exit(reason, intent_id, requested_at)
                .await;
        });
    }

    async fn reconcile_target_cancel(self: Arc<Self>, reason: &'static str, order_no: String) {
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            if self.defer_auto_exit_for_market_day() {
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                return;
            }
            let Some(position) = self.automation.lock().unwrap().position().cloned() else {
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                return;
            };
            if position.target_order_no.as_deref() != Some(order_no.as_str()) {
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                return;
            }
            let Some(intent_id) = position.target_intent_id.clone() else {
                self.automation
                    .lock()
                    .unwrap()
                    .exit_waiting("목표 주문 취소 조정에 target intent가 없습니다");
                continue;
            };
            let intent = match self.ledger.get_order(&intent_id) {
                Ok(Some(intent))
                    if intent.broker_order_id.as_deref() == Some(order_no.as_str()) =>
                {
                    intent
                }
                _ => {
                    self.automation
                        .lock()
                        .unwrap()
                        .exit_waiting("목표 주문번호와 target intent 장부가 일치하지 않습니다");
                    continue;
                }
            };
            let Some(intent_org_no) = intent
                .broker_org_no
                .as_deref()
                .filter(|org_no| !org_no.trim().is_empty())
            else {
                self.automation
                    .lock()
                    .unwrap()
                    .exit_waiting("목표 주문조직번호가 없어 정확 취소 종결을 확정할 수 없습니다");
                continue;
            };
            let status = match self
                .broker
                .order_status(&intent.trading_date, &order_no)
                .await
            {
                Ok(Some(status)) => status,
                Ok(None) | Err(_) => continue,
            };
            if status.order_no != order_no
                || status.trading_date != intent.trading_date
                || status.org_no != intent_org_no
                || position.target_org_no.as_deref() != Some(intent_org_no)
                || status.code != position.code
                || status.side != Side::Sell
                || status.ordered_qty != intent.qty
                || status.ordered_at < intent.requested_at
                || intent.price.is_some_and(|price| {
                    status.filled_qty > 0 && status.avg_fill_price < price as f64
                })
            {
                self.automation.lock().unwrap().exit_waiting(
                    "정확 목표 주문 조회 결과가 저장된 target intent와 일치하지 않습니다",
                );
                continue;
            }
            if status.filled_qty > 0 {
                self.apply_broker_sell_fills(vec![crate::broker::BrokerFill {
                    fill_id: format!(
                        "exact-target:{}:{}:{}:{}",
                        status.trading_date,
                        status.order_no,
                        status.filled_qty,
                        status.avg_fill_price
                    ),
                    order_no: status.order_no.clone(),
                    original_order_no: status.original_order_no.clone(),
                    org_no: status.org_no.clone(),
                    code: status.code.clone(),
                    side: status.side,
                    qty: status.filled_qty,
                    price: status.avg_fill_price,
                    filled_at: status.ordered_at,
                }]);
            }
            if self.automation.lock().unwrap().position().is_none() {
                let _ = self.ledger.update_order_status(
                    &intent_id,
                    LedgerOrderStatus::Filled,
                    Some(reason),
                    now_kst_fake_epoch(),
                );
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                return;
            }
            if !status.is_terminal() || !self.refresh_account().await {
                continue;
            }
            let actual_qty = self.cached_position_qty(&position.code);
            let runtime_qty = self
                .automation
                .lock()
                .unwrap()
                .position()
                .map(|item| item.qty)
                .unwrap_or(0);
            if actual_qty != runtime_qty {
                continue;
            }
            let terminal_status = if status.filled_qty >= status.ordered_qty {
                LedgerOrderStatus::Filled
            } else if status.filled_qty > 0 {
                LedgerOrderStatus::PartiallyFilled
            } else if status.rejected_qty >= status.ordered_qty {
                LedgerOrderStatus::Rejected
            } else {
                LedgerOrderStatus::Cancelled
            };
            if self
                .ledger
                .update_order_status(
                    &intent_id,
                    terminal_status,
                    Some(reason),
                    now_kst_fake_epoch(),
                )
                .is_err()
            {
                continue;
            }
            let org_no = if status.org_no.trim().is_empty() {
                position.target_org_no.clone().unwrap_or_default()
            } else {
                status.org_no.clone()
            };
            {
                let mut runtime = self.automation.lock().unwrap();
                runtime.clear_target_order();
                runtime.exit_waiting("목표 주문의 정확한 취소 종결을 확인했습니다");
            }
            if let Err(error) = self.persist_automation_required() {
                let mut runtime = self.automation.lock().unwrap();
                runtime.restore_target_order(intent_id, order_no.clone(), org_no);
                runtime.exit_waiting(format!(
                    "목표 취소 종결 상태 저장 실패로 시장가 청산을 보류합니다: {error}"
                ));
                drop(runtime);
                self.persist_automation();
                self.emit_automation_state();
                continue;
            }
            if let Some(reservation) = self.reservations.write().unwrap().remove(&position.code) {
                self.emit(
                    "reservation",
                    &reservation_info(
                        &position.code,
                        &reservation,
                        "cancelled",
                        Some(format!("자동 청산 우선순위 적용: {reason}")),
                    ),
                );
            }
            self.emit_automation_state();
            self.exit_reconcile_pending.store(false, Ordering::SeqCst);
            if actual_qty == 0 {
                self.complete_real_exit(reason, now_kst_fake_epoch());
            } else {
                let engine = Arc::clone(&self);
                tokio::spawn(async move { engine.execute_auto_exit(reason).await });
            }
            return;
        }
        self.automation
            .lock()
            .unwrap()
            .exit_waiting("목표 취소 조정이 60초 넘게 완료되지 않았습니다");
        self.persist_automation();
        self.emit_automation_state();
        self.exit_reconcile_pending.store(false, Ordering::SeqCst);
        let engine = Arc::clone(&self);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            engine.execute_auto_exit(reason).await;
        });
    }

    async fn reconcile_active_exit(self: Arc<Self>, reason: &'static str, order_no: String) {
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            if self.defer_auto_exit_for_market_day() {
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                return;
            }
            let Some(position) = self.automation.lock().unwrap().position().cloned() else {
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                return;
            };
            if position.exit_order_no.as_deref() != Some(order_no.as_str()) {
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                return;
            }
            let Some(intent_id) = position.exit_intent_id.clone() else {
                self.automation
                    .lock()
                    .unwrap()
                    .exit_waiting("활성 청산 주문의 intent 식별자가 없습니다");
                continue;
            };
            let mut intent = match self.ledger.get_order(&intent_id) {
                Ok(Some(intent))
                    if intent
                        .broker_order_id
                        .as_deref()
                        .is_none_or(|known| known == order_no) =>
                {
                    intent
                }
                _ => {
                    self.automation
                        .lock()
                        .unwrap()
                        .exit_waiting("활성 청산 주문번호와 장부 ACK가 일치하지 않습니다");
                    continue;
                }
            };
            if intent.broker_order_id.is_none() {
                let org_no = position.exit_order_org_no.clone();
                if self
                    .ledger
                    .record_order_ack(
                        &intent_id,
                        &OrderAcknowledgement {
                            broker_order_id: Some(order_no.clone()),
                            broker_org_no: org_no.clone(),
                            original_order_id: None,
                            status: LedgerOrderStatus::Submitted,
                            message: Some("런타임에 보존된 청산 ACK를 장부에 복구".into()),
                            acknowledged_at: now_kst_fake_epoch(),
                        },
                    )
                    .is_err()
                {
                    continue;
                }
                intent.broker_order_id = Some(order_no.clone());
                intent.broker_org_no = org_no;
            }
            let status = match self
                .broker
                .order_status(&intent.trading_date, &order_no)
                .await
            {
                Ok(Some(status)) => status,
                Ok(None) | Err(_) => continue,
            };
            if status.order_no != order_no
                || status.trading_date != intent.trading_date
                || status.code != position.code
                || status.side != Side::Sell
                || status.ordered_qty != intent.qty
                || intent
                    .broker_org_no
                    .as_deref()
                    .is_some_and(|org_no| org_no != status.org_no)
            {
                self.automation
                    .lock()
                    .unwrap()
                    .exit_waiting("정확 주문 조회 결과가 저장된 청산 ACK와 일치하지 않습니다");
                continue;
            }

            if status.filled_qty > 0 {
                self.apply_broker_sell_fills(vec![crate::broker::BrokerFill {
                    fill_id: format!(
                        "exact:{}:{}:{}:{}",
                        status.trading_date,
                        status.order_no,
                        status.filled_qty,
                        status.avg_fill_price
                    ),
                    order_no: status.order_no.clone(),
                    original_order_no: status.original_order_no.clone(),
                    org_no: status.org_no.clone(),
                    code: status.code.clone(),
                    side: status.side,
                    qty: status.filled_qty,
                    price: status.avg_fill_price,
                    filled_at: status.ordered_at,
                }]);
            }
            if self.automation.lock().unwrap().position().is_none() {
                let _ = self.ledger.update_order_status(
                    &intent_id,
                    if status.filled_qty >= status.ordered_qty {
                        LedgerOrderStatus::Filled
                    } else {
                        LedgerOrderStatus::PartiallyFilled
                    },
                    Some(reason),
                    now_kst_fake_epoch(),
                );
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                return;
            }
            if !status.is_terminal() || !self.refresh_account().await {
                continue;
            }
            let actual_qty = self.cached_position_qty(&position.code);
            let runtime_qty = self
                .automation
                .lock()
                .unwrap()
                .position()
                .map(|item| item.qty)
                .unwrap_or(0);
            if actual_qty != runtime_qty {
                continue;
            }

            let terminal_status = if status.filled_qty >= status.ordered_qty {
                LedgerOrderStatus::Filled
            } else if status.filled_qty > 0 {
                LedgerOrderStatus::PartiallyFilled
            } else if status.rejected_qty >= status.ordered_qty {
                LedgerOrderStatus::Rejected
            } else {
                LedgerOrderStatus::Cancelled
            };
            if self
                .ledger
                .update_order_status(
                    &intent_id,
                    terminal_status,
                    Some(reason),
                    now_kst_fake_epoch(),
                )
                .is_err()
            {
                continue;
            }
            if actual_qty == 0 {
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                self.complete_real_exit(reason, now_kst_fake_epoch());
                return;
            }

            let requested_at = position.exit_requested_at.unwrap_or(intent.requested_at);
            let org_no = (!status.org_no.trim().is_empty())
                .then(|| status.org_no.clone())
                .or(intent.broker_org_no.clone());
            {
                let mut runtime = self.automation.lock().unwrap();
                runtime.clear_active_exit_order();
                runtime.exit_waiting("청산 주문의 정확한 종결을 확인해 잔량을 이어서 청산합니다");
            }
            if let Err(error) = self.persist_automation_required() {
                let mut runtime = self.automation.lock().unwrap();
                runtime.set_active_exit_order(
                    intent_id,
                    Some(order_no.clone()),
                    org_no,
                    requested_at,
                );
                runtime.exit_waiting(format!(
                    "종결된 청산 주문 상태 저장 실패로 잔량 재주문을 보류합니다: {error}"
                ));
                drop(runtime);
                self.persist_automation();
                self.emit_automation_state();
                continue;
            }
            self.emit_automation_state();
            self.exit_reconcile_pending.store(false, Ordering::SeqCst);
            // 확정 업무거부도 이 경로로 들어온다. 즉시 재POST하면 지속 거부 시
            // 주문 API를 폭주시키므로 공통 5초 백오프 뒤 최신 계좌로 재시도한다.
            self.start_delayed_exit(reason);
            return;
        }
        self.automation
            .lock()
            .unwrap()
            .exit_waiting("활성 청산 주문의 정확한 종결을 60초 넘게 확인하지 못했습니다");
        self.persist_automation();
        self.emit_automation_state();
        self.exit_reconcile_pending.store(false, Ordering::SeqCst);
        let engine = Arc::clone(&self);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            engine.start_active_exit_reconcile(reason, order_no);
        });
    }

    async fn reconcile_unknown_exit(
        self: Arc<Self>,
        reason: &'static str,
        intent_id: String,
        requested_at: i64,
    ) {
        let intent = match self.ledger.get_order(&intent_id) {
            Ok(Some(order)) => order,
            _ => {
                self.automation
                    .lock()
                    .unwrap()
                    .exit_waiting("불명확 청산 intent를 장부에서 찾지 못했습니다");
                self.persist_automation();
                self.emit_automation_state();
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                return;
            }
        };

        // 아직 `intent`라면 dispatch CAS가 성공한 적이 없어 POST도 시작되지 않았다.
        // 이 경우에만 미제출 intent를 종결하고 새 청산 시도를 허용한다. 종결 상태를
        // 런타임에 저장하지 못하면 식별자를 되돌려 다음 조정에서도 재POST를 막는다.
        if intent.broker_order_id.is_none()
            && matches!(
                intent.status,
                LedgerOrderStatus::Intent
                    | LedgerOrderStatus::Cancelled
                    | LedgerOrderStatus::Rejected
            )
        {
            if intent.status == LedgerOrderStatus::Intent
                && self
                    .ledger
                    .update_order_status(
                        &intent_id,
                        LedgerOrderStatus::Cancelled,
                        Some("POST 전 실패로 미제출 청산 intent 종결"),
                        now_kst_fake_epoch(),
                    )
                    .is_err()
            {
                self.automation
                    .lock()
                    .unwrap()
                    .exit_waiting("미제출 청산 intent 종결 기록에 실패해 재제출을 보류합니다");
                self.persist_automation();
                self.emit_automation_state();
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                let engine = Arc::clone(&self);
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    engine.start_unknown_exit_reconcile(reason, intent_id, requested_at);
                });
                return;
            }

            {
                let mut runtime = self.automation.lock().unwrap();
                runtime.clear_active_exit_order();
                runtime.exit_waiting("미제출 청산 intent를 종결해 다시 청산을 시도합니다");
            }
            if let Err(error) = self.persist_automation_required() {
                let mut runtime = self.automation.lock().unwrap();
                runtime.set_active_exit_order(intent_id.clone(), None, None, requested_at);
                runtime.exit_waiting(format!(
                    "미제출 청산 intent 종결 상태 저장 실패로 재제출을 보류합니다: {error}"
                ));
                drop(runtime);
                self.persist_automation();
                self.emit_automation_state();
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                let engine = Arc::clone(&self);
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    engine.start_unknown_exit_reconcile(reason, intent_id, requested_at);
                });
                return;
            }
            self.emit_automation_state();
            self.exit_reconcile_pending.store(false, Ordering::SeqCst);
            // 업무 거부처럼 POST가 확정 종결된 경우에도 즉시 재POST하면 같은
            // 거부가 지속될 때 주문 API를 폭주시키게 된다. 최신 계좌를 다시
            // 읽는 다음 청산 시도까지 공통 백오프를 둔다.
            self.start_delayed_exit(reason);
            return;
        }

        let expected_qty = intent.qty;
        // ACK는 장부에 기록됐지만 그 직후 프로세스가 종료되어 runtime bundle에만
        // 주문번호가 빠진 경계다. 이미 아는 주문을 조회 대상으로 복구하고 절대 재POST하지 않는다.
        if let Some(order_no) = intent.broker_order_id.clone() {
            self.automation.lock().unwrap().set_active_exit_order(
                intent_id,
                Some(order_no.clone()),
                intent.broker_org_no,
                requested_at,
            );
            self.persist_automation();
            self.emit_automation_state();
            self.exit_reconcile_pending.store(false, Ordering::SeqCst);
            self.start_active_exit_reconcile(reason, order_no);
            return;
        }
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            if self.defer_auto_exit_for_market_day() {
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                return;
            }
            let Some(position) = self.automation.lock().unwrap().position().cloned() else {
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                return;
            };
            let fills = match self
                .wait_recent_fills(&position.code, Side::Sell, requested_at)
                .await
            {
                Ok(fills) => fills,
                Err(_) => continue,
            };
            let orders = match self.broker.open_orders().await {
                Ok(orders) => orders,
                Err(_) => continue,
            };
            let mut candidates = HashMap::<String, String>::new();
            for order in orders.iter().filter(|order| {
                order.code == position.code
                    && order.side == Side::Sell
                    && order.ordered_at >= requested_at
                    && order.ordered_qty == expected_qty
            }) {
                candidates.insert(order.order_no.clone(), order.org_no.clone());
            }
            for fill in fills
                .iter()
                .filter(|fill| fill.qty <= expected_qty && !fill.org_no.trim().is_empty())
            {
                candidates.insert(fill.order_no.clone(), fill.org_no.clone());
            }
            if candidates.len() > 1 {
                continue;
            }
            if let Some((order_no, org_no)) = candidates.into_iter().next() {
                self.automation.lock().unwrap().set_active_exit_order(
                    intent_id.clone(),
                    Some(order_no.clone()),
                    Some(org_no.clone()),
                    requested_at,
                );
                let _ = self.ledger.record_order_ack(
                    &intent_id,
                    &OrderAcknowledgement {
                        broker_order_id: Some(order_no.clone()),
                        broker_org_no: Some(org_no),
                        original_order_id: None,
                        status: LedgerOrderStatus::Submitted,
                        message: Some("불명확 응답 뒤 조회로 청산 주문 확인".into()),
                        acknowledged_at: now_kst_fake_epoch(),
                    },
                );
                self.persist_automation();
                self.apply_broker_sell_fills(
                    fills
                        .into_iter()
                        .filter(|fill| fill.order_no == order_no)
                        .collect(),
                );
                self.exit_reconcile_pending.store(false, Ordering::SeqCst);
                self.start_active_exit_reconcile(reason, order_no);
                return;
            }
            // 주문번호 없는 POST 결과는 빈 체결·미체결·불변 계좌만으로 주문 부재를
            // 증명할 수 없다. 유일한 주문번호를 찾기 전에는 절대 재POST하지 않는다.
        }
        self.automation
            .lock()
            .unwrap()
            .exit_waiting("불명확한 청산 주문 조정이 60초 넘게 완료되지 않았습니다");
        self.persist_automation();
        self.emit_automation_state();
        self.exit_reconcile_pending.store(false, Ordering::SeqCst);
        let engine = Arc::clone(&self);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            engine.start_unknown_exit_reconcile(reason, intent_id, requested_at);
        });
    }

    /// 장부상 Auto가 소유한 미체결만 취소한다. 외부·수동 주문은 건드리지 않는다.
    async fn cancel_known_auto_open_orders(&self, reason: &str) -> Result<(), String> {
        let orders = self
            .broker
            .open_orders()
            .await
            .map_err(|error| error.to_string())?;
        for order in orders {
            let order_key =
                BrokerOrderKey::from_fake_epoch(order.ordered_at, &order.org_no, &order.order_no)
                    .map_err(|error| error.to_string())?;
            let original_intent = self
                .ledger
                .get_auto_order_by_broker_key(&order_key)
                .map_err(|error| error.to_string())?;
            if order.cancelable_qty == 0 || original_intent.is_none() {
                continue;
            }
            let original_intent = original_intent.unwrap();
            let open_price_matches = match (original_intent.order_type, original_intent.price) {
                (LedgerOrderType::Limit | LedgerOrderType::IocLimit, Some(price)) => {
                    order.price.round() as u64 == price
                }
                (LedgerOrderType::Market, _) => true,
                _ => false,
            };
            if original_intent.code != order.code
                || original_intent.side != ledger_side(order.side)
                || original_intent.qty != order.ordered_qty
                || original_intent.requested_at > order.ordered_at
                || !open_price_matches
            {
                return Err(format!(
                    "{reason}: Auto 미체결 주문이 저장된 intent 형태와 일치하지 않습니다: {}/{}",
                    order_key.trading_date, order.order_no
                ));
            }
            let intent_id = unique_id("auto-orphan-cancel");
            let (session_id, decision_id) = {
                let runtime = self.automation.lock().unwrap();
                (
                    runtime.session_id().map(str::to_owned),
                    runtime.decision_key().map(str::to_owned),
                )
            };
            self.ledger
                .record_order_intent(&NewOrderIntent {
                    intent_id: intent_id.clone(),
                    session_id,
                    trade_id: None,
                    decision_id,
                    scenario_product: None,
                    execution_kind: LedgerExecutionKind::Real,
                    origin: LedgerOrigin::Auto,
                    code: order.code.clone(),
                    side: ledger_side(order.side),
                    order_type: LedgerOrderType::Cancel,
                    qty: order.cancelable_qty,
                    price: Some(order.price.max(0.0).round() as u64),
                    requested_at: now_kst_fake_epoch(),
                })
                .map_err(|error| error.to_string())?;
            match self
                .broker
                .cancel_order(&order.code, &order.order_no, &order.org_no)
                .await
            {
                Ok(ack) => self.record_broker_ack(&intent_id, &ack, LedgerOrderStatus::Filled),
                Err(error) => {
                    self.record_broker_error(
                        &intent_id,
                        &error.to_string(),
                        !error.is_confirmed_order_rejection(),
                    );
                    tracing::warn!("{reason}: {} 취소 응답 오류: {error}", order.order_no);
                }
            }
            let status = self
                .wait_exact_order_terminal(&original_intent)
                .await
                .map_err(|error| {
                    format!(
                        "{reason}: {}/{} 취소의 정확한 종결 조회 실패: {error}",
                        order_key.trading_date, order.order_no
                    )
                })?;
            if status.filled_qty > 0 {
                self.apply_reconciled_fill(&crate::types::FillEvent {
                    fill_id: format!(
                        "exact-auto-cleanup:{}:{}:{}:{}",
                        status.trading_date,
                        status.order_no,
                        status.filled_qty,
                        status.avg_fill_price
                    ),
                    order_no: status.order_no.clone(),
                    original_order_no: status.original_order_no.clone(),
                    org_no: status.org_no.clone(),
                    code: status.code.clone(),
                    side: status.side,
                    qty: status.filled_qty,
                    price: status.avg_fill_price,
                    filled_at: status.ordered_at,
                    status: "reconciled".into(),
                });
            }
            self.ledger
                .update_order_status(
                    &original_intent.intent_id,
                    Self::terminal_ledger_order_status(&status),
                    Some(reason),
                    now_kst_fake_epoch(),
                )
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    /// Auto 소유권 인계가 중간에 실패하면 포지션을 지우지 않은 채 자동 보호 상태로
    /// 되돌린다. 포지션이 없던 경우에는 Suspended로 두어 신규 진입을 막는다.
    fn abort_auto_handoff(&self, message: impl Into<String>) {
        let message = message.into();
        self.automation
            .lock()
            .unwrap()
            .abort_handoff(message.clone());
        self.persist_automation();
        self.emit_automation_state();
    }

    pub async fn set_control_mode(
        self: &Arc<Self>,
        mode: ControlMode,
    ) -> Result<AutomationSnapshot, String> {
        if !self.is_active() {
            return Err("폐기된 엔진에서는 모드를 전환할 수 없습니다".into());
        }
        // 진행 중인 주문 actor가 종결된 뒤 현재 모드·OCO·주문 복구 표식을 다시 읽는다.
        // 전환이 actor를 먼저 얻으면 아래 장부 종결과 revision 증가가 늦은 진입을 막는다.
        let _actor = self.order_actor.lock().await;
        let current = self.automation.lock().unwrap().mode();
        let durable_handoff = self.pending_auto_handoff()?;
        if let Some(handoff) = durable_handoff.as_ref() {
            if current != ControlMode::Auto {
                if current != handoff.target_mode {
                    return Err(format!(
                        "Auto 인계 복구 표식과 현재 모드가 일치하지 않습니다: 현재={current:?}, 대상={:?}",
                        handoff.target_mode
                    ));
                }
                // 최종 모드는 이미 내구화됐고 표식 삭제만 남은 crash 경계다.
                self.persist_automation_required()?;
                self.ledger
                    .delete_runtime_state(AUTO_HANDOFF_STATE_KEY)
                    .map_err(|error| format!("완료된 Auto 인계 복구 표식 삭제 실패: {error}"))?;
            }
        }
        if current == mode {
            return Ok(self.automation_snapshot());
        }
        if matches!(mode, ControlMode::Auto | ControlMode::Shadow)
            && self
                .settings
                .read()
                .unwrap()
                .openai_api_key
                .trim()
                .is_empty()
        {
            return Err("OpenAI API 키를 먼저 설정하세요".into());
        }

        // Auto 대상의 외부 조회는 기존 모드·시나리오·세션을 변경하기 전에 끝낸다.
        // 조회 실패가 전환 도중 발생해 화면과 장부만 Auto로 남는 부분 전이를 막는다.
        let auto_has_unknown_exposure = if mode == ControlMode::Auto {
            if !self.refresh_account().await {
                return Err("Auto 전환 시 계좌 조회에 실패했습니다".into());
            }
            let settings = self.settings.read().unwrap().clone();
            let auto_codes = [
                settings.auto_symbols.leverage.as_str(),
                settings.auto_symbols.inverse.as_str(),
            ];
            let unknown_position = self
                .account
                .read()
                .unwrap()
                .positions
                .iter()
                .any(|position| position.qty > 0 && auto_codes.contains(&position.code.as_str()));
            let open_orders = self
                .broker
                .open_orders()
                .await
                .map_err(|error| format!("Auto 전환 시 미체결 조회 실패: {error}"))?;
            let unknown_order = open_orders
                .iter()
                .any(|order| order.cancelable_qty > 0 && auto_codes.contains(&order.code.as_str()));
            unknown_position || unknown_order
        } else {
            false
        };

        if current == ControlMode::Shadow {
            self.close_current_group_for_mode_transition()
                .map_err(|error| {
                    format!("기존 섀도 시나리오 종결 실패로 모드를 유지합니다: {error}")
                })?;
            let update = self
                .shadow
                .lock()
                .unwrap()
                .as_mut()
                .and_then(|session| session.request_mode_exit(now_kst_fake_epoch()).ok());
            if let Some(update) = update {
                self.apply_shadow_update(update);
            }
            if self
                .shadow
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|session| session.position().is_some())
            {
                return Err("섀도 포지션이 매수1호가 수량만큼 부분 청산 중입니다. 새 호가에서 계속 정리합니다".into());
            }
            *self.shadow.lock().unwrap() = None;
        }

        // Auto→Shadow는 새 세션 장부 생성까지 성공한 뒤에만 Auto 포지션 소유권을
        // 제거한다. 이후 공통 모드 전환에서는 이 세션을 재사용한다.
        let mut prepared_session = None;
        if current == ControlMode::Auto {
            if self.automation.lock().unwrap().pending_entry().is_some() {
                return Err(
                    "자동 진입 주문을 조정 중입니다. 체결·목표 주문 확인 뒤 모드를 전환하세요"
                        .into(),
                );
            }
            let mut handoff_position = self.automation.lock().unwrap().position().cloned();
            let original_position = handoff_position.clone();
            if handoff_position
                .as_ref()
                .is_some_and(|position| position.exit_intent_id.is_some())
            {
                return Err(
                    "자동 청산 주문을 조정 중입니다. 종결 확인 뒤 모드를 전환하세요".into(),
                );
            }
            if handoff_position.as_ref().is_some_and(|position| {
                position.target_intent_id.is_some()
                    && (position.target_order_no.is_none() || position.target_org_no.is_none())
            }) {
                return Err(
                    "자동 목표 주문 응답을 조정 중입니다. 주문번호·조직번호 확인 뒤 모드를 전환하세요"
                        .into(),
                );
            }

            let created_handoff_marker = match durable_handoff.as_ref() {
                Some(handoff) if handoff.target_mode != mode => {
                    return Err(format!(
                        "이미 {:?} 모드로 Auto 인계를 복구 중입니다",
                        handoff.target_mode
                    ));
                }
                Some(_) => false,
                None => {
                    self.ledger
                        .set_runtime_state(
                            AUTO_HANDOFF_STATE_KEY,
                            &PendingAutoHandoff {
                                target_mode: mode,
                                requested_at: now_kst_fake_epoch(),
                            },
                            now_kst_fake_epoch(),
                        )
                        .map_err(|error| format!("Auto 인계 복구 표식 저장 실패: {error}"))?;
                    true
                }
            };
            if let Err(error) = self.close_current_group_for_mode_transition() {
                let marker_cleanup_error = created_handoff_marker
                    .then(|| {
                        self.ledger
                            .delete_runtime_state(AUTO_HANDOFF_STATE_KEY)
                            .err()
                    })
                    .flatten();
                return Err(if let Some(cleanup_error) = marker_cleanup_error {
                    format!(
                        "기존 Auto 시나리오 종결 실패로 모드를 유지하고, 인계 표식 삭제도 실패했습니다: {error}; {cleanup_error}"
                    )
                } else {
                    format!("기존 Auto 시나리오 종결 실패로 모드를 유지합니다: {error}")
                });
            }
            self.automation.lock().unwrap().mark_handoff();
            if let Err(error) = self.persist_automation_required() {
                self.abort_auto_handoff(format!(
                    "Auto 인계 시작 상태를 저장하지 못해 주문 취소를 시작하지 않았습니다: {error}"
                ));
                return Err(format!(
                    "Auto 인계 시작 상태 저장 실패로 모드 전환을 중단했습니다: {error}"
                ));
            }
            if let Some(position) = handoff_position.clone() {
                if let (Some(order_no), Some(org_no)) = (
                    position.target_order_no.as_deref(),
                    position.target_org_no.as_deref(),
                ) {
                    let target_intent = position
                        .target_intent_id
                        .as_deref()
                        .ok_or_else(|| "수동 인계할 목표 주문의 intent가 없습니다".to_string())
                        .and_then(|intent_id| {
                            self.ledger
                                .get_order(intent_id)
                                .map_err(|error| error.to_string())?
                                .ok_or_else(|| {
                                    "수동 인계할 목표 주문 intent를 장부에서 찾지 못했습니다"
                                        .to_string()
                                })
                        });
                    let target_intent = match target_intent {
                        Ok(intent)
                            if intent.broker_order_id.as_deref() == Some(order_no)
                                && intent.broker_org_no.as_deref() == Some(org_no)
                                && intent.code == position.code
                                && intent.side == LedgerSide::Sell
                                && intent.order_type == LedgerOrderType::Limit =>
                        {
                            intent
                        }
                        Ok(_) => {
                            let error = "수동 인계할 목표 주문 identity가 장부 intent와 다릅니다";
                            self.abort_auto_handoff(error);
                            return Err(error.into());
                        }
                        Err(error) => {
                            self.abort_auto_handoff(error.clone());
                            return Err(error);
                        }
                    };
                    let cancel_intent = match self
                        .record_auto_cancel_intent(&position, "수동 인계 취소 의도 기록 실패")
                    {
                        Ok(intent_id) => intent_id,
                        Err(error) => {
                            self.abort_auto_handoff(error.clone());
                            return Err(error);
                        }
                    };
                    match self
                        .broker
                        .cancel_order(&position.code, order_no, org_no)
                        .await
                    {
                        Ok(ack) => {
                            self.record_broker_ack(&cancel_intent, &ack, LedgerOrderStatus::Filled)
                        }
                        Err(error) => self.record_broker_error(
                            &cancel_intent,
                            &error.to_string(),
                            !error.is_confirmed_order_rejection(),
                        ),
                    }
                    // 미체결 목록 부재가 아니라 거래일·조직번호·주문 형태까지 일치하는
                    // 정확 누적상태가 terminal일 때만 인계한다. 취소와 경합한 부분체결도
                    // 같은 상태에서 먼저 적용한 뒤 최신 잔여수량을 넘긴다.
                    if let Err(error) = self.wait_exact_order_terminal(&target_intent).await {
                        self.abort_auto_handoff(format!(
                            "수동 인계 전 목표 주문의 정확한 종결 확인 실패: {error}"
                        ));
                        return Err(format!(
                            "목표 주문의 정확한 종결을 확인하지 못해 모드 전환을 중단했습니다: {error}"
                        ));
                    }
                    match self
                        .reconcile_bound_target_status_for_exit(
                            &position,
                            &target_intent,
                            order_no,
                            org_no,
                        )
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => {
                            let error = "정확 조회 뒤 목표 주문이 다시 비종결 상태로 보여 모드 전환을 중단했습니다";
                            self.abort_auto_handoff(error);
                            return Err(error.into());
                        }
                        Err(error) => {
                            self.abort_auto_handoff(format!(
                                "수동 인계 전 목표 체결·계좌 조정 실패: {error}"
                            ));
                            return Err(format!(
                                "목표 주문 조정 실패로 모드 전환을 중단했습니다: {error}"
                            ));
                        }
                    }
                }
            }
            // 포지션 소유권을 제거하기 전에 장부상 Auto 미체결이 모두 종결됐음을
            // 확인한다. 실패 시 abort_handoff가 Holding/Suspended로 복원하므로
            // Auto Idle에서 새 OCO가 무장되는 상태가 생기지 않는다.
            if let Err(error) = self.cancel_known_auto_open_orders("Auto 모드 종료").await {
                self.abort_auto_handoff(error.clone());
                return Err(error);
            }

            if mode == ControlMode::Shadow {
                let session_id = unique_id("shadow");
                if let Err(error) = self.ledger.start_session(&NewSession {
                    session_id: session_id.clone(),
                    execution_kind: LedgerExecutionKind::Shadow,
                    control_mode: ledger_control_mode(mode),
                    starting_cash: Some(self.account.read().unwrap().cash),
                    started_at: now_kst_fake_epoch(),
                }) {
                    let error = format!("Shadow 세션 장부 생성 실패: {error}");
                    self.abort_auto_handoff(error.clone());
                    return Err(error);
                }
                prepared_session = Some(session_id);
            }

            // 목표 체결 통보와 경합해도 한 잠금 안에서 최신 잔여 포지션만 인계한다.
            handoff_position = self.automation.lock().unwrap().clear_position_for_handoff();
            if let Some(position) = original_position.as_ref().filter(|position| {
                position.target_order_no.is_some() && position.target_org_no.is_some()
            }) {
                // 전량 목표 체결이면 complete_real_exit가 예약과 목표 intent를 이미
                // 종결했다. 잔여 포지션이 있을 때만 취소된 잔량을 정리한다.
                if handoff_position.is_some() {
                    if let Some(reservation) =
                        self.reservations.write().unwrap().remove(&position.code)
                    {
                        self.emit(
                            "reservation",
                            &reservation_info(
                                &position.code,
                                &reservation,
                                "cancelled",
                                Some("Auto 포지션을 Manual로 인계했습니다".into()),
                            ),
                        );
                    }
                    if let Some(intent_id) = &position.target_intent_id {
                        let _ = self.ledger.update_order_status(
                            intent_id,
                            LedgerOrderStatus::Cancelled,
                            Some("auto_handoff"),
                            now_kst_fake_epoch(),
                        );
                    }
                } else if let Some(intent_id) = &position.target_intent_id {
                    let _ = self.ledger.update_order_status(
                        intent_id,
                        LedgerOrderStatus::Filled,
                        Some("auto_handoff_target_filled"),
                        now_kst_fake_epoch(),
                    );
                }
            }
            if let Some(position) = &handoff_position {
                let _ = self.ledger.upsert_trade(&NewTrade {
                    trade_id: position.trade_id.clone(),
                    session_id: self
                        .automation
                        .lock()
                        .unwrap()
                        .session_id()
                        .map(str::to_owned),
                    execution_kind: LedgerExecutionKind::Real,
                    origin: LedgerOrigin::AutoHandoff,
                    code: position.code.clone(),
                    entry_qty: position.entry_qty,
                    exit_qty: position.exit_qty,
                    entry_avg_price: position.avg_price,
                    exit_avg_price: None,
                    pnl_rate: None,
                    entered_at: position.first_fill_at,
                    exited_at: None,
                    status: LedgerTradeStatus::HandedOff,
                    exit_reason: Some("auto_handoff".into()),
                    updated_at: now_kst_fake_epoch(),
                });
                let accumulator = ManualTradeAccumulator {
                    trade_id: position.trade_id.clone(),
                    code: position.code.clone(),
                    origin: LedgerOrigin::AutoHandoff,
                    entry_qty: position.entry_qty,
                    entry_value: position.avg_price * position.entry_qty as f64,
                    exit_qty: position.exit_qty,
                    exit_value: position.exit_value,
                    entered_at: position.first_fill_at,
                };
                let _ = self.ledger.set_runtime_state(
                    &format!("manual_trade:{}", position.code),
                    &accumulator,
                    now_kst_fake_epoch(),
                );
            }
        }

        let old_session = self
            .automation
            .lock()
            .unwrap()
            .session_id()
            .map(str::to_owned);
        if let Some(session_id) = old_session {
            let _ = self.ledger.end_session(
                &session_id,
                crate::ledger::LedgerSessionStatus::Closed,
                now_kst_fake_epoch(),
            );
        }

        let account_cash = self.account.read().unwrap().cash;
        let new_session = if mode == ControlMode::Manual {
            None
        } else if let Some(session_id) = prepared_session {
            Some(session_id)
        } else {
            let session_id = unique_id(if mode == ControlMode::Auto {
                "auto"
            } else {
                "shadow"
            });
            self.ledger
                .start_session(&NewSession {
                    session_id: session_id.clone(),
                    execution_kind: if mode == ControlMode::Shadow {
                        LedgerExecutionKind::Shadow
                    } else {
                        LedgerExecutionKind::Real
                    },
                    control_mode: ledger_control_mode(mode),
                    starting_cash: Some(account_cash),
                    started_at: now_kst_fake_epoch(),
                })
                .map_err(|error| error.to_string())?;
            Some(session_id)
        };
        if mode == ControlMode::Shadow {
            *self.shadow.lock().unwrap() = Some(ShadowSession::start(account_cash));
        }
        {
            let mut runtime = self.automation.lock().unwrap();
            runtime.set_session_id(new_session);
            runtime.set_mode_after_cleanup(
                mode,
                self.next_automation_slot(now_kst_fake_epoch().saturating_add(1)),
                (mode == ControlMode::Shadow).then_some(account_cash),
            );
        }

        if mode == ControlMode::Auto && auto_has_unknown_exposure {
            self.automation.lock().unwrap().suspend(
                "소유권을 확인할 수 없는 자동 종목 보유·주문이 있어 신규 진입을 중단했습니다",
            );
        }

        if let Err(error) = self.persist_automation_required() {
            self.emit_automation_state();
            return Err(format!(
                "모드 전환은 메모리에 반영됐지만 최종 상태 저장에 실패했습니다. 다음 실행에서 Auto 인계를 복구합니다: {error}"
            ));
        }
        if current == ControlMode::Auto {
            self.ledger
                .delete_runtime_state(AUTO_HANDOFF_STATE_KEY)
                .map_err(|error| {
                    format!("모드 전환 완료 뒤 Auto 인계 복구 표식 삭제 실패: {error}")
                })?;
        }
        self.emit_automation_state();
        Ok(self.automation_snapshot())
    }

    /// KIS의 현재 잔고를 기준으로 자동매매 소유권·인계 표식·메모리 캐시를 버리고
    /// 수동 Idle로 되돌린다. 거래·주문 기록과 설정, KIS 토큰 파일은 보존한다.
    ///
    /// 실제 미체결 주문이 있으면 소유권을 버릴 수 없으므로 아무 상태도 바꾸지 않고
    /// 거부한다. 호출자는 성공 직후 이 엔진을 내려 새 피드로 다시 시작해야 한다.
    pub async fn reset_runtime_for_resync(&self) -> Result<(), String> {
        // 진행 중인 주문 actor가 끝난 뒤 검증부터 초기화까지 독점해, 조회와 상태
        // 폐기 사이에 이 앱이 새 주문을 제출하는 경계를 없앤다.
        let _actor = self.order_actor.lock().await;
        let account = verify_runtime_resync_safe(self.broker.as_ref()).await?;
        self.ledger
            .reset_runtime_state_to_manual(now_kst_fake_epoch())
            .map_err(|error| format!("런타임 상태 초기화 실패: {error}"))?;

        *self.automation.lock().unwrap() =
            AutomationRuntime::new(PersistedAutomation::default(), None);
        *self.shadow.lock().unwrap() = None;
        self.quotes.write().unwrap().clear();
        self.reservations.write().unwrap().clear();
        self.stop_loss_lock.lock().unwrap().clear();
        self.fill_reconcile_pending.lock().unwrap().clear();
        self.auto_flatten_pending.store(false, Ordering::SeqCst);
        self.entry_reconcile_pending.store(false, Ordering::SeqCst);
        self.exit_reconcile_pending.store(false, Ordering::SeqCst);
        self.refresh_pending.store(false, Ordering::SeqCst);
        self.connected.store(false, Ordering::SeqCst);
        *self.account.write().unwrap() = account.clone();
        self.account_gen.fetch_add(1, Ordering::SeqCst);
        self.account_refreshed_gen
            .store(self.account_gen.load(Ordering::SeqCst), Ordering::SeqCst);
        self.market_history.mark_all_gapped().await;
        self.emit("account", &account);
        self.emit_automation_state();
        Ok(())
    }

    /// 브로커 재시작이 필요 없는 설정 변경(테마·차트 주기 등)을 반영
    pub fn update_settings(&self, new: Settings) {
        *self.settings.write().unwrap() = new;
    }

    /// 계좌·키·구독 등 브로커 설정을 바꾸기 전에 기존 엔진을 안전하게 내릴 수
    /// 있는지 확인한다. 실제 보유나 주문 조정 상태가 하나라도 있으면 설정 저장을
    /// 거부해 기존 계좌의 보호 태스크와 주문 식별자를 고아로 만들지 않는다.
    pub async fn ensure_broker_restart_safe(&self) -> Result<(), String> {
        let _actor = self.order_actor.lock().await;
        {
            let runtime = self.automation.lock().unwrap();
            if runtime.mode() != ControlMode::Manual
                || runtime.position().is_some()
                || runtime.pending_entry().is_some()
                || runtime.phase() != crate::types::AutomationPhase::Idle
            {
                return Err(
                    "브로커 설정은 수동 모드의 주문·자동 포지션 없는 Idle 상태에서만 변경할 수 있습니다"
                        .into(),
                );
            }
        }
        if !self.reservations.read().unwrap().is_empty() {
            return Err("예약 매도를 취소한 뒤 브로커 설정을 변경하세요".into());
        }
        if self
            .ledger
            .get_runtime_state::<LateAutoEntryWatch>(LATE_AUTO_ENTRY_WATCH_KEY)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err(
                "지연 Auto 진입 주문을 조정 중이어서 브로커 설정을 변경할 수 없습니다".into(),
            );
        }
        if !self.refresh_account().await {
            return Err("브로커 설정 변경 전 기존 계좌 잔고 조회에 실패했습니다".into());
        }
        if self
            .account_snapshot()
            .positions
            .iter()
            .any(|position| position.qty > 0)
        {
            return Err("기존 계좌 보유를 모두 정리한 뒤 브로커 설정을 변경하세요".into());
        }
        let open_orders = self
            .broker
            .open_orders()
            .await
            .map_err(|error| format!("브로커 설정 변경 전 미체결 조회 실패: {error}"))?;
        if open_orders.iter().any(|order| order.cancelable_qty > 0) {
            return Err("기존 계좌 미체결 주문을 모두 종결한 뒤 브로커 설정을 변경하세요".into());
        }

        // 미체결 목록은 주문 직후 일시적으로 비어 있을 수 있다. 특히 broker ID가
        // 없는 Dispatching/Unknown POST를 남긴 채 계정·키를 바꾸면 뒤늦은 체결을
        // 새 엔진이 영원히 조정하지 못한다. 당일 장부의 비종결 실주문을 exact
        // order_status로 하나씩 종결하고, 식별자가 없거나 형태가 다르면 변경을 막는다.
        let trading_date = crate::util::trading_date_from_fake_epoch(now_kst_fake_epoch())
            .map_err(|error| error.to_string())?;
        let unresolved = self
            .ledger
            .list_unresolved_real_orders(&trading_date)
            .map_err(|error| error.to_string())?;
        for intent in unresolved {
            if intent.broker_order_id.as_deref().is_none_or(str::is_empty)
                || intent.broker_org_no.as_deref().is_none_or(str::is_empty)
            {
                return Err(format!(
                    "브로커 응답이 불명확한 당일 실주문({})을 조정 중이어서 설정을 변경할 수 없습니다",
                    intent.intent_id
                ));
            }
            let status = self
                .wait_exact_order_terminal(&intent)
                .await
                .map_err(|error| {
                    format!(
                        "당일 실주문({})의 정확한 종결을 확인할 수 없어 설정을 변경하지 않습니다: {error}",
                        intent.intent_id
                    )
                })?;
            self.ledger
                .update_order_status(
                    &intent.intent_id,
                    Self::terminal_ledger_order_status(&status),
                    Some("브로커 설정 변경 전 정확 주문상태로 종결 확인"),
                    now_kst_fake_epoch(),
                )
                .map_err(|error| error.to_string())?;
        }

        // exact 조회 중 늦은 체결이나 새 계좌 반영이 나타날 수 있으므로 actor를 잡은
        // 상태에서 잔고와 미체결을 마지막으로 다시 읽는다. 위 주문들이 terminal이라
        // 이 최종 스냅샷 뒤에 새 경제 체결이 생길 경로는 없다.
        if !self.refresh_account().await {
            return Err("브로커 설정 변경 직전 최종 잔고 조회에 실패했습니다".into());
        }
        if self
            .account_snapshot()
            .positions
            .iter()
            .any(|position| position.qty > 0)
        {
            return Err("기존 계좌 보유를 모두 정리한 뒤 브로커 설정을 변경하세요".into());
        }
        let final_open_orders = self
            .broker
            .open_orders()
            .await
            .map_err(|error| format!("브로커 설정 변경 직전 최종 미체결 조회 실패: {error}"))?;
        if final_open_orders
            .iter()
            .any(|order| order.cancelable_qty > 0)
        {
            return Err("기존 계좌 미체결 주문을 모두 종결한 뒤 브로커 설정을 변경하세요".into());
        }
        Ok(())
    }

    /// 프론트로 이벤트 전송 (테스트 등 콜백이 없으면 생략)
    fn emit<S: serde::Serialize>(&self, event: &str, payload: &S) {
        if !self.is_active() {
            return;
        }
        let Some(f) = &self.emit_fn else { return };
        match serde_json::to_value(payload) {
            Ok(v) => f(event, v),
            Err(e) => tracing::warn!("이벤트 직렬화 실패({event}): {e}"),
        }
    }

    pub async fn refresh_account(&self) -> bool {
        // 조회 도중 새 체결이 오면 gen이 더 커져 다음 주문이 다시 갱신하게 된다
        let gen = self.account_gen.load(Ordering::SeqCst);
        match self.broker.account().await {
            Ok(snap) => {
                *self.account.write().unwrap() = snap.clone();
                self.account_refreshed_gen.fetch_max(gen, Ordering::SeqCst);
                self.last_error.lock().unwrap().clear();
                // 실제로 보유가 사라진 종목은 손절 잠금을 해제해 재무장한다
                // (다시 매수해 손절선에 닿으면 또 발동해야 한다)
                let held: HashSet<&str> = snap
                    .positions
                    .iter()
                    .filter(|p| p.qty > 0)
                    .map(|p| p.code.as_str())
                    .collect();
                self.stop_loss_lock
                    .lock()
                    .unwrap()
                    .retain(|code, _| held.contains(code.as_str()));
                self.emit("account", &snap);
                true
            }
            Err(e) => {
                let msg = e.to_string();
                let mut last = self.last_error.lock().unwrap();
                if *last != msg {
                    *last = msg.clone();
                    self.emit("engine-error", &format!("계좌 조회 실패: {msg}"));
                }
                false
            }
        }
    }

    pub fn account_snapshot(&self) -> AccountSnapshot {
        self.account.read().unwrap().clone()
    }

    /// 1분봉 조회 — 최초/실제 연결 공백 때만 백필하고 정상 연결 중에는 실시간으로 갱신한다.
    pub async fn candles(&self, code: &str) -> AppResult<Vec<Candle>> {
        let started = std::time::Instant::now();
        let broker = Arc::clone(&self.broker);
        let bars = self
            .market_history
            .get_or_fetch(code, || async move { broker.candles_1m(code).await })
            .await?;
        let elapsed = started.elapsed();
        if elapsed.as_millis() > 300 {
            tracing::info!(
                "분봉 로드 {code}: {}건, {:.1}초",
                bars.len(),
                elapsed.as_secs_f32()
            );
        }
        Ok((*bars).clone())
    }

    /// 주문에 쓸 신선한 시세. 캐시가 낡았으면 REST 폴백, 그래도 없으면 None.
    async fn fresh_quote(&self, code: &str) -> Option<Quote> {
        let cached = self.quotes.read().unwrap().get(code).cloned();
        let now = now_kst_fake_epoch();
        if let Some(q) = &cached {
            if now - q.trade_ts <= QUOTE_FRESH_SECS {
                return cached;
            }
        }
        match self.broker.snapshot(code).await {
            Ok(q) => {
                let _market_guard = self.automation_market_gate.lock().await;
                self.quotes
                    .write()
                    .unwrap()
                    .insert(code.to_string(), q.clone());
                // 백엔드 캐시만 채우면 화면 수익률은 낡은 값에 머문다 — 프론트에도 반영
                self.emit("quote", &q);
                Some(q)
            }
            Err(e) => {
                tracing::warn!("스냅샷 폴백 실패({code}): {e}");
                cached.filter(|q| now - q.trade_ts <= QUOTE_STALE_LIMIT_SECS)
            }
        }
    }

    /// 원클릭 즉시 매수: 매도1호가 +3% IOC지정가, 매도1호가 기준 주문가능현금의 95%
    pub async fn buy_max(&self, code: &str) -> OrderResult {
        let _actor = self.order_actor.lock().await;
        let fail = |message: String| OrderResult {
            ok: false,
            side: Side::Buy,
            code: code.to_string(),
            qty: 0,
            price: 0,
            message,
            local_order_id: None,
        };
        if self.automation.lock().unwrap().mode() == ControlMode::Auto {
            return fail("Auto 모드에서는 수동 매수 주문을 실행할 수 없습니다".into());
        }

        let Some(q) = self.fresh_quote(code).await else {
            return fail("시세 없음 — 연결 상태를 확인하세요".into());
        };
        // 지금 살 수 있는 매물을 기준으로 삼고, 호가가 없을 때만 현재가로 폴백한다.
        // 신선한 시세는 웹소켓 캐시에서 읽으므로 이 계산은 REST 호출을 추가하지 않는다.
        let base = if q.ask1 > 0.0 { q.ask1 } else { q.price };
        if base <= 0.0 {
            return fail("시세 정보 없음".into());
        }

        let etf = self.settings.read().unwrap().is_etf(code);
        let limit = buy_limit_price(base as u64, etf);
        // 매도 직후 재매수 등 직전 주문이 아직 캐시에 반영 전이면 동기 갱신 (스캘핑 연속 매매 대응)
        self.sync_account_if_stale().await;
        let cash = self.account.read().unwrap().cash;
        // 수량은 매도1호가 기준 95%로 잡는다. +3%는 체결 허용 상한일 뿐 실제 체결가는
        // 현재 매도호가부터 적용되므로 시드가 약 95% 투입된다.
        let mut qty = max_buy_qty(cash, base as u64);
        if qty == 0 {
            // 앱 시작 직후 첫 갱신 전 등 캐시가 비어 있을 수 있다 — 동기 갱신 후 재계산
            self.refresh_account().await;
            let cash = self.account.read().unwrap().cash;
            qty = max_buy_qty(cash, base as u64);
            if qty == 0 {
                return fail(format!("주문가능금액 부족 (예수금 {cash}원)"));
            }
        }

        let ioc = true;
        let (intent_id, local_order_id) = match self.record_manual_order_intent(
            code,
            LedgerSide::Buy,
            LedgerOrderType::IocLimit,
            qty,
            Some(limit),
        ) {
            Ok(intent) => intent,
            Err(error) => return fail(format!("주문 의도 장부 기록 실패: {error}")),
        };
        match self.broker.place_buy(code, qty, limit, ioc).await {
            Ok(ack) => {
                self.record_broker_ack(&intent_id, &ack, LedgerOrderStatus::Submitted);
                self.after_buy_success(code).await;
                self.mark_account_stale();
                self.schedule_account_refresh();
                OrderResult {
                    ok: true,
                    side: Side::Buy,
                    code: code.to_string(),
                    qty,
                    price: limit,
                    message: ack.message,
                    local_order_id: Some(local_order_id),
                }
            }
            Err(e) => {
                let confirmed_rejection = e.is_confirmed_order_rejection();
                self.record_broker_error(&intent_id, &e.to_string(), !confirmed_rejection);
                if !confirmed_rejection {
                    self.schedule_account_refresh();
                    return fail(format!(
                        "주문 응답 불명확 — 재주문하지 않고 장부에 확인 대기로 기록했습니다: {e}"
                    ));
                }
                if !e
                    .order_rejection()
                    .is_some_and(|rejection| rejection.is_buying_power_shortfall())
                {
                    // 매수가능수량 재조회·재주문은 KIS가 주문가능금액 부족/초과를
                    // 명시한 경우에만 허용한다. 매매정지·가격 오류·유량 제한 같은
                    // 다른 확정 거부는 원문을 그대로 노출하고 두 번째 POST를 막는다.
                    self.schedule_account_refresh();
                    return fail(e.to_string());
                }
                // 캐시 예수금이 실제 주문가능금액보다 부풀려졌을 수 있다(미정산 매도대금 등).
                // KIS가 계산한 매수가능수량으로 1회만 재주문 — 첫 주문은 확정 거부라 이중 주문 위험 없음.
                let retried = self.retry_buy_with_psbl(code, qty, limit, ioc).await;
                self.schedule_account_refresh();
                retried.unwrap_or_else(|| fail(e.to_string()))
            }
        }
    }

    /// 주문가능금액 부족/초과 거부 시 KIS 매수가능수량(미수없는매수수량)으로 1회 재주문.
    /// 수량이 줄지 않으면 None을 돌려 원래 에러를 노출한다.
    async fn retry_buy_with_psbl(
        &self,
        code: &str,
        rejected_qty: u64,
        limit: u64,
        ioc: bool,
    ) -> Option<OrderResult> {
        let fail = |message: String| OrderResult {
            ok: false,
            side: Side::Buy,
            code: code.to_string(),
            qty: 0,
            price: 0,
            message,
            local_order_id: None,
        };

        let qty = match self.broker.max_buy_qty(code, limit).await {
            Ok(q) => q,
            Err(e) => {
                tracing::warn!("매수가능수량 조회 실패({code}): {e}");
                return None;
            }
        };
        if qty >= rejected_qty {
            return None;
        }
        if qty == 0 {
            return Some(fail("주문가능금액 부족".into()));
        }

        tracing::info!(
            "매수 거부 → KIS 매수가능수량 {qty}주로 재주문 ({code}, 거부 수량 {rejected_qty}주)"
        );
        let (intent_id, local_order_id) = match self.record_manual_order_intent(
            code,
            LedgerSide::Buy,
            LedgerOrderType::IocLimit,
            qty,
            Some(limit),
        ) {
            Ok(intent) => intent,
            Err(error) => return Some(fail(format!("재주문 의도 장부 기록 실패: {error}"))),
        };
        match self.broker.place_buy(code, qty, limit, ioc).await {
            Ok(ack) => {
                self.record_broker_ack(&intent_id, &ack, LedgerOrderStatus::Submitted);
                self.after_buy_success(code).await;
                self.mark_account_stale();
                Some(OrderResult {
                    ok: true,
                    side: Side::Buy,
                    code: code.to_string(),
                    qty,
                    price: limit,
                    message: ack.message,
                    local_order_id: Some(local_order_id),
                })
            }
            Err(e) => {
                self.record_broker_error(
                    &intent_id,
                    &e.to_string(),
                    !e.is_confirmed_order_rejection(),
                );
                Some(fail(e.to_string()))
            }
        }
    }

    /// 원클릭 전량 매도: 보유 수량 전부 시장가
    pub async fn sell_all(&self, code: &str) -> OrderResult {
        self.sell_all_internal(code, false).await
    }

    /// 기존 Manual 보유의 -0.6% 보호는 Auto 모드에서도 유지한다. 사용자 수동
    /// 주문 차단과 엔진의 Manual 보호청산을 분리하되 Auto 소유 종목은 재검증한다.
    async fn sell_all_internal(&self, code: &str, manual_protection: bool) -> OrderResult {
        let _actor = self.order_actor.lock().await;
        let fail = |message: String| OrderResult {
            ok: false,
            side: Side::Sell,
            code: code.to_string(),
            qty: 0,
            price: 0,
            message,
            local_order_id: None,
        };
        let (auto_mode, auto_owned) = {
            let runtime = self.automation.lock().unwrap();
            (
                runtime.mode() == ControlMode::Auto,
                runtime.owns_real_code(code),
            )
        };
        if auto_mode && !manual_protection {
            return fail("Auto 모드에서는 수동 매도 주문을 실행할 수 없습니다".into());
        }
        if manual_protection && auto_owned {
            return fail("Auto 소유 포지션은 Manual 손절 경로로 매도할 수 없습니다".into());
        }

        // 예약 매도가 걸려 있으면 물량이 잠겨 시장가 매도가 거부되므로 먼저 취소한다
        match self.cancel_reservation_internal(code).await {
            Ok(Some(r)) => {
                self.emit(
                    "reservation",
                    &reservation_info(code, &r, "cancelled", None),
                );
            }
            Ok(None) => {}
            Err(error) => {
                return fail(format!(
                    "예약 주문 취소 실패 — 시장가 매도를 중단했습니다: {error}"
                ))
            }
        }

        // 매수 체결 직후 매도 등 직전 주문이 아직 캐시에 반영 전이면 동기 갱신 (스캘핑 연속 매매 대응)
        self.sync_account_if_stale().await;
        let mut qty = self.cached_position_qty(code);
        if qty == 0 {
            // 앱 밖(HTS 등)에서 산 종목처럼 엔진이 모르는 변동일 수 있다 — 동기 갱신 후 재확인
            self.refresh_account().await;
            qty = self.cached_position_qty(code);
        }
        if qty == 0 {
            return fail("보유 수량 없음".into());
        }

        let (intent_id, local_order_id) = match self.record_manual_order_intent(
            code,
            LedgerSide::Sell,
            LedgerOrderType::Market,
            qty,
            None,
        ) {
            Ok(intent) => intent,
            Err(error) => return fail(format!("주문 의도 장부 기록 실패: {error}")),
        };
        match self.broker.place_sell_market(code, qty).await {
            Ok(ack) => {
                self.record_broker_ack(&intent_id, &ack, LedgerOrderStatus::Submitted);
                self.mark_account_stale();
                self.schedule_account_refresh();
                OrderResult {
                    ok: true,
                    side: Side::Sell,
                    code: code.to_string(),
                    qty,
                    price: 0,
                    message: ack.message,
                    local_order_id: Some(local_order_id),
                }
            }
            Err(e) => {
                self.record_broker_error(
                    &intent_id,
                    &e.to_string(),
                    !e.is_confirmed_order_rejection(),
                );
                self.schedule_account_refresh();
                fail(e.to_string())
            }
        }
    }

    /// 잔고를 바꾼 사건(주문 접수/체결통보) 발생 표시 — 다음 주문이 캐시를 갱신하게 한다
    fn mark_account_stale(&self) {
        self.account_gen.fetch_add(1, Ordering::SeqCst);
    }

    /// 마지막 사건 이후 성공한 잔고 갱신이 없으면 동기로 1회 갱신.
    /// 최신이면 REST 호출 없이 즉시 반환하므로 정상 주문 경로의 속도는 그대로다.
    async fn sync_account_if_stale(&self) -> bool {
        let stale = self.account_refreshed_gen.load(Ordering::SeqCst)
            < self.account_gen.load(Ordering::SeqCst);
        if stale {
            self.refresh_account().await
        } else {
            true
        }
    }

    fn cached_position_qty(&self, code: &str) -> u64 {
        self.account
            .read()
            .unwrap()
            .positions
            .iter()
            .find(|p| p.code == code)
            .map(|p| p.qty)
            .unwrap_or(0)
    }

    /// 캐시된 보유 (수량, 평단). 없으면 (0, 0.0)
    fn cached_position(&self, code: &str) -> (u64, f64) {
        self.account
            .read()
            .unwrap()
            .positions
            .iter()
            .find(|p| p.code == code)
            .map(|p| (p.qty, p.avg_price))
            .unwrap_or((0, 0.0))
    }

    /// 현재 체결가 기준 수익률이 강제 손절선 이하이고 재발동 잠금이 풀려 있으면
    /// 잠금을 걸고 true를 돌려준다. 판정과 잠금을 한 번에 처리해 연속 틱의 중복 매도를 막는다.
    fn arm_stop_loss_if_breached(&self, code: &str, price: f64) -> bool {
        let (qty, avg) = self.cached_position(code);
        if qty == 0 || !crate::util::hits_stop_loss(avg, price) {
            return false;
        }
        let now = std::time::Instant::now();
        let mut lock = self.stop_loss_lock.lock().unwrap();
        if lock.get(code).is_some_and(|until| now < *until) {
            return false; // 발동 직후 or 실패 후 재시도 대기 중
        }
        lock.insert(code.to_string(), now + STOP_LOSS_RELOCK);
        true
    }

    /// 강제 손절 실행: 보유 전량 시장가 매도(예약 매도가 걸려 있으면 sell_all이 먼저 취소).
    /// 성공하면 잠금이 풀리기 전에 잔고 갱신이 포지션을 지우고, 실패하면 잠금이 만료된 뒤 재시도된다.
    async fn force_stop_loss(&self, code: &str) {
        let (qty, avg) = self.cached_position(code);
        let price = self
            .quotes
            .read()
            .unwrap()
            .get(code)
            .map(|q| q.price)
            .unwrap_or(0.0);
        let rate = crate::util::pnl_rate(avg, price);
        tracing::warn!("강제 손절 발동: {code} 수익률 {rate:.2}% (평단 {avg}, 현재가 {price}, {qty}주) — 전량 매도");
        self.emit(
            "engine-error",
            &format!(
                "🛑 강제 손절: {code} 수익률 {rate:.2}% (손절선 {}%) → 전량 매도",
                crate::util::STOP_LOSS_PCT
            ),
        );

        let result = self.sell_all_internal(code, true).await;
        if !result.ok {
            tracing::error!("강제 손절 매도 실패({code}): {}", result.message);
            self.emit(
                "engine-error",
                &format!("강제 손절 매도 실패({code}): {}", result.message),
            );
            // 잠금은 유지 — STOP_LOSS_RELOCK 뒤 다음 틱에 재시도된다(초당 주문 폭주 방지)
        }
    }

    /// 예약 매도 설정: 평단 × (1 + pct/100) 이상 첫 호가에 보유 전량 지정가 매도를 걸어둔다.
    /// 기존 예약이 있으면 취소 후 교체한다.
    pub async fn place_reserved_sell(&self, code: &str, target_pct: f64) -> OrderResult {
        let _actor = self.order_actor.lock().await;
        let fail = |message: String| OrderResult {
            ok: false,
            side: Side::Sell,
            code: code.to_string(),
            qty: 0,
            price: 0,
            message,
            local_order_id: None,
        };
        if self.automation.lock().unwrap().mode() == ControlMode::Auto {
            return fail("Auto 모드에서는 수동 예약매도를 실행할 수 없습니다".into());
        }

        // 매수 직후 예약 등 직전 주문이 아직 캐시에 반영 전이면 동기 갱신
        self.sync_account_if_stale().await;
        let (mut qty, mut avg) = self.cached_position(code);
        if qty == 0 || avg <= 0.0 {
            // 앱 시작 직후 등 캐시가 비어 있을 수 있다 — 동기 갱신 후 재확인
            self.refresh_account().await;
            let (q, a) = self.cached_position(code);
            qty = q;
            avg = a;
        }
        if qty == 0 || avg <= 0.0 {
            return fail("보유 수량 없음".into());
        }

        let etf = self.settings.read().unwrap().is_etf(code);
        let target = sell_target_price(avg, target_pct, etf);
        if target == 0 {
            return fail("목표가 계산 실패".into());
        }

        // 같은 종목에 이미 예약이 있으면 먼저 취소 (물량 이중 주문 방지)
        if let Err(error) = self.cancel_reservation_internal(code).await {
            return fail(format!("기존 예약 주문 취소 실패: {error}"));
        }

        let (intent_id, local_order_id) = match self.record_manual_order_intent(
            code,
            LedgerSide::Sell,
            LedgerOrderType::Limit,
            qty,
            Some(target),
        ) {
            Ok(intent) => intent,
            Err(error) => return fail(format!("예약 주문 의도 장부 기록 실패: {error}")),
        };
        match self.broker.place_sell_limit(code, qty, target).await {
            Ok(ack) => {
                self.record_broker_ack(&intent_id, &ack, LedgerOrderStatus::Submitted);
                let message = ack.message.clone();
                let r = Reservation {
                    target_pct,
                    target_price: target,
                    qty,
                    order_no: ack.order_no,
                    org_no: ack.org_no,
                };
                self.reservations
                    .write()
                    .unwrap()
                    .insert(code.to_string(), r.clone());
                self.mark_account_stale();
                self.schedule_account_refresh();
                self.emit("reservation", &reservation_info(code, &r, "waiting", None));
                OrderResult {
                    ok: true,
                    side: Side::Sell,
                    code: code.to_string(),
                    qty,
                    price: target,
                    message,
                    local_order_id: Some(local_order_id),
                }
            }
            Err(e) => {
                self.record_broker_error(
                    &intent_id,
                    &e.to_string(),
                    !e.is_confirmed_order_rejection(),
                );
                self.schedule_account_refresh();
                let mut result = fail(e.to_string());
                result.local_order_id = Some(local_order_id);
                result
            }
        }
    }

    /// 예약 매도 취소 (사용자 요청). 이미 체결돼 예약이 사라진 경우는 실패로 안내.
    pub async fn cancel_reserved_sell(&self, code: &str) -> OrderResult {
        let _actor = self.order_actor.lock().await;
        if self.automation.lock().unwrap().mode() == ControlMode::Auto {
            return OrderResult {
                ok: false,
                side: Side::Sell,
                code: code.to_string(),
                qty: 0,
                price: 0,
                message: "Auto 모드에서는 수동 예약매도 취소를 실행할 수 없습니다".into(),
                local_order_id: None,
            };
        }
        let Some(r) = self.reservations.read().unwrap().get(code).cloned() else {
            return OrderResult {
                ok: false,
                side: Side::Sell,
                code: code.to_string(),
                qty: 0,
                price: 0,
                message: "취소할 예약이 없습니다".into(),
                local_order_id: None,
            };
        };
        let (intent_id, local_order_id) = match self.record_manual_order_intent(
            code,
            LedgerSide::Sell,
            LedgerOrderType::Cancel,
            r.qty.max(1),
            Some(r.target_price),
        ) {
            Ok(intent) => intent,
            Err(error) => {
                return OrderResult {
                    ok: false,
                    side: Side::Sell,
                    code: code.to_string(),
                    qty: 0,
                    price: 0,
                    message: format!("취소 의도 장부 기록 실패: {error}"),
                    local_order_id: None,
                }
            }
        };
        let result = self.broker.cancel_order(code, &r.order_no, &r.org_no).await;
        match result {
            Ok(ack) => {
                self.record_broker_ack(&intent_id, &ack, LedgerOrderStatus::Filled);
                // await 중 체결통보로 이미 제거됐을 수 있다.
                let removed = self.reservations.write().unwrap().remove(code).is_some();
                if removed {
                    self.emit(
                        "reservation",
                        &reservation_info(code, &r, "cancelled", None),
                    );
                    self.mark_account_stale();
                    self.schedule_account_refresh();
                }
                OrderResult {
                    ok: true,
                    side: Side::Sell,
                    code: code.to_string(),
                    qty: r.qty,
                    price: r.target_price,
                    message: ack.message,
                    local_order_id: Some(local_order_id),
                }
            }
            Err(e) => {
                self.record_broker_error(
                    &intent_id,
                    &e.to_string(),
                    !e.is_confirmed_order_rejection(),
                );
                OrderResult {
                    ok: false,
                    side: Side::Sell,
                    code: code.to_string(),
                    qty: 0,
                    price: 0,
                    message: e.to_string(),
                    local_order_id: Some(local_order_id),
                }
            }
        }
    }

    /// 현재 걸려 있는 예약 목록 (프론트 하이드레이션용)
    pub fn get_reservations(&self) -> Vec<ReservationInfo> {
        self.reservations
            .read()
            .unwrap()
            .iter()
            .map(|(code, r)| reservation_info(code, r, "waiting", None))
            .collect()
    }

    /// 예약이 있으면 브로커 취소가 확인된 뒤에만 로컬 상태를 제거한다.
    /// emit은 호출자가 상황(교체/수동매도/추가매수)에 맞게 처리한다.
    async fn cancel_reservation_internal(&self, code: &str) -> Result<Option<Reservation>, String> {
        let Some(r) = self.reservations.read().unwrap().get(code).cloned() else {
            return Ok(None);
        };
        let (intent_id, _) = self.record_manual_order_intent(
            code,
            LedgerSide::Sell,
            LedgerOrderType::Cancel,
            r.qty.max(1),
            Some(r.target_price),
        )?;
        match self.broker.cancel_order(code, &r.order_no, &r.org_no).await {
            Ok(ack) => {
                self.record_broker_ack(&intent_id, &ack, LedgerOrderStatus::Filled);
                self.reservations.write().unwrap().remove(code);
                Ok(Some(r))
            }
            Err(error) => {
                self.record_broker_error(
                    &intent_id,
                    &error.to_string(),
                    !error.is_confirmed_order_rejection(),
                );
                Err(error.to_string())
            }
        }
    }

    /// 매수 성공 시 걸려 있던 예약을 취소한다 (평단이 바뀌어 목표가가 무의미해짐).
    async fn after_buy_success(&self, code: &str) {
        // 새로 산 포지션은 평단이 바뀌었으니 손절 잠금을 풀어 다시 감시한다.
        // (직전 손절 매도 체결이 잔고에 반영되기 전 재매수하는 경합도 이걸로 커버)
        self.stop_loss_lock.lock().unwrap().remove(code);
        match self.cancel_reservation_internal(code).await {
            Ok(Some(r)) => {
                let reason =
                    Some("추가 매수로 예약 매도가 취소되었습니다. 다시 설정하세요.".to_string());
                self.emit(
                    "reservation",
                    &reservation_info(code, &r, "cancelled", reason),
                );
            }
            Ok(None) => {}
            Err(error) => self.emit(
                "engine-error",
                &format!("추가 매수 뒤 기존 예약 주문 취소 실패({code}): {error}"),
            ),
        }
    }

    /// 매도 체결 시 예약 잔량을 차감하고, 소진되면 예약을 제거한다.
    /// 반환: 프론트로 emit할 예약 상태 변화 (예약이 없으면 None).
    fn on_sell_fill(&self, code: &str, filled_qty: u64) -> Option<ReservationInfo> {
        let mut map = self.reservations.write().unwrap();
        let remaining = {
            let r = map.get_mut(code)?;
            r.qty = r.qty.saturating_sub(filled_qty);
            r.qty
        };
        if remaining == 0 {
            let r = map.remove(code)?;
            Some(reservation_info(code, &r, "filled", None))
        } else {
            // 부분 체결 — 남은 수량으로 대기 상태 유지
            let r = map.get(code)?;
            Some(reservation_info(code, r, "waiting", None))
        }
    }

    /// 주문·체결 직후 잠시 뒤 잔고 갱신 (체결통보와 별개의 안전망).
    /// 이미 예약된 갱신이 있으면 합류해 REST 호출(1회당 GET 2건)이 리미터 큐에 쌓이지 않게 한다.
    fn schedule_account_refresh(&self) {
        if self.refresh_pending.swap(true, Ordering::SeqCst) {
            return;
        }
        let Some(engine) = self.weak.upgrade() else {
            self.refresh_pending.store(false, Ordering::SeqCst);
            return;
        };
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(700)).await;
            // 대기 중 도착한 체결통보가 새 예약을 걸 수 있도록 조회 전에 해제
            engine.refresh_pending.store(false, Ordering::SeqCst);
            engine.refresh_account().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::{
        BrokerFill, BrokerMarketDay, BrokerOpenOrder, BrokerOrderStatus, OrderAck,
    };
    use crate::error::{AppError, AppResult};
    use crate::mock::MockBroker;
    use std::sync::atomic::AtomicUsize;

    fn test_automation_now() -> i64 {
        let date = crate::util::now_kst().format("%Y%m%d").to_string();
        crate::util::kst_str_to_fake_epoch(&date, "100000").unwrap()
    }

    fn test_reversal_decision() -> ValidatedDecision {
        validate_decision(
            185_000,
            &crate::types::ModelDecision {
                market_regime: MarketRegime::Range,
                decision_summary_ko: "지지 반응 시험".into(),
                scenarios: vec![crate::types::ModelScenario {
                    product: ProductKind::Leverage,
                    setup_type: SetupType::Reversal,
                    reference_price: 184_800,
                    confirmation_price: 185_200,
                    invalidation_price: 184_600,
                    target_return_pct: 0.3,
                    rationale_ko: "반복 지지 뒤 거래량 회복".into(),
                }],
            },
        )
        .unwrap()
    }

    #[test]
    fn 응답적용_seed는_입력뒤_정확한_체결을_순서대로_재생한다() {
        let decision = test_reversal_decision();
        let scenario = &decision.scenarios[0];
        let bars = vec![
            Candle {
                time: 120,
                open: 185_000.0,
                high: 185_100.0,
                low: 184_700.0,
                close: 185_000.0,
                volume: 100.0,
            },
            Candle {
                time: 180,
                open: 185_000.0,
                high: 185_100.0,
                low: 184_900.0,
                close: 185_000.0,
                volume: 100.0,
            },
        ];
        let input_indicators = chart_image::indicator_payload(&bars, 210);

        let before_new_touch = seed_scenario_from_latest_market(
            scenario,
            185_000,
            210,
            &bars,
            &input_indicators.one_minute,
            150,
            &[],
            &[],
        );
        assert_eq!(before_new_touch.status, crate::types::ScenarioStatus::Armed);
        assert_eq!(before_new_touch.reference_observed_at, None);

        // reset epoch가 분 경계로 절삭돼도 그 bucket 전체를 신뢰하지 않는다.
        let exact_boundary_without_tick = seed_scenario_from_latest_market(
            scenario,
            185_000,
            210,
            &bars,
            &input_indicators.one_minute,
            120,
            &[],
            &[],
        );
        assert_eq!(exact_boundary_without_tick.reference_observed_at, None);

        let mut sixteen_bars_without_recent_touch = vec![Candle {
            time: 1,
            open: 185_000.0,
            high: 185_100.0,
            low: 184_700.0,
            close: 185_000.0,
            volume: 100.0,
        }];
        sixteen_bars_without_recent_touch.extend((2..=17).map(|time| Candle {
            time,
            open: 185_000.0,
            high: 185_100.0,
            low: 184_900.0,
            close: 185_000.0,
            volume: 100.0,
        }));
        assert_eq!(
            reference_observed_at(scenario, &sixteen_bars_without_recent_touch, 0),
            None
        );

        let tick = |sequence: u64, epoch: i64, price: u64| TradeTick {
            sequence,
            price,
            volume: 1,
            at: Duration::from_secs(sequence),
            epoch,
        };
        assert!(needs_reset_minute_tick_recovery(
            &input_indicators.one_minute,
            150
        ));
        assert!(needs_reset_minute_tick_recovery(
            &input_indicators.one_minute,
            120
        ));
        let exact_boundary_post_reset_touch = [tick(1, 120, 184_800)];
        let exact_boundary_recovered = seed_scenario_from_latest_market(
            scenario,
            185_000,
            210,
            &bars,
            &input_indicators.one_minute,
            120,
            &exact_boundary_post_reset_touch,
            &[],
        );
        assert_eq!(exact_boundary_recovered.reference_observed_at, Some(120));

        let post_reset_pre_input_touch = [tick(1, 160, 184_800)];
        let recovered_same_minute_touch = seed_scenario_from_latest_market(
            scenario,
            185_000,
            210,
            &bars,
            &input_indicators.one_minute,
            150,
            &post_reset_pre_input_touch,
            &[],
        );
        assert_eq!(recovered_same_minute_touch.reference_observed_at, Some(160));
        assert_eq!(
            recovered_same_minute_touch.status,
            crate::types::ScenarioStatus::Armed
        );

        let touched = [tick(1, 220, 184_800)];
        let after_new_touch = seed_scenario_from_latest_market(
            scenario,
            185_000,
            230,
            &bars,
            &input_indicators.one_minute,
            150,
            &[],
            &touched,
        );
        assert_eq!(after_new_touch.reference_observed_at, Some(220));

        let touched_then_confirmed = [tick(1, 220, 184_800), tick(2, 221, 185_300)];
        let missed = seed_scenario_from_latest_market(
            scenario,
            185_000,
            230,
            &bars,
            &input_indicators.one_minute,
            150,
            &[],
            &touched_then_confirmed,
        );
        assert_eq!(missed.status, crate::types::ScenarioStatus::Missed);

        let touched_confirmed_then_invalidated = [
            tick(1, 220, 184_800),
            tick(2, 221, 185_300),
            tick(3, 222, 184_500),
        ];
        let recovered_invalidated = seed_scenario_from_latest_market(
            scenario,
            185_000,
            230,
            &bars,
            &input_indicators.one_minute,
            150,
            &[],
            &touched_confirmed_then_invalidated,
        );
        assert_eq!(
            recovered_invalidated.status,
            crate::types::ScenarioStatus::Invalidated
        );

        let invalidated = seed_scenario_from_latest_market(
            scenario,
            184_500,
            232,
            &bars,
            &input_indicators.one_minute,
            150,
            &[],
            &[],
        );
        assert_eq!(
            invalidated.status,
            crate::types::ScenarioStatus::Invalidated
        );

        // 입력 형성봉의 기존 고가가 이미 C 위여도 cursor 이후 C 재통과를 놓치지 않는다.
        let continuation = validate_decision(
            185_000,
            &crate::types::ModelDecision {
                market_regime: MarketRegime::Uptrend,
                decision_summary_ko: "재통과 확인".into(),
                scenarios: vec![crate::types::ModelScenario {
                    product: ProductKind::Leverage,
                    setup_type: SetupType::Continuation,
                    reference_price: 185_100,
                    confirmation_price: 185_300,
                    invalidation_price: 184_500,
                    target_return_pct: 0.3,
                    rationale_ko: "입력 뒤 확인가 재통과".into(),
                }],
            },
        )
        .unwrap();
        let mut high_already_above = bars.clone();
        high_already_above[1].high = 185_400.0;
        let continuation_input = chart_image::indicator_payload(&high_already_above, 210);
        let crossed_and_recovered = [tick(4, 223, 185_300), tick(5, 224, 185_000)];
        let recovered_missed = seed_scenario_from_latest_market(
            &continuation.scenarios[0],
            185_000,
            230,
            &high_already_above,
            &continuation_input.one_minute,
            150,
            &[],
            &crossed_and_recovered,
        );
        assert_eq!(
            recovered_missed.status,
            crate::types::ScenarioStatus::Missed
        );
    }

    #[test]
    fn 체결_journal은_입력순번_이하를_제외하고_유실시_안전폐기한다() {
        let mut journal = AutomationTradeJournal::default();
        let marker = journal.marker(3, 100);
        for (sequence, price) in [(100, 185_300), (99, 184_500), (101, 185_300)] {
            journal.push(TradeTick {
                sequence,
                price,
                volume: 1,
                at: Duration::from_secs(sequence),
                epoch: 1_000 + sequence as i64,
            });
        }
        let replayed = journal.after(marker).unwrap();
        assert_eq!(
            replayed
                .iter()
                .map(|tick| tick.sequence)
                .collect::<Vec<_>>(),
            vec![101]
        );

        let mut overflowed = AutomationTradeJournal::default();
        let old_marker = overflowed.marker(3, 0);
        for sequence in 1..=(AUTOMATION_TRADE_JOURNAL_CAPACITY as u64 + 1) {
            overflowed.push(TradeTick {
                sequence,
                price: 185_000,
                volume: 1,
                at: Duration::from_secs(sequence),
                epoch: 1_000 + sequence as i64,
            });
        }
        assert!(overflowed.after(old_marker).is_err());

        let mut reconnected = AutomationTradeJournal::default();
        reconnected.push(TradeTick {
            sequence: 900,
            price: 184_700,
            volume: 1,
            at: Duration::from_secs(1),
            epoch: 1_000,
        });
        reconnected.mark_reset();
        reconnected.push(TradeTick {
            sequence: 1,
            price: 184_800,
            volume: 1,
            at: Duration::from_secs(2),
            epoch: 1_010,
        });
        let reconnect_marker = reconnected.marker(4, 1);
        assert_eq!(
            reconnected
                .since_reset_through(reconnect_marker)
                .unwrap()
                .iter()
                .map(|tick| tick.sequence)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn 모델출력_분류는_skip_invalid_missed_invalidated를_구분한다() {
        let skip = validate_decision(
            185_000,
            &crate::types::ModelDecision {
                market_regime: MarketRegime::Unclear,
                decision_summary_ko: "시간축 충돌".into(),
                scenarios: vec![],
            },
        )
        .unwrap();
        assert_eq!(
            initial_decision_status(true, &skip, &[]),
            LedgerDecisionStatus::Skipped
        );

        let invalid = validate_decision(
            185_000,
            &crate::types::ModelDecision {
                market_regime: MarketRegime::Range,
                decision_summary_ko: "잘못된 가격 순서".into(),
                scenarios: vec![crate::types::ModelScenario {
                    product: ProductKind::Leverage,
                    setup_type: SetupType::Continuation,
                    reference_price: 184_900,
                    confirmation_price: 184_800,
                    invalidation_price: 184_700,
                    target_return_pct: 0.3,
                    rationale_ko: "의도적인 오류".into(),
                }],
            },
        )
        .unwrap();
        assert_eq!(
            initial_decision_status(true, &invalid, &[]),
            LedgerDecisionStatus::Invalid
        );

        let reversal = test_reversal_decision();
        assert_eq!(
            initial_decision_status(
                true,
                &reversal,
                &[ScenarioSeed::missed(ProductKind::Leverage, Some(100))]
            ),
            LedgerDecisionStatus::Missed
        );
        assert_eq!(
            initial_decision_status(
                true,
                &reversal,
                &[ScenarioSeed::invalidated(ProductKind::Leverage, Some(100))]
            ),
            LedgerDecisionStatus::Invalidated
        );
        assert_eq!(
            initial_decision_status(false, &reversal, &[]),
            LedgerDecisionStatus::Discarded
        );
    }

    #[test]
    fn 장부_해시는_sha256_소문자_16진수다() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    fn test_engine(broker: Arc<dyn Broker>, settings: Settings) -> Arc<Engine> {
        test_engine_with_emit(broker, settings, None)
    }

    fn test_engine_with_emit(
        broker: Arc<dyn Broker>,
        settings: Settings,
        emit_fn: Option<EmitFn>,
    ) -> Arc<Engine> {
        let ledger = Arc::new(Ledger::open_in_memory().unwrap());
        let shadow_entry_executor =
            ShadowEntryExecutor::new(Arc::new(BrokerShadowCashSource::new(Arc::clone(&broker))));
        Arc::new_cyclic(|weak| Engine {
            active: AtomicBool::new(true),
            retirement_gate: Mutex::new(()),
            runtime_id: unique_id("test-engine"),
            runtime_generation: ENGINE_GENERATION_SEQUENCE.fetch_add(1, Ordering::Relaxed),
            settings: RwLock::new(settings),
            broker,
            shadow_entry_executor,
            quotes: RwLock::new(HashMap::new()),
            account: RwLock::new(AccountSnapshot {
                cash: 0,
                positions: Vec::new(),
            }),
            reservations: RwLock::new(HashMap::new()),
            stop_loss_lock: Mutex::new(HashMap::new()),
            market_history: MarketHistory::new(),
            automation: Mutex::new(AutomationRuntime::new(PersistedAutomation::default(), None)),
            automation_market_gate: tokio::sync::Mutex::new(()),
            automation_trade_journal: Mutex::new(AutomationTradeJournal::default()),
            automation_feed_tx: Mutex::new(None),
            market_day: RwLock::new(PersistedMarketDayState {
                date: market_date_info(test_automation_now()).unwrap().0,
                status: MarketDayStatus::Open,
                message: None,
                last_attempt_at: test_automation_now(),
            }),
            market_day_refresh_gate: tokio::sync::Mutex::new(()),
            shadow: Mutex::new(None),
            ledger,
            scenario_history_fingerprint: Mutex::new(None),
            order_actor: tokio::sync::Mutex::new(()),
            fill_ingest_lock: Mutex::new(()),
            fill_reconcile_pending: Mutex::new(HashMap::new()),
            trade_sequence: AtomicU64::new(0),
            connected: AtomicBool::new(true),
            automation_feed_seen_connection: AtomicBool::new(true),
            automation_feed_generation: AtomicU64::new(0),
            automation_feed_reset_epoch: AtomicI64::new(0),
            feed_reconnect: Arc::new(tokio::sync::Notify::new()),
            feed_half_dead_since: AtomicI64::new(0),
            feed_watchdog_last_fired: AtomicI64::new(0),
            auto_flatten_pending: AtomicBool::new(false),
            entry_reconcile_pending: AtomicBool::new(false),
            exit_reconcile_pending: AtomicBool::new(false),
            last_flatten_day: AtomicI64::new(-1),
            last_error: Mutex::new(String::new()),
            refresh_pending: AtomicBool::new(false),
            account_gen: AtomicU64::new(1),
            account_refreshed_gen: AtomicU64::new(0),
            automation_now_override: AtomicI64::new(test_automation_now()),
            emit_fn,
            weak: weak.clone(),
        })
    }

    /// 신선한 웹소켓 시세를 캐시에 심는다 (버튼 클릭 경로 테스트용)
    fn seed_quote(engine: &Engine, ask1: f64) {
        engine.quotes.write().unwrap().insert(
            "0193T0".into(),
            Quote {
                code: "0193T0".into(),
                price: 10_500.0,
                change_rate: 0.0,
                ask1,
                bid1: 9_995.0,
                ask1_qty: 100_000,
                bid1_qty: 100_000,
                volume: 0.0,
                trade_sequence: 1,
                received_at_micros: crate::util::monotonic_now()
                    .as_micros()
                    .try_into()
                    .unwrap_or(u64::MAX),
                trade_ts: now_kst_fake_epoch(),
                book_ts: now_kst_fake_epoch(),
            },
        );
    }

    #[tokio::test]
    async fn llm_요청_태스크를_시작하기_전에_호출_슬롯을_내구화한다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        let slot = test_automation_now();
        let next = schedule::following_slot(slot);
        engine.automation.lock().unwrap().set_mode_after_cleanup(
            ControlMode::Auto,
            Some(slot),
            None,
        );
        assert!(engine
            .automation
            .lock()
            .unwrap()
            .begin_analysis(slot, next)
            .is_some());
        engine.persist_automation_required().unwrap();

        let saved = engine
            .ledger
            .get_runtime_state::<PersistedAutomationBundle>(AUTOMATION_BUNDLE_STATE_KEY)
            .unwrap()
            .expect("OpenAI 태스크 시작 전 저장된 런타임");
        assert_eq!(saved.automation.last_decision_slot, Some(slot));
        assert_eq!(saved.automation.next_decision_at, next);
    }

    #[tokio::test]
    async fn 런타임_재동기화는_수동_idle과_빈_캐시로_되돌린다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        let now = test_automation_now();
        engine.automation.lock().unwrap().set_mode_after_cleanup(
            ControlMode::Auto,
            Some(now),
            None,
        );
        engine.persist_automation_required().unwrap();
        engine
            .ledger
            .set_runtime_state(
                AUTO_HANDOFF_STATE_KEY,
                &PendingAutoHandoff {
                    target_mode: ControlMode::Manual,
                    requested_at: now,
                },
                now,
            )
            .unwrap();
        engine
            .ledger
            .set_runtime_state(
                "manual_trade:0193T0",
                &serde_json::json!({ "stale": true }),
                now,
            )
            .unwrap();
        seed_quote(&engine, 10_005.0);

        engine.reset_runtime_for_resync().await.unwrap();

        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.mode(), ControlMode::Manual);
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::Idle);
        assert!(runtime.position().is_none());
        drop(runtime);
        assert!(engine.quotes.read().unwrap().is_empty());
        assert_eq!(
            engine.ledger.get_control_mode().unwrap(),
            Some(LedgerControlMode::Manual)
        );
        assert!(engine
            .ledger
            .get_runtime_state::<PendingAutoHandoff>(AUTO_HANDOFF_STATE_KEY)
            .unwrap()
            .is_none());
        assert!(engine
            .ledger
            .get_runtime_state::<serde_json::Value>("manual_trade:0193T0")
            .unwrap()
            .is_none());
    }

    #[test]
    fn 런타임_재동기화는_실제_미체결_주문을_표시하고_거부한다() {
        let message = active_open_order_summary(&[BrokerOpenOrder {
            order_no: "0014024200".into(),
            original_order_no: String::new(),
            org_no: "12345".into(),
            code: "0193T0".into(),
            side: Side::Sell,
            ordered_qty: 10,
            filled_qty: 0,
            cancelable_qty: 10,
            price: 10_500.0,
            ordered_at: test_automation_now(),
        }])
        .expect("미체결 주문 안내");

        assert!(message.contains("미체결 주문 1건"));
        assert!(message.contains("0193T0 12345/0014024200 10주"));
    }

    fn seed_shadow_quotes(engine: &Engine, entry_ask_qty: u64, exit_bid_qty: u64) {
        let now = engine.automation_now();
        let mut quotes = engine.quotes.write().unwrap();
        for (code, price, ask_qty, bid_qty) in [
            ("000660", 185_000.0, 10_000, 10_000),
            ("0193T0", 10_000.0, entry_ask_qty, exit_bid_qty),
            ("0197X0", 10_000.0, 10_000, 10_000),
        ] {
            quotes.insert(
                code.into(),
                Quote {
                    code: code.into(),
                    price,
                    change_rate: 0.0,
                    ask1: price,
                    bid1: price - 5.0,
                    ask1_qty: ask_qty,
                    bid1_qty: bid_qty,
                    volume: 1.0,
                    trade_sequence: 1,
                    received_at_micros: crate::util::monotonic_now()
                        .as_micros()
                        .try_into()
                        .unwrap_or(u64::MAX),
                    trade_ts: now,
                    book_ts: now,
                },
            );
        }
    }

    #[tokio::test]
    async fn 호가이벤트는_llm용_마지막_체결량을_캐시에_보존한다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        seed_shadow_quotes(&engine, 10_000, 10_000);
        engine
            .quotes
            .write()
            .unwrap()
            .get_mut("000660")
            .unwrap()
            .volume = 321.0;
        let (tx, rx) = mpsc::channel(1);
        tx.send(FeedEvent::Book {
            code: "000660".into(),
            ask1: 185_100.0,
            bid1: 185_000.0,
            ask1_qty: 2_000,
            bid1_qty: 1_500,
            ts: test_automation_now(),
        })
        .await
        .unwrap();
        drop(tx);

        consume_feed(Arc::clone(&engine), rx).await;

        let cached = engine
            .quotes
            .read()
            .unwrap()
            .get("000660")
            .cloned()
            .unwrap();
        assert_eq!(cached.volume, 321.0);
        assert_eq!(MarketQuoteInput::from(&cached).last_trade_volume, 321.0);
    }

    #[tokio::test]
    async fn 체결_전에_도착한_호가는_버리지_않고_신규_엔트리로_병합한다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        let now = test_automation_now();
        let (tx, rx) = mpsc::channel(1);
        tx.send(FeedEvent::Book {
            code: "000660".into(),
            ask1: 185_100.0,
            bid1: 185_000.0,
            ask1_qty: 2_000,
            bid1_qty: 1_500,
            ts: now,
        })
        .await
        .unwrap();
        drop(tx);

        consume_feed(Arc::clone(&engine), rx).await;

        let cached = engine
            .quotes
            .read()
            .unwrap()
            .get("000660")
            .cloned()
            .unwrap();
        assert_eq!(cached.book_ts, now, "호가 시각이 유실되면 안 됨");
        assert_eq!(cached.ask1, 185_100.0);
        assert_eq!(cached.trade_ts, 0, "체결 수신 전이므로 체결 시각은 비어야 함");
        assert_eq!(cached.price, 0.0, "체결 전에는 가격을 만들어내지 않는다");
    }

    /// 000660 체결은 흐르는데 호가만 미수신인 관측 사례를 그대로 재현한다.
    fn make_half_dead(engine: &Engine, at: i64) {
        let mut quotes = engine.quotes.write().unwrap();
        let quote = quotes.get_mut("000660").unwrap();
        quote.trade_ts = at;
        quote.book_ts = 0;
    }

    #[test]
    fn half_dead_판정은_반대쪽_스트림이_신선할_때만_참이다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        seed_shadow_quotes(&engine, 10, 10);
        let now = test_automation_now();

        assert!(engine.automation_feed_half_dead(now).is_none(), "정상 상태");

        make_half_dead(&engine, now);
        let reason = engine.automation_feed_half_dead(now).unwrap();
        assert!(reason.contains("000660"), "{reason}");
        assert!(reason.contains("호가 미수신"), "{reason}");

        // 체결까지 낡으면 조용한 시장·완전 두절과 구분할 수 없으므로 미판정
        engine
            .quotes
            .write()
            .unwrap()
            .get_mut("000660")
            .unwrap()
            .trade_ts = now - QUOTE_FRESH_SECS - 5;
        assert!(engine.automation_feed_half_dead(now).is_none());

        // 완전 두절(둘 다 0)도 미판정 — 연결 판정과 신선도 검사 소관
        {
            let mut quotes = engine.quotes.write().unwrap();
            let quote = quotes.get_mut("000660").unwrap();
            quote.trade_ts = 0;
            quote.book_ts = 0;
        }
        assert!(engine.automation_feed_half_dead(now).is_none());
    }

    #[tokio::test]
    async fn 감시견은_반사망_60초_지속_시_한_번만_재접속을_요청한다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        seed_shadow_quotes(&engine, 10, 10);
        let start = test_automation_now();
        let no_permit = |engine: &Arc<Engine>| {
            let engine = Arc::clone(engine);
            async move {
                tokio::time::timeout(
                    std::time::Duration::from_millis(10),
                    engine.feed_reconnect.notified(),
                )
                .await
                .is_err()
            }
        };

        make_half_dead(&engine, start);
        engine.feed_watchdog_tick(start);
        make_half_dead(&engine, start + 30);
        engine.feed_watchdog_tick(start + 30);
        assert!(no_permit(&engine).await, "60초 전에는 요청하면 안 됨");

        make_half_dead(&engine, start + 60);
        engine.feed_watchdog_tick(start + 60);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                engine.feed_reconnect.notified(),
            )
            .await
            .is_ok(),
            "60초 지속이면 재접속을 요청해야 함"
        );

        // 발동 후 5분(FEED_WATCHDOG_MIN_GAP_SECS) 이내에는 재요청하지 않는다
        make_half_dead(&engine, start + 90);
        engine.feed_watchdog_tick(start + 90);
        make_half_dead(&engine, start + 200);
        engine.feed_watchdog_tick(start + 200);
        assert!(no_permit(&engine).await, "5분 이내 재요청 금지");

        // 간격이 지나면 다시 요청할 수 있다
        make_half_dead(&engine, start + 60 + FEED_WATCHDOG_MIN_GAP_SECS + 20);
        engine.feed_watchdog_tick(start + 60 + FEED_WATCHDOG_MIN_GAP_SECS + 20);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                engine.feed_reconnect.notified(),
            )
            .await
            .is_ok(),
            "간격 경과 후에는 다시 요청해야 함"
        );
    }

    #[tokio::test]
    async fn 감시견은_복구되면_지속_시간을_처음부터_다시_센다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        seed_shadow_quotes(&engine, 10, 10);
        let start = test_automation_now();

        make_half_dead(&engine, start);
        engine.feed_watchdog_tick(start);

        // 호가 복구 → 지속 시간 리셋
        {
            let mut quotes = engine.quotes.write().unwrap();
            let quote = quotes.get_mut("000660").unwrap();
            quote.trade_ts = start + 30;
            quote.book_ts = start + 30;
        }
        engine.feed_watchdog_tick(start + 30);

        // 다시 반사망 — 60초는 새로 세야 하므로 55초 지속으로는 미발동
        make_half_dead(&engine, start + 40);
        engine.feed_watchdog_tick(start + 40);
        make_half_dead(&engine, start + 95);
        engine.feed_watchdog_tick(start + 95);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                engine.feed_reconnect.notified(),
            )
            .await
            .is_err(),
            "복구 후에는 지속 시간을 새로 계산해야 함"
        );
    }

    #[tokio::test]
    async fn 피드_fifo_배리어는_앞선_체결의_journal_반영까지_기다린다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        let marker = engine
            .automation_trade_journal
            .lock()
            .unwrap()
            .marker(engine.automation_feed_generation.load(Ordering::SeqCst), 0);
        let (tx, rx) = mpsc::channel(4);
        *engine.automation_feed_tx.lock().unwrap() = Some(tx.clone());
        let consumer = tokio::spawn(consume_feed(Arc::clone(&engine), rx));
        let now = now_kst_fake_epoch();
        tx.send(FeedEvent::Quote(Quote {
            code: "000660".into(),
            price: 185_300.0,
            change_rate: 0.0,
            ask1: 185_300.0,
            bid1: 185_200.0,
            ask1_qty: 1_000,
            bid1_qty: 1_000,
            volume: 7.0,
            trade_sequence: 42,
            received_at_micros: 1,
            trade_ts: now,
            book_ts: now,
        }))
        .await
        .unwrap();

        engine
            .drain_automation_feed_queue(now.saturating_add(30))
            .await
            .unwrap();

        let replayed = engine
            .automation_trade_journal
            .lock()
            .unwrap()
            .after(marker)
            .unwrap();
        assert_eq!(
            replayed
                .iter()
                .map(|tick| tick.sequence)
                .collect::<Vec<_>>(),
            vec![42]
        );
        consumer.abort();
    }

    #[tokio::test]
    async fn 최초연결은_피드세대를_유지하고_재연결만_초기화한다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        seed_shadow_quotes(&engine, 10_000, 10_000);
        engine
            .automation_feed_seen_connection
            .store(false, Ordering::SeqCst);
        engine.automation_feed_generation.store(0, Ordering::SeqCst);
        engine
            .automation_feed_reset_epoch
            .store(0, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel(3);
        tx.send(FeedEvent::Conn(true)).await.unwrap();
        tx.send(FeedEvent::Conn(false)).await.unwrap();
        tx.send(FeedEvent::Conn(true)).await.unwrap();
        drop(tx);

        consume_feed(Arc::clone(&engine), rx).await;

        assert_eq!(engine.automation_feed_generation.load(Ordering::SeqCst), 2);
        assert!(engine.automation_feed_reset_epoch.load(Ordering::SeqCst) > 0);
        assert!(!engine.auto_quotes_fresh(test_automation_now()));
    }

    #[test]
    fn 자동시세_신선도오류는_종목과_체결호가원인을_구분한다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        let now = engine.automation_now();
        seed_shadow_quotes(&engine, 10_000, 10_000);

        assert!(engine.auto_quotes_fresh(now));
        assert_eq!(engine.auto_quotes_freshness_error(now), None);

        engine
            .quotes
            .write()
            .unwrap()
            .get_mut("0197X0")
            .unwrap()
            .book_ts = now - QUOTE_FRESH_SECS - 1;
        let stale_book = engine.auto_quotes_freshness_error(now).unwrap();
        assert!(stale_book.contains("0197X0"), "{stale_book}");
        assert!(stale_book.contains("호가 11초 경과"), "{stale_book}");
        assert!(!stale_book.contains("000660["), "{stale_book}");

        {
            let mut quotes = engine.quotes.write().unwrap();
            let inverse = quotes.get_mut("0197X0").unwrap();
            inverse.book_ts = now;
            inverse.trade_ts = 0;
        }
        let missing_trade = engine.auto_quotes_freshness_error(now).unwrap();
        assert!(missing_trade.contains("0197X0"), "{missing_trade}");
        assert!(missing_trade.contains("체결 미수신"), "{missing_trade}");

        engine.connected.store(false, Ordering::SeqCst);
        let disconnected = engine.auto_quotes_freshness_error(now).unwrap();
        assert!(disconnected.contains("웹소켓 연결 끊김"), "{disconnected}");
    }

    fn prepare_shadow_scenario(engine: &Engine) -> (u64, i64, Duration) {
        let decision = validate_decision(
            185_000,
            &crate::types::ModelDecision {
                market_regime: MarketRegime::Uptrend,
                decision_summary_ko: "상승 추세 돌파 시험".into(),
                scenarios: vec![crate::types::ModelScenario {
                    product: ProductKind::Leverage,
                    setup_type: SetupType::Continuation,
                    reference_price: 185_100,
                    confirmation_price: 185_300,
                    invalidation_price: 184_800,
                    target_return_pct: 0.3,
                    rationale_ko: "거래량을 동반한 상단 압축".into(),
                }],
            },
        )
        .unwrap();
        let scenario = &decision.scenarios[0];
        let applied_trigger = scenario.trigger_price;
        let now = engine.automation_now();
        let start = engine.monotonic_now();
        let mut runtime = engine.automation.lock().unwrap();
        runtime.set_mode_after_cleanup(ControlMode::Shadow, None, Some(1_000_000));
        let revision = runtime.begin_analysis(now, None).unwrap();
        let decision_id = unique_id("shadow-test-decision");
        let row_id = engine
            .ledger
            .record_decision(
                &NewDecision {
                    decision_id: decision_id.clone(),
                    session_id: None,
                    control_mode: LedgerControlMode::Shadow,
                    revision: revision as i64,
                    as_of_ts: now,
                    expires_at: now + 300,
                    underlying_price: 185_000.0,
                    status: LedgerDecisionStatus::Armed,
                    model: "test".into(),
                    prompt_version: "test".into(),
                    input_tokens: 0,
                    cached_input_tokens: 0,
                    cache_write_tokens: 0,
                    output_tokens: 0,
                    reasoning_tokens: 0,
                    latency_ms: 0,
                    input_hash: None,
                    chart_hash: None,
                    market_regime: Some(LedgerMarketRegime::Uptrend),
                    decision_summary_ko: Some("상승 추세 돌파 시험".into()),
                    error: None,
                    created_at: now,
                },
                &[NewDecisionScenario {
                    product: LedgerProductKind::Leverage,
                    setup_type: Some(LedgerSetupType::Continuation),
                    reference_price: Some(scenario.reference_price),
                    confirmation_price: Some(scenario.confirmation_price),
                    invalidation_price: Some(scenario.invalidation_price),
                    trigger_price: applied_trigger,
                    target_return_pct: 0.3,
                    rationale_ko: Some(scenario.rationale_ko.clone()),
                    status: LedgerScenarioStatus::Armed,
                    reference_observed_at: None,
                    terminal_reason: None,
                }],
            )
            .unwrap();
        let seeds = [ScenarioSeed::armed(ProductKind::Leverage, None)];
        assert!(runtime.accept_decision(
            revision,
            row_id,
            decision_id,
            &decision,
            AutomationDecisionStatus::Armed,
            &seeds,
            None,
            row_id,
            start,
            start + Duration::from_secs(300),
            now + 300,
        ));
        (applied_trigger, now, start)
    }

    fn arm_shadow_trigger(engine: &Engine) -> TriggeredScenario {
        let (applied_trigger, now, start) = prepare_shadow_scenario(engine);
        let mut runtime = engine.automation.lock().unwrap();
        let mut triggered = None;
        for (sequence, second) in [(1, 1), (2, 2), (3, 4), (4, 5)] {
            triggered = runtime.on_trade_tick(TradeTick {
                sequence,
                price: applied_trigger,
                volume: 1,
                at: start + Duration::from_secs(second),
                epoch: now + second as i64,
            });
            if triggered.is_some() {
                break;
            }
        }
        triggered.expect("3초·3틱 확인 뒤 섀도 시나리오가 확정되어야 함")
    }

    fn arm_auto_dual_trigger(engine: &Engine) -> TriggeredScenario {
        let decision = validate_decision(
            185_000,
            &crate::types::ModelDecision {
                market_regime: MarketRegime::Transition,
                decision_summary_ko: "양방향 추세 확인 시험".into(),
                scenarios: vec![
                    crate::types::ModelScenario {
                        product: ProductKind::Leverage,
                        setup_type: SetupType::Continuation,
                        reference_price: 185_100,
                        confirmation_price: 185_300,
                        invalidation_price: 184_800,
                        target_return_pct: 0.3,
                        rationale_ko: "상단 확인".into(),
                    },
                    crate::types::ModelScenario {
                        product: ProductKind::Inverse,
                        setup_type: SetupType::Continuation,
                        reference_price: 184_900,
                        confirmation_price: 184_700,
                        invalidation_price: 185_500,
                        target_return_pct: 0.2,
                        rationale_ko: "하단 확인".into(),
                    },
                ],
            },
        )
        .unwrap();
        let leverage_trigger = decision.scenarios[0].trigger_price;
        let inverse_trigger = decision.scenarios[1].trigger_price;
        let now = engine.automation_now();
        let start = engine.monotonic_now();
        let mut runtime = engine.automation.lock().unwrap();
        runtime.set_mode_after_cleanup(ControlMode::Auto, None, None);
        let revision = runtime.begin_analysis(now, None).unwrap();
        let decision_id = unique_id("auto-test-decision");
        let rows = [
            NewDecisionScenario {
                product: LedgerProductKind::Leverage,
                setup_type: Some(LedgerSetupType::Continuation),
                reference_price: Some(decision.scenarios[0].reference_price),
                confirmation_price: Some(decision.scenarios[0].confirmation_price),
                invalidation_price: Some(decision.scenarios[0].invalidation_price),
                trigger_price: leverage_trigger,
                target_return_pct: 0.3,
                rationale_ko: Some(decision.scenarios[0].rationale_ko.clone()),
                status: LedgerScenarioStatus::Armed,
                reference_observed_at: None,
                terminal_reason: None,
            },
            NewDecisionScenario {
                product: LedgerProductKind::Inverse,
                setup_type: Some(LedgerSetupType::Continuation),
                reference_price: Some(decision.scenarios[1].reference_price),
                confirmation_price: Some(decision.scenarios[1].confirmation_price),
                invalidation_price: Some(decision.scenarios[1].invalidation_price),
                trigger_price: inverse_trigger,
                target_return_pct: 0.2,
                rationale_ko: Some(decision.scenarios[1].rationale_ko.clone()),
                status: LedgerScenarioStatus::Armed,
                reference_observed_at: None,
                terminal_reason: None,
            },
        ];
        let row_id = engine
            .ledger
            .record_decision(
                &NewDecision {
                    decision_id: decision_id.clone(),
                    session_id: None,
                    control_mode: LedgerControlMode::Auto,
                    revision: revision as i64,
                    as_of_ts: now,
                    expires_at: now + 300,
                    underlying_price: 185_000.0,
                    status: LedgerDecisionStatus::Armed,
                    model: "test".into(),
                    prompt_version: "test".into(),
                    input_tokens: 0,
                    cached_input_tokens: 0,
                    cache_write_tokens: 0,
                    output_tokens: 0,
                    reasoning_tokens: 0,
                    latency_ms: 0,
                    input_hash: None,
                    chart_hash: None,
                    market_regime: Some(LedgerMarketRegime::Transition),
                    decision_summary_ko: Some("양방향 추세 확인 시험".into()),
                    error: None,
                    created_at: now,
                },
                &rows,
            )
            .unwrap();
        assert!(runtime.accept_decision(
            revision,
            row_id,
            decision_id,
            &decision,
            AutomationDecisionStatus::Armed,
            &[
                ScenarioSeed::armed(ProductKind::Leverage, None),
                ScenarioSeed::armed(ProductKind::Inverse, None),
            ],
            None,
            row_id,
            start,
            start + Duration::from_secs(300),
            now + 300,
        ));
        for (sequence, second) in [(1, 1), (2, 2), (3, 3), (4, 4)] {
            if let Some(triggered) = runtime.on_trade_tick(TradeTick {
                sequence,
                price: leverage_trigger,
                volume: 1,
                at: start + Duration::from_secs(second),
                epoch: now + second as i64,
            }) {
                return triggered;
            }
        }
        panic!("상단 3초·3틱 확인 뒤 레버리지 시나리오가 확정되어야 함");
    }

    #[test]
    fn recreated_engine_has_newer_runtime_generation() {
        let first_settings = Settings::default();
        let first = test_engine(Arc::new(MockBroker::new(&first_settings)), first_settings);
        let second_settings = Settings::default();
        let second = test_engine(Arc::new(MockBroker::new(&second_settings)), second_settings);

        assert!(second.runtime_generation > first.runtime_generation);
        assert_eq!(
            second.automation_snapshot().runtime_generation,
            second.runtime_generation
        );
    }

    /// 스캘핑 핵심 시나리오: 매수 체결 직후(지연 잔고 갱신 도착 전) 즉시 매도가 성공해야 한다
    #[tokio::test]
    async fn sell_right_after_buy_succeeds() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine.refresh_account().await; // 초기 예수금 캐시 적재

        let buy = engine.buy_max("0193T0").await;
        assert!(buy.ok, "매수 실패: {}", buy.message);
        assert!(buy.qty > 0);

        // 캐시에는 아직 포지션이 없다 — 지연 갱신(700ms)을 기다리지 않고 즉시 매도
        let sell = engine.sell_all("0193T0").await;
        assert!(sell.ok, "매도 실패: {}", sell.message);
        assert_eq!(sell.qty, buy.qty, "전량 매도여야 한다");
    }

    /// 매도 직후 재매수: 낡은 캐시의 잔돈이 아니라 매도 대금이 반영된 예수금으로 최대 수량이 잡혀야 한다
    #[tokio::test]
    async fn rebuy_right_after_sell_uses_fresh_cash() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine.refresh_account().await;

        let buy1 = engine.buy_max("0193T0").await;
        assert!(buy1.ok, "{}", buy1.message);
        let sell = engine.sell_all("0193T0").await;
        assert!(sell.ok, "{}", sell.message);

        let buy2 = engine.buy_max("0193T0").await;
        assert!(buy2.ok, "재매수 실패: {}", buy2.message);
        // 낡은 캐시(매수 후 잔돈)로 계산하면 한두 주만 사게 된다
        assert!(
            buy2.qty > buy1.qty / 2,
            "재매수 수량이 너무 적음 (낡은 예수금 캐시 사용 의심): {} vs 첫 매수 {}",
            buy2.qty,
            buy1.qty
        );
    }

    /// 신선한 웹소켓 시세와 계좌 캐시가 있으면 버튼 클릭 경로는 주문 외 REST 조회를 하지 않는다.
    /// 현재가와 관계없이 지금 체결 가능한 매도1호가를 기준으로 상한과 95% 수량을 계산한다.
    #[tokio::test]
    async fn cached_buy_places_order_without_extra_queries() {
        let broker = Arc::new(FastBuyBroker {
            account_calls: AtomicUsize::new(0),
            snapshot_calls: AtomicUsize::new(0),
            order: Mutex::new(None),
        });
        let settings = Settings::default();
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, settings);
        engine.refresh_account().await;
        seed_quote(&engine, 10_100.0);

        let result = engine.buy_max("0193T0").await;

        assert!(result.ok, "{}", result.message);
        assert_eq!(result.price, 10_400); // 10,100원 +3%를 ETF 호가단위로 내림
        assert_eq!(result.qty, 94); // 1,000,000원 × 95% / 매도1호가 10,100원
        assert_eq!(broker.account_calls.load(Ordering::SeqCst), 1);
        assert_eq!(broker.snapshot_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            broker.order.lock().unwrap().as_ref(),
            Some(&("0193T0".into(), 94, 10_400, true))
        );
    }

    struct FastBuyBroker {
        account_calls: AtomicUsize,
        snapshot_calls: AtomicUsize,
        order: Mutex<Option<(String, u64, u64, bool)>>,
    }

    #[async_trait::async_trait]
    impl Broker for FastBuyBroker {
        async fn candles_1m(&self, _code: &str) -> AppResult<Vec<Candle>> {
            Ok(Vec::new())
        }
        async fn account(&self) -> AppResult<AccountSnapshot> {
            self.account_calls.fetch_add(1, Ordering::SeqCst);
            Ok(AccountSnapshot {
                cash: 1_000_000,
                positions: Vec::new(),
            })
        }
        async fn snapshot(&self, _code: &str) -> AppResult<Quote> {
            self.snapshot_calls.fetch_add(1, Ordering::SeqCst);
            Err(AppError::Config("신선한 캐시가 있어 호출되면 안 됨".into()))
        }
        async fn max_buy_qty(&self, _code: &str, _limit_price: u64) -> AppResult<u64> {
            Err(AppError::Config("빠른 경로에서 호출되면 안 됨".into()))
        }
        async fn place_buy(
            &self,
            code: &str,
            qty: u64,
            price: u64,
            ioc: bool,
        ) -> AppResult<OrderAck> {
            *self.order.lock().unwrap() = Some((code.into(), qty, price, ioc));
            Ok(OrderAck {
                order_no: "TEST-BUY".into(),
                org_no: "TEST-ORG".into(),
                message: "주문 접수".into(),
            })
        }
        async fn place_sell_market(&self, _code: &str, _qty: u64) -> AppResult<OrderAck> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn place_sell_limit(&self, _c: &str, _q: u64, _p: u64) -> AppResult<OrderAck> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn cancel_order(&self, _c: &str, _o: &str, _g: &str) -> AppResult<OrderAck> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn start_feed(
            &self,
            _codes: Vec<String>,
            _tx: mpsc::Sender<FeedEvent>,
            _reconnect: std::sync::Arc<tokio::sync::Notify>,
        ) -> AppResult<Vec<JoinHandle<()>>> {
            Ok(Vec::new())
        }
    }

    /// 잔고 조회 횟수만 세는 브로커 (합류 검증용)
    struct CountingBroker {
        account_calls: AtomicUsize,
        market_day_calls: AtomicUsize,
        market_open: AtomicBool,
        market_error: AtomicBool,
        block_first_market_day: Option<Arc<tokio::sync::Notify>>,
    }

    struct ShadowPostCountingBroker {
        post_calls: AtomicUsize,
    }

    /// 시장가 청산 ACK 뒤 정확 주문상태가 보일 때만 재주문되는지 검증한다.
    struct ExactExitBroker {
        sell_calls: AtomicUsize,
        account_qty_before_post: u64,
        account_qty_after_post: u64,
        status: Mutex<Option<BrokerOrderStatus>>,
    }

    #[async_trait::async_trait]
    impl Broker for ExactExitBroker {
        async fn candles_1m(&self, _code: &str) -> AppResult<Vec<Candle>> {
            Ok(Vec::new())
        }

        async fn account(&self) -> AppResult<AccountSnapshot> {
            let qty = if self.sell_calls.load(Ordering::SeqCst) == 0 {
                self.account_qty_before_post
            } else {
                self.account_qty_after_post
            };
            Ok(AccountSnapshot {
                cash: 1_000_000,
                positions: (qty > 0)
                    .then(|| crate::types::Position {
                        code: "0193T0".into(),
                        qty,
                        avg_price: 10_005.0,
                        eval_pnl: 0.0,
                        pnl_rate: 0.0,
                    })
                    .into_iter()
                    .collect(),
            })
        }

        async fn snapshot(&self, _code: &str) -> AppResult<Quote> {
            Err(AppError::Config("정확 청산 테스트에서 미사용".into()))
        }

        async fn max_buy_qty(&self, _code: &str, _limit_price: u64) -> AppResult<u64> {
            Err(AppError::Config("정확 청산 테스트에서 미사용".into()))
        }

        async fn place_buy(
            &self,
            _code: &str,
            _qty: u64,
            _limit_price: u64,
            _ioc: bool,
        ) -> AppResult<OrderAck> {
            Err(AppError::Order("정확 청산 테스트에서 매수 금지".into()))
        }

        async fn place_sell_market(&self, _code: &str, _qty: u64) -> AppResult<OrderAck> {
            let call = self.sell_calls.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(OrderAck {
                order_no: format!("EXACT-EXIT-{call}"),
                org_no: "EXACT-ORG".into(),
                message: "시장가 접수".into(),
            })
        }

        async fn place_sell_limit(
            &self,
            _code: &str,
            _qty: u64,
            _limit_price: u64,
        ) -> AppResult<OrderAck> {
            Err(AppError::Order(
                "정확 청산 테스트에서 목표 주문 금지".into(),
            ))
        }

        async fn cancel_order(
            &self,
            _code: &str,
            _order_no: &str,
            _org_no: &str,
        ) -> AppResult<OrderAck> {
            Err(AppError::Order("정확 청산 테스트에서 취소 금지".into()))
        }

        async fn order_status(
            &self,
            trading_date: &str,
            order_no: &str,
        ) -> AppResult<Option<BrokerOrderStatus>> {
            Ok(self.status.lock().unwrap().clone().filter(|status| {
                status.trading_date == trading_date && status.order_no == order_no
            }))
        }

        async fn start_feed(
            &self,
            _codes: Vec<String>,
            _tx: mpsc::Sender<FeedEvent>,
            _reconnect: std::sync::Arc<tokio::sync::Notify>,
        ) -> AppResult<Vec<JoinHandle<()>>> {
            Ok(Vec::new())
        }
    }

    /// 주문번호 불명확 목표와 15:15 tombstone의 exact REST 조정용 브로커 더블.
    struct ReconcileSafetyBroker {
        account_qty: u64,
        fills: Mutex<Vec<BrokerFill>>,
        status: Mutex<Option<BrokerOrderStatus>>,
        sell_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Broker for ReconcileSafetyBroker {
        async fn candles_1m(&self, _code: &str) -> AppResult<Vec<Candle>> {
            Ok(Vec::new())
        }

        async fn account(&self) -> AppResult<AccountSnapshot> {
            Ok(AccountSnapshot {
                cash: 1_000_000,
                positions: (self.account_qty > 0)
                    .then(|| crate::types::Position {
                        code: "0193T0".into(),
                        qty: self.account_qty,
                        avg_price: 10_005.0,
                        eval_pnl: 0.0,
                        pnl_rate: 0.0,
                    })
                    .into_iter()
                    .collect(),
            })
        }

        async fn snapshot(&self, _code: &str) -> AppResult<Quote> {
            Err(AppError::Config("안전 조정 테스트에서 미사용".into()))
        }

        async fn max_buy_qty(&self, _code: &str, _limit_price: u64) -> AppResult<u64> {
            Err(AppError::Config("안전 조정 테스트에서 미사용".into()))
        }

        async fn place_buy(
            &self,
            _code: &str,
            _qty: u64,
            _limit_price: u64,
            _ioc: bool,
        ) -> AppResult<OrderAck> {
            Err(AppError::Order("안전 조정 테스트에서 매수 금지".into()))
        }

        async fn place_sell_market(&self, _code: &str, _qty: u64) -> AppResult<OrderAck> {
            let call = self.sell_calls.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(OrderAck {
                order_no: format!("SAFETY-EXIT-{call}"),
                org_no: "SAFETY-ORG".into(),
                message: "시장가 접수".into(),
            })
        }

        async fn place_sell_limit(
            &self,
            _code: &str,
            _qty: u64,
            _limit_price: u64,
        ) -> AppResult<OrderAck> {
            Err(AppError::Order(
                "안전 조정 테스트에서 목표 주문 금지".into(),
            ))
        }

        async fn cancel_order(
            &self,
            _code: &str,
            order_no: &str,
            _org_no: &str,
        ) -> AppResult<OrderAck> {
            Ok(OrderAck {
                order_no: order_no.into(),
                org_no: "SAFETY-ORG".into(),
                message: "취소 접수".into(),
            })
        }

        async fn today_fills(&self) -> AppResult<Vec<BrokerFill>> {
            Ok(self.fills.lock().unwrap().clone())
        }

        async fn order_status(
            &self,
            trading_date: &str,
            order_no: &str,
        ) -> AppResult<Option<BrokerOrderStatus>> {
            Ok(self.status.lock().unwrap().clone().filter(|status| {
                status.trading_date == trading_date && status.order_no == order_no
            }))
        }

        async fn start_feed(
            &self,
            _codes: Vec<String>,
            _tx: mpsc::Sender<FeedEvent>,
            _reconnect: std::sync::Arc<tokio::sync::Notify>,
        ) -> AppResult<Vec<JoinHandle<()>>> {
            Ok(Vec::new())
        }
    }

    /// Auto 인계 중 미체결 조회 실패가 소유권 제거보다 먼저 처리되는지 검증한다.
    struct HandoffOpenOrdersFailBroker {
        open_orders_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Broker for HandoffOpenOrdersFailBroker {
        async fn candles_1m(&self, _code: &str) -> AppResult<Vec<Candle>> {
            Ok(Vec::new())
        }

        async fn account(&self) -> AppResult<AccountSnapshot> {
            Ok(AccountSnapshot {
                cash: 1_000_000,
                positions: Vec::new(),
            })
        }

        async fn snapshot(&self, _code: &str) -> AppResult<Quote> {
            Err(AppError::Config("인계 실패 테스트에서 미사용".into()))
        }

        async fn max_buy_qty(&self, _code: &str, _limit_price: u64) -> AppResult<u64> {
            Err(AppError::Config("인계 실패 테스트에서 미사용".into()))
        }

        async fn place_buy(
            &self,
            _code: &str,
            _qty: u64,
            _limit_price: u64,
            _ioc: bool,
        ) -> AppResult<OrderAck> {
            Err(AppError::Order("인계 실패 테스트에서 매수 금지".into()))
        }

        async fn place_sell_market(&self, _code: &str, _qty: u64) -> AppResult<OrderAck> {
            Err(AppError::Order("인계 실패 테스트에서 매도 금지".into()))
        }

        async fn place_sell_limit(
            &self,
            _code: &str,
            _qty: u64,
            _limit_price: u64,
        ) -> AppResult<OrderAck> {
            Err(AppError::Order(
                "인계 실패 테스트에서 목표 주문 금지".into(),
            ))
        }

        async fn cancel_order(
            &self,
            _code: &str,
            _order_no: &str,
            _org_no: &str,
        ) -> AppResult<OrderAck> {
            Err(AppError::Order("인계 실패 테스트에서 취소 금지".into()))
        }

        async fn open_orders(&self) -> AppResult<Vec<BrokerOpenOrder>> {
            self.open_orders_calls.fetch_add(1, Ordering::SeqCst);
            Err(AppError::Kis("미체결 조회 실패".into()))
        }

        async fn start_feed(
            &self,
            _codes: Vec<String>,
            _tx: mpsc::Sender<FeedEvent>,
            _reconnect: std::sync::Arc<tokio::sync::Notify>,
        ) -> AppResult<Vec<JoinHandle<()>>> {
            Ok(Vec::new())
        }
    }

    /// Auto IOC의 누적 REST 체결과 목표 주문 수량을 검증하는 브로커 더블.
    struct AutoEntryBroker {
        final_fill_qty: u64,
        early_fill_qty: Option<u64>,
        fill_price: f64,
        buy_calls: AtomicUsize,
        today_fill_calls: AtomicUsize,
        requested_buy_qty: Mutex<Option<u64>>,
        orderable_cash_code: Mutex<Option<String>>,
        target_orders: Mutex<Vec<(String, u64, u64)>>,
    }

    /// ACK 또는 정확 주문상태가 끝내 보이지 않는 진입을 재현한다.
    struct UnresolvedEntryBroker {
        ack_success: bool,
        definitive_reject: bool,
        buy_calls: AtomicUsize,
        target_calls: AtomicUsize,
        exact_status_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Broker for UnresolvedEntryBroker {
        async fn candles_1m(&self, _code: &str) -> AppResult<Vec<Candle>> {
            Ok(Vec::new())
        }

        async fn account(&self) -> AppResult<AccountSnapshot> {
            Ok(AccountSnapshot {
                cash: 1_000_000,
                positions: Vec::new(),
            })
        }

        async fn snapshot(&self, _code: &str) -> AppResult<Quote> {
            Err(AppError::Config("불명확 진입 테스트에서 미사용".into()))
        }

        async fn max_buy_qty(&self, _code: &str, _limit_price: u64) -> AppResult<u64> {
            Err(AppError::Config("불명확 진입 테스트에서 미사용".into()))
        }

        async fn place_buy(
            &self,
            _code: &str,
            _qty: u64,
            _limit_price: u64,
            _ioc: bool,
        ) -> AppResult<OrderAck> {
            self.buy_calls.fetch_add(1, Ordering::SeqCst);
            if self.ack_success {
                Ok(OrderAck {
                    order_no: "UNRESOLVED-BUY".into(),
                    org_no: "UNRESOLVED-ORG".into(),
                    message: "IOC 접수".into(),
                })
            } else if self.definitive_reject {
                Err(AppError::Order("IOC 업무 거부".into()))
            } else {
                Err(AppError::Kis("IOC ACK 응답 유실".into()))
            }
        }

        async fn place_sell_market(&self, _code: &str, _qty: u64) -> AppResult<OrderAck> {
            Err(AppError::Order("불명확 진입 테스트에서 매도 금지".into()))
        }

        async fn place_sell_limit(
            &self,
            _code: &str,
            _qty: u64,
            _limit_price: u64,
        ) -> AppResult<OrderAck> {
            self.target_calls.fetch_add(1, Ordering::SeqCst);
            Err(AppError::Order(
                "불명확 진입 테스트에서 목표 주문 금지".into(),
            ))
        }

        async fn cancel_order(
            &self,
            _code: &str,
            _order_no: &str,
            _org_no: &str,
        ) -> AppResult<OrderAck> {
            Err(AppError::Order("불명확 진입 테스트에서 취소 금지".into()))
        }

        async fn open_orders(&self) -> AppResult<Vec<BrokerOpenOrder>> {
            Ok(Vec::new())
        }

        async fn today_fills(&self) -> AppResult<Vec<BrokerFill>> {
            Ok(Vec::new())
        }

        async fn order_status(
            &self,
            _trading_date: &str,
            _order_no: &str,
        ) -> AppResult<Option<BrokerOrderStatus>> {
            self.exact_status_calls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }

        async fn start_feed(
            &self,
            _codes: Vec<String>,
            _tx: mpsc::Sender<FeedEvent>,
            _reconnect: std::sync::Arc<tokio::sync::Notify>,
        ) -> AppResult<Vec<JoinHandle<()>>> {
            Ok(Vec::new())
        }
    }

    #[async_trait::async_trait]
    impl Broker for AutoEntryBroker {
        async fn candles_1m(&self, _code: &str) -> AppResult<Vec<Candle>> {
            Ok(Vec::new())
        }

        async fn account(&self) -> AppResult<AccountSnapshot> {
            let holding = self.buy_calls.load(Ordering::SeqCst) > 0 && self.final_fill_qty > 0;
            Ok(AccountSnapshot {
                cash: if holding { 50_000 } else { 1_000_000 },
                positions: holding
                    .then(|| crate::types::Position {
                        code: "0193T0".into(),
                        qty: self.final_fill_qty,
                        avg_price: self.fill_price,
                        eval_pnl: 0.0,
                        pnl_rate: 0.0,
                    })
                    .into_iter()
                    .collect(),
            })
        }

        async fn orderable_cash(&self, code: &str) -> AppResult<u64> {
            *self.orderable_cash_code.lock().unwrap() = Some(code.to_string());
            Ok(1_000_000)
        }

        async fn snapshot(&self, _code: &str) -> AppResult<Quote> {
            Err(AppError::Config("Auto 진입 테스트에서 미사용".into()))
        }

        async fn max_buy_qty(&self, _code: &str, _limit_price: u64) -> AppResult<u64> {
            Err(AppError::Order("Auto 진입 테스트에서 미사용".into()))
        }

        async fn place_buy(
            &self,
            code: &str,
            qty: u64,
            _limit_price: u64,
            ioc: bool,
        ) -> AppResult<OrderAck> {
            assert_eq!(code, "0193T0");
            assert!(ioc);
            self.buy_calls.fetch_add(1, Ordering::SeqCst);
            *self.requested_buy_qty.lock().unwrap() = Some(qty);
            Ok(OrderAck {
                order_no: "AUTO-BUY-1".into(),
                org_no: "AUTO-ORG".into(),
                message: "IOC 접수".into(),
            })
        }

        async fn place_sell_market(&self, _code: &str, _qty: u64) -> AppResult<OrderAck> {
            Err(AppError::Order("Auto 진입 테스트에서 미사용".into()))
        }

        async fn place_sell_limit(&self, code: &str, qty: u64, price: u64) -> AppResult<OrderAck> {
            self.target_orders
                .lock()
                .unwrap()
                .push((code.into(), qty, price));
            Ok(OrderAck {
                order_no: "AUTO-TARGET-1".into(),
                org_no: "AUTO-ORG".into(),
                message: "목표 접수".into(),
            })
        }

        async fn cancel_order(
            &self,
            _code: &str,
            _order_no: &str,
            _org_no: &str,
        ) -> AppResult<OrderAck> {
            Err(AppError::Order("Auto 진입 테스트에서 미사용".into()))
        }

        async fn open_orders(&self) -> AppResult<Vec<BrokerOpenOrder>> {
            Ok(Vec::new())
        }

        async fn today_fills(&self) -> AppResult<Vec<BrokerFill>> {
            if self.buy_calls.load(Ordering::SeqCst) == 0 || self.final_fill_qty == 0 {
                return Ok(Vec::new());
            }
            let call = self.today_fill_calls.fetch_add(1, Ordering::SeqCst);
            let qty = self
                .early_fill_qty
                .filter(|_| call < 3)
                .unwrap_or(self.final_fill_qty);
            Ok(vec![BrokerFill {
                fill_id: format!("daily:AUTO-BUY-1:{qty}:{}", self.fill_price),
                order_no: "AUTO-BUY-1".into(),
                original_order_no: String::new(),
                org_no: "AUTO-ORG".into(),
                code: "0193T0".into(),
                side: Side::Buy,
                qty,
                price: self.fill_price,
                filled_at: test_automation_now(),
            }])
        }

        async fn order_status(
            &self,
            trading_date: &str,
            order_no: &str,
        ) -> AppResult<Option<BrokerOrderStatus>> {
            if self.buy_calls.load(Ordering::SeqCst) == 0 || order_no != "AUTO-BUY-1" {
                return Ok(None);
            }
            let ordered_qty = self
                .requested_buy_qty
                .lock()
                .unwrap()
                .expect("매수 요청수량이 먼저 저장돼야 함");
            Ok(Some(BrokerOrderStatus {
                trading_date: trading_date.into(),
                order_no: order_no.into(),
                original_order_no: String::new(),
                org_no: "AUTO-ORG".into(),
                code: "0193T0".into(),
                side: Side::Buy,
                ordered_qty,
                filled_qty: self.final_fill_qty,
                cancelled_qty: ordered_qty.saturating_sub(self.final_fill_qty),
                remaining_qty: 0,
                rejected_qty: 0,
                avg_fill_price: if self.final_fill_qty > 0 {
                    self.fill_price
                } else {
                    0.0
                },
                ordered_at: test_automation_now(),
            }))
        }

        async fn start_feed(
            &self,
            _codes: Vec<String>,
            _tx: mpsc::Sender<FeedEvent>,
            _reconnect: std::sync::Arc<tokio::sync::Notify>,
        ) -> AppResult<Vec<JoinHandle<()>>> {
            Ok(Vec::new())
        }
    }

    fn auto_entry_broker(final_fill_qty: u64, early_fill_qty: Option<u64>) -> Arc<AutoEntryBroker> {
        Arc::new(AutoEntryBroker {
            final_fill_qty,
            early_fill_qty,
            fill_price: 10_005.0,
            buy_calls: AtomicUsize::new(0),
            today_fill_calls: AtomicUsize::new(0),
            requested_buy_qty: Mutex::new(None),
            orderable_cash_code: Mutex::new(None),
            target_orders: Mutex::new(Vec::new()),
        })
    }

    async fn run_auto_entry_case(
        broker: Arc<AutoEntryBroker>,
    ) -> (Arc<Engine>, Arc<AutoEntryBroker>) {
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        seed_shadow_quotes(&engine, 100_000, 100_000);
        let triggered = arm_auto_dual_trigger(&engine);
        Arc::clone(&engine).execute_real_entry(triggered).await;
        (engine, broker)
    }

    #[tokio::test]
    async fn auto_ioc_완전체결과_부분체결은_실제_etf_vwap으로_목표를_건다() {
        for (final_qty, early_qty) in [(95, None), (17, Some(2))] {
            let (engine, broker) =
                run_auto_entry_case(auto_entry_broker(final_qty, early_qty)).await;
            assert_eq!(broker.buy_calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                broker.orderable_cash_code.lock().unwrap().as_deref(),
                Some("0193T0"),
                "Auto 수량은 실제 선택된 ETF 기준 미수없는매수금액을 사용해야 한다"
            );
            assert_eq!(*broker.requested_buy_qty.lock().unwrap(), Some(95));

            let position = engine
                .automation
                .lock()
                .unwrap()
                .position()
                .cloned()
                .expect("체결 수량만큼 Auto 포지션이 생성돼야 함");
            assert_eq!(position.entry_qty, final_qty);
            assert_eq!(position.qty, final_qty);
            assert_eq!(position.avg_price, 10_005.0);
            assert_eq!(position.target_return_pct, 0.3);
            let expected_target = sell_target_price(10_005.0, 0.3, true);
            assert_eq!(position.target_price, expected_target);
            assert_eq!(
                broker.target_orders.lock().unwrap().as_slice(),
                &[("0193T0".into(), final_qty, expected_target)]
            );
        }
    }

    #[tokio::test]
    async fn auto_ioc_무체결은_목표주문과_포지션을_만들지_않는다() {
        let (engine, broker) = run_auto_entry_case(auto_entry_broker(0, None)).await;

        assert_eq!(broker.buy_calls.load(Ordering::SeqCst), 1);
        assert!(broker.target_orders.lock().unwrap().is_empty());
        let runtime = engine.automation.lock().unwrap();
        assert!(runtime.position().is_none());
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::Idle);
    }

    #[tokio::test]
    async fn auto_ioc_ack_성공도_정확_terminal이_없으면_pending을_해제하지_않는다() {
        let broker = Arc::new(UnresolvedEntryBroker {
            ack_success: true,
            definitive_reject: false,
            buy_calls: AtomicUsize::new(0),
            target_calls: AtomicUsize::new(0),
            exact_status_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        seed_shadow_quotes(&engine, 100_000, 100_000);
        let triggered = arm_auto_dual_trigger(&engine);

        Arc::clone(&engine).execute_real_entry(triggered).await;

        assert_eq!(broker.buy_calls.load(Ordering::SeqCst), 1);
        assert!(broker.exact_status_calls.load(Ordering::SeqCst) >= 8);
        assert_eq!(broker.target_calls.load(Ordering::SeqCst), 0);
        let mut runtime = engine.automation.lock().unwrap();
        assert!(runtime.pending_entry().is_some());
        assert!(runtime.position().is_none());
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::Suspended);
        runtime.entry_failed("테스트 조정 종료");
    }

    #[tokio::test]
    async fn auto_ioc_주문번호없는_불명확_ack는_빈조회로_pending을_해제하지_않는다() {
        let broker = Arc::new(UnresolvedEntryBroker {
            ack_success: false,
            definitive_reject: false,
            buy_calls: AtomicUsize::new(0),
            target_calls: AtomicUsize::new(0),
            exact_status_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        seed_shadow_quotes(&engine, 100_000, 100_000);
        let triggered = arm_auto_dual_trigger(&engine);

        Arc::clone(&engine).execute_real_entry(triggered).await;

        assert_eq!(broker.buy_calls.load(Ordering::SeqCst), 1);
        assert_eq!(broker.target_calls.load(Ordering::SeqCst), 0);
        let order = {
            let runtime = engine.automation.lock().unwrap();
            let pending = runtime.pending_entry().expect("pending이 유지돼야 함");
            engine
                .ledger
                .get_order(&pending.intent_id)
                .unwrap()
                .unwrap()
        };
        assert_eq!(order.status, LedgerOrderStatus::Unknown);
        assert!(order.broker_order_id.is_none());
        let mut runtime = engine.automation.lock().unwrap();
        assert!(runtime.pending_entry().is_some());
        assert!(runtime.position().is_none());
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::Suspended);
        runtime.entry_failed("테스트 조정 종료");
    }

    #[test]
    fn 구버전_유량제한_unknown은_조회유예와_무노출확인뒤_rejected로_복구한다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        let requested_at = test_automation_now();
        let intent_id = "legacy-rate-limited-entry";
        let trade_id = "legacy-rate-limited-trade";
        engine
            .ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: intent_id.into(),
                session_id: None,
                trade_id: Some(trade_id.into()),
                decision_id: None,
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                side: LedgerSide::Buy,
                order_type: LedgerOrderType::IocLimit,
                qty: 95,
                price: Some(10_300),
                requested_at,
            })
            .unwrap();
        assert!(engine
            .ledger
            .begin_order_dispatch(intent_id, requested_at)
            .unwrap());
        engine
            .ledger
            .record_order_ack(
                intent_id,
                &OrderAcknowledgement {
                    broker_order_id: None,
                    broker_org_no: None,
                    original_order_id: None,
                    status: LedgerOrderStatus::Unknown,
                    message: Some(
                        "KIS API 오류: HTTP 500 Internal Server Error: 초당 거래건수를 초과하였습니다."
                            .into(),
                    ),
                    acknowledged_at: requested_at,
                },
            )
            .unwrap();
        *engine.automation.lock().unwrap() = AutomationRuntime::new(
            PersistedAutomation {
                mode: ControlMode::Auto,
                session_id: None,
                position: None,
                pending_entry: Some(pending_entry(intent_id, trade_id)),
                shadow_cash: None,
                next_decision_at: None,
                last_decision_slot: None,
            },
            None,
        );

        // 조회 반영 유예 전에는 빈 조회만으로 미접수를 확정하지 않는다.
        assert!(engine
            .reconcile_pending_auto_entry_startup(
                &AccountSnapshot {
                    cash: 1_000_000,
                    positions: Vec::new(),
                },
                &[],
                &[],
            )
            .is_err());
        assert!(engine.automation.lock().unwrap().pending_entry().is_some());

        engine.automation_now_override.store(
            requested_at + LEGACY_RATE_LIMIT_RECONCILE_SECS,
            Ordering::SeqCst,
        );
        // 유예 뒤에도 기준수량과 다른 보유가 있으면 절대 자동 해제하지 않는다.
        assert!(engine
            .reconcile_pending_auto_entry_startup(
                &AccountSnapshot {
                    cash: 1_000_000,
                    positions: vec![crate::types::Position {
                        code: "0193T0".into(),
                        qty: 1,
                        avg_price: 10_000.0,
                        eval_pnl: 0.0,
                        pnl_rate: 0.0,
                    }],
                },
                &[],
                &[],
            )
            .is_err());
        assert!(engine.automation.lock().unwrap().pending_entry().is_some());

        engine
            .reconcile_pending_auto_entry_startup(
                &AccountSnapshot {
                    cash: 1_000_000,
                    positions: Vec::new(),
                },
                &[],
                &[],
            )
            .unwrap();

        let runtime = engine.automation.lock().unwrap();
        assert!(runtime.pending_entry().is_none());
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::Idle);
        drop(runtime);
        assert_eq!(
            engine.ledger.get_order(intent_id).unwrap().unwrap().status,
            LedgerOrderStatus::Rejected
        );
    }

    #[tokio::test]
    async fn auto_ioc_업무거부는_rejected로_종결하고_pending을_해제한다() {
        let broker = Arc::new(UnresolvedEntryBroker {
            ack_success: false,
            definitive_reject: true,
            buy_calls: AtomicUsize::new(0),
            target_calls: AtomicUsize::new(0),
            exact_status_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        seed_shadow_quotes(&engine, 100_000, 100_000);
        let triggered = arm_auto_dual_trigger(&engine);

        Arc::clone(&engine).execute_real_entry(triggered).await;

        assert_eq!(broker.buy_calls.load(Ordering::SeqCst), 1);
        assert_eq!(broker.exact_status_calls.load(Ordering::SeqCst), 0);
        assert_eq!(broker.target_calls.load(Ordering::SeqCst), 0);
        let runtime = engine.automation.lock().unwrap();
        assert!(runtime.pending_entry().is_none());
        assert!(runtime.position().is_none());
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::Idle);
        assert!(runtime
            .snapshot(
                &engine.runtime_id,
                engine.runtime_generation,
                &engine.settings.read().unwrap().auto_symbols,
                engine.monotonic_now(),
                MarketDayStatus::Open,
                None,
            )
            .error
            .as_deref()
            .is_some_and(|error| error.contains("확정 거부")));
    }

    #[tokio::test]
    async fn 양방향_oco_확정이_경합해도_실전_진입_post는_한_번뿐이다() {
        let broker = auto_entry_broker(95, None);
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        seed_shadow_quotes(&engine, 100_000, 100_000);
        let triggered = arm_auto_dual_trigger(&engine);

        tokio::join!(
            Arc::clone(&engine).execute_real_entry(triggered.clone()),
            Arc::clone(&engine).execute_real_entry(triggered),
        );

        assert_eq!(broker.buy_calls.load(Ordering::SeqCst), 1);
        assert_eq!(broker.target_orders.lock().unwrap().len(), 1);
        let snapshot = engine.automation_snapshot();
        assert!(snapshot.scenarios.iter().any(|scenario| {
            scenario.product == ProductKind::Inverse
                && scenario.status == crate::types::ScenarioStatus::CancelledByOco
        }));
    }

    #[tokio::test]
    async fn auto_진입_actor_대기중_슬롯이_만료되면_post없이_pending을_해제한다() {
        let broker = auto_entry_broker(95, None);
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        seed_shadow_quotes(&engine, 100_000, 100_000);
        let triggered = arm_auto_dual_trigger(&engine);

        let actor = engine.order_actor.lock().await;
        let entry_engine = Arc::clone(&engine);
        let entry = tokio::spawn(async move {
            entry_engine.execute_real_entry(triggered).await;
        });
        tokio::task::yield_now().await;
        engine
            .automation_now_override
            .store(test_automation_now() + 300, Ordering::SeqCst);
        drop(actor);
        entry.await.unwrap();

        assert_eq!(broker.buy_calls.load(Ordering::SeqCst), 0);
        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::Idle);
        assert!(runtime.pending_entry().is_none());
    }

    #[tokio::test]
    async fn auto_진입_actor_대기중_1515가_되면_post하지_않는다() {
        let broker = auto_entry_broker(95, None);
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        seed_shadow_quotes(&engine, 100_000, 100_000);
        let triggered = arm_auto_dual_trigger(&engine);

        let actor = engine.order_actor.lock().await;
        let entry_engine = Arc::clone(&engine);
        let entry = tokio::spawn(async move {
            entry_engine.execute_real_entry(triggered).await;
        });
        tokio::task::yield_now().await;
        let close = test_automation_now().div_euclid(86_400) * 86_400 + 15 * 3_600 + 15 * 60;
        engine
            .automation_now_override
            .store(close, Ordering::SeqCst);
        engine
            .account_refreshed_gen
            .store(engine.account_gen.load(Ordering::SeqCst), Ordering::SeqCst);
        drop(actor);
        entry.await.unwrap();

        assert_eq!(broker.buy_calls.load(Ordering::SeqCst), 0);
        let snapshot = engine.automation_snapshot();
        assert_eq!(snapshot.phase, crate::types::AutomationPhase::Idle);
        assert!(snapshot
            .error
            .as_deref()
            .is_some_and(|error| error.contains("15:15")));
    }

    #[tokio::test]
    async fn shadow_진입_actor_대기중_슬롯이_만료되면_가상포지션을_만들지_않는다() {
        let broker = Arc::new(ShadowPostCountingBroker {
            post_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.account.write().unwrap().cash = 1_000_000;
        seed_shadow_quotes(&engine, 10, 10);
        let triggered = arm_shadow_trigger(&engine);

        let actor = engine.order_actor.lock().await;
        let entry_engine = Arc::clone(&engine);
        let entry = tokio::spawn(async move {
            entry_engine.execute_shadow_entry(triggered).await;
        });
        tokio::task::yield_now().await;
        engine
            .automation_now_override
            .store(test_automation_now() + 300, Ordering::SeqCst);
        drop(actor);
        entry.await.unwrap();

        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);
        assert!(engine.shadow.lock().unwrap().is_none());
        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::Idle);
        assert!(runtime.position().is_none());
    }

    fn recovered_position(trade_id: &str, target_intent_id: Option<String>) -> OwnedPosition {
        OwnedPosition {
            product: ProductKind::Leverage,
            code: "0193T0".into(),
            entry_qty: 17,
            qty: 17,
            avg_price: 10_005.0,
            last_price: 10_005.0,
            target_return_pct: 0.3,
            target_price: sell_target_price(10_005.0, 0.3, true),
            first_fill_at: now_kst_fake_epoch(),
            exit_deadline: now_kst_fake_epoch() + AUTO_MAX_HOLD_SECS,
            target_order_no: None,
            target_org_no: None,
            target_intent_id,
            exit_order_no: None,
            exit_order_org_no: None,
            exit_intent_id: None,
            exit_requested_at: None,
            pending_exit_reason: None,
            exit_qty: 0,
            exit_value: 0.0,
            trade_id: trade_id.into(),
            shadow: false,
            profit_guard_armed: false,
        }
    }

    #[test]
    fn 깨진_본주_가격_틱은_oco_확인과_revision을_변경하지_않는다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        seed_shadow_quotes(&engine, 10, 10);
        let (_, now, start) = prepare_shadow_scenario(&engine);
        let revision = engine.automation.lock().unwrap().revision();

        for (index, price) in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY]
            .into_iter()
            .enumerate()
        {
            engine.handle_automation_quote(&Quote {
                code: "000660".into(),
                price,
                change_rate: 0.0,
                ask1: 185_100.0,
                bid1: 185_000.0,
                ask1_qty: 1_000,
                bid1_qty: 1_000,
                volume: 1.0,
                trade_sequence: index as u64 + 1,
                received_at_micros: start.as_micros() as u64 + index as u64 + 1,
                trade_ts: now + index as i64,
                book_ts: now + index as i64,
            });
        }

        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.revision(), revision);
        assert!(runtime.pending_entry().is_none());
        drop(runtime);
        let snapshot = engine.automation_snapshot();
        assert_eq!(snapshot.scenarios.len(), 1);
        assert_eq!(snapshot.scenarios[0].confirming_ticks, 0);
        assert_eq!(snapshot.scenarios[0].confirming_elapsed_ms, 0);
    }

    #[tokio::test]
    async fn 깨진_가격_틱은_auto_손절을_발동시키지_않는다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        // 첫 체결일을 다음 날로 두어 실행 시각과 무관하게 15:15·최대보유가 먼저
        // 걸리지 않게 한다. 이 테스트가 보는 것은 가격 기반 청산 판정뿐이다.
        let mut position = recovered_position("broken-price", None);
        position.first_fill_at = now_kst_fake_epoch() + 86_400;
        position.exit_deadline = position.first_fill_at + AUTO_MAX_HOLD_SECS;
        {
            let mut runtime = engine.automation.lock().unwrap();
            runtime.mark_holding(position.clone());
            runtime.set_mode_after_cleanup(ControlMode::Auto, None, None);
        }

        let tick = |price: f64| Quote {
            code: "0193T0".into(),
            price,
            change_rate: 0.0,
            ask1: 10_000.0,
            bid1: 9_995.0,
            ask1_qty: 100,
            bid1_qty: 100,
            volume: 10.0,
            trade_sequence: 100,
            // 단조시계를 쓰면 이 테스트가 프로세스 첫 호출일 때 0µs가 나와 틱이
            // 가격과 무관하게 걸러진다. 고정 양수로 그 경로를 배제한다.
            received_at_micros: 1_000_000,
            trade_ts: now_kst_fake_epoch(),
            book_ts: now_kst_fake_epoch(),
        };

        // 0·음수·NaN·무한대는 -100% 손절로 읽히면 안 되고 평단 기준가도 못 덮는다.
        for price in [0.0, -5.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            engine.handle_automation_quote(&tick(price));
            let runtime = engine.automation.lock().unwrap();
            assert_eq!(
                runtime.phase(),
                crate::types::AutomationPhase::Holding,
                "가격 {price} 틱이 자동 청산을 시작했다"
            );
            assert_eq!(
                runtime.position().unwrap().last_price,
                position.avg_price,
                "가격 {price} 틱이 마지막 체결가를 덮어썼다"
            );
        }

        // 대조군: 실제로 손절선을 깬 가격은 그대로 청산으로 이어져야 한다.
        engine.handle_automation_quote(&tick(position.avg_price * 0.99));
        assert_eq!(
            engine.automation.lock().unwrap().phase(),
            crate::types::AutomationPhase::ExitPending
        );
    }

    #[tokio::test]
    async fn 다음날_오전_복원된_auto_포지션은_최대보유보다_장마감이_우선한다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        let next_morning = test_automation_now();
        let mut position = recovered_position("overnight-auto", None);
        position.first_fill_at = next_morning - 86_400;
        position.exit_deadline = position.first_fill_at + AUTO_MAX_HOLD_SECS;
        {
            let mut runtime = engine.automation.lock().unwrap();
            runtime.set_mode_after_cleanup(ControlMode::Auto, None, None);
            runtime.mark_holding(position);
        }
        engine
            .automation_now_override
            .store(next_morning, Ordering::SeqCst);

        // 비동기 청산 POST는 actor에서 멈추고 우선순위 상태 전이만 검증한다.
        let actor = engine.order_actor.lock().await;
        engine.automation_tick().await;
        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::ExitPending);
        assert_eq!(runtime.exit_reason(), Some("market_close"));
        assert_eq!(
            runtime
                .position()
                .and_then(|position| position.pending_exit_reason.as_deref()),
            Some("market_close")
        );
        drop(runtime);
        drop(actor);
    }

    #[test]
    fn 다음날_오전의_불명확_auto_진입도_주문의도일_장마감으로_판정한다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        let next_morning = test_automation_now();
        let requested_at = next_morning - 86_400;
        let intent_id = "overnight-pending-entry".to_string();
        engine
            .ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: intent_id.clone(),
                session_id: None,
                trade_id: Some("overnight-pending-trade".into()),
                decision_id: None,
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                side: LedgerSide::Buy,
                order_type: LedgerOrderType::IocLimit,
                qty: 10,
                price: Some(10_300),
                requested_at,
            })
            .unwrap();
        *engine.automation.lock().unwrap() = AutomationRuntime::new(
            PersistedAutomation {
                mode: ControlMode::Auto,
                pending_entry: Some(PendingAutoEntry {
                    intent_id,
                    trade_id: "overnight-pending-trade".into(),
                    baseline_qty: 0,
                    product: ProductKind::Leverage,
                    target_return_pct: 0.3,
                }),
                ..PersistedAutomation::default()
            },
            None,
        );

        assert_eq!(engine.pending_entry_reference_at(), Some(requested_at));
        assert!(engine.pending_entry_market_close_due(next_morning));
    }

    #[test]
    fn 재시작은_저장된_auto_포지션_외의_보유와_미체결을_알수없는노출로_판정한다() {
        let settings = Settings::default();
        let mut owned = recovered_position("startup-owned", Some("target-intent".into()));
        owned.target_order_no = Some("OWNED-TARGET".into());
        owned.target_org_no = Some("OWNED-ORG".into());
        let account = AccountSnapshot {
            cash: 500_000,
            positions: vec![crate::types::Position {
                code: owned.code.clone(),
                qty: owned.qty,
                avg_price: owned.avg_price,
                eval_pnl: 0.0,
                pnl_rate: 0.0,
            }],
        };
        let target = BrokerOpenOrder {
            order_no: "OWNED-TARGET".into(),
            original_order_no: String::new(),
            org_no: "OWNED-ORG".into(),
            code: owned.code.clone(),
            side: Side::Sell,
            ordered_qty: owned.entry_qty,
            filled_qty: 0,
            cancelable_qty: owned.qty,
            price: owned.target_price as f64,
            ordered_at: owned.first_fill_at,
        };

        assert!(!Engine::has_unknown_auto_exposure(
            &settings,
            Some(&owned),
            &account,
            std::slice::from_ref(&target),
        ));

        let mut extra_position = account.clone();
        extra_position.positions.push(crate::types::Position {
            code: settings.auto_symbols.inverse.clone(),
            qty: 3,
            avg_price: 9_000.0,
            eval_pnl: 0.0,
            pnl_rate: 0.0,
        });
        assert!(Engine::has_unknown_auto_exposure(
            &settings,
            Some(&owned),
            &extra_position,
            std::slice::from_ref(&target),
        ));

        let mut extra_orders = vec![target];
        extra_orders.push(BrokerOpenOrder {
            order_no: "UNKNOWN-INVERSE-ORDER".into(),
            original_order_no: String::new(),
            org_no: "UNKNOWN-ORG".into(),
            code: settings.auto_symbols.inverse.clone(),
            side: Side::Buy,
            ordered_qty: 5,
            filled_qty: 0,
            cancelable_qty: 5,
            price: 9_000.0,
            ordered_at: owned.first_fill_at,
        });
        assert!(Engine::has_unknown_auto_exposure(
            &settings,
            Some(&owned),
            &account,
            &extra_orders,
        ));
    }

    fn arm_exact_exit(engine: &Engine, trade_id: &str) {
        let mut runtime = engine.automation.lock().unwrap();
        runtime.set_mode_after_cleanup(ControlMode::Auto, None, None);
        runtime.mark_holding(recovered_position(trade_id, None));
        assert!(runtime.begin_exit("stop_loss").is_some());
    }

    fn exact_exit_status(
        filled_qty: u64,
        cancelled_qty: u64,
        remaining_qty: u64,
        rejected_qty: u64,
    ) -> BrokerOrderStatus {
        let ordered_at = now_kst_fake_epoch();
        BrokerOrderStatus {
            trading_date: crate::util::now_kst().format("%Y%m%d").to_string(),
            order_no: "EXACT-EXIT-1".into(),
            original_order_no: String::new(),
            org_no: "EXACT-ORG".into(),
            code: "0193T0".into(),
            side: Side::Sell,
            ordered_qty: 17,
            filled_qty,
            cancelled_qty,
            remaining_qty,
            rejected_qty,
            avg_fill_price: if filled_qty > 0 { 10_000.0 } else { 0.0 },
            ordered_at,
        }
    }

    #[tokio::test]
    async fn 청산_ack_뒤_정확주문상태가_비어있으면_주문번호를_유지하고_재post하지_않는다() {
        let broker = Arc::new(ExactExitBroker {
            sell_calls: AtomicUsize::new(0),
            account_qty_before_post: 17,
            account_qty_after_post: 17,
            status: Mutex::new(None),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        arm_exact_exit(&engine, "exact-pending-trade");

        Arc::clone(&engine).execute_auto_exit("stop_loss").await;
        tokio::time::sleep(Duration::from_millis(2_250)).await;

        assert_eq!(broker.sell_calls.load(Ordering::SeqCst), 1);
        let runtime = engine.automation.lock().unwrap();
        assert_eq!(
            runtime
                .position()
                .and_then(|position| position.exit_order_no.as_deref()),
            Some("EXACT-EXIT-1")
        );
    }

    #[tokio::test]
    async fn 정확주문상태가_전량거부로_종결된_뒤에만_잔량을_한번_재주문한다() {
        let broker = Arc::new(ExactExitBroker {
            sell_calls: AtomicUsize::new(0),
            account_qty_before_post: 17,
            account_qty_after_post: 17,
            status: Mutex::new(Some(exact_exit_status(0, 0, 0, 17))),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        arm_exact_exit(&engine, "exact-rejected-trade");

        Arc::clone(&engine).execute_auto_exit("stop_loss").await;
        tokio::time::timeout(Duration::from_secs(10), async {
            while broker.sell_calls.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("정확한 전량거부 종결 뒤에는 잔량 주문이 이어져야 함");

        assert_eq!(broker.sell_calls.load(Ordering::SeqCst), 2);
        let runtime = engine.automation.lock().unwrap();
        assert_eq!(
            runtime
                .position()
                .and_then(|position| position.exit_order_no.as_deref()),
            Some("EXACT-EXIT-2")
        );
    }

    fn arm_exact_target_cancel(engine: &Engine) {
        let now = now_kst_fake_epoch();
        let intent_id = "exact-target-intent";
        engine
            .ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: intent_id.into(),
                session_id: None,
                trade_id: Some("exact-target-trade".into()),
                decision_id: None,
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                side: LedgerSide::Sell,
                order_type: LedgerOrderType::Limit,
                qty: 17,
                price: Some(sell_target_price(10_005.0, 0.3, true)),
                requested_at: now,
            })
            .unwrap();
        engine
            .ledger
            .record_order_ack(
                intent_id,
                &OrderAcknowledgement {
                    broker_order_id: Some("EXACT-TARGET-1".into()),
                    broker_org_no: Some("EXACT-ORG".into()),
                    original_order_id: None,
                    status: LedgerOrderStatus::Submitted,
                    message: None,
                    acknowledged_at: now,
                },
            )
            .unwrap();
        let mut position = recovered_position("exact-target-trade", Some(intent_id.into()));
        position.target_order_no = Some("EXACT-TARGET-1".into());
        position.target_org_no = Some("EXACT-ORG".into());
        let mut runtime = engine.automation.lock().unwrap();
        runtime.set_mode_after_cleanup(ControlMode::Auto, None, None);
        runtime.mark_holding(position);
        assert!(runtime.begin_exit("stop_loss").is_some());
    }

    #[tokio::test]
    async fn 목표취소도_정확주문상태가_비어있으면_시장가를_겹쳐_post하지_않는다() {
        let broker = Arc::new(ExactExitBroker {
            sell_calls: AtomicUsize::new(0),
            account_qty_before_post: 17,
            account_qty_after_post: 17,
            status: Mutex::new(None),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        arm_exact_target_cancel(&engine);

        engine.start_target_cancel_reconcile("stop_loss", "EXACT-TARGET-1".into());
        tokio::time::sleep(Duration::from_millis(2_250)).await;

        assert_eq!(broker.sell_calls.load(Ordering::SeqCst), 0);
        let runtime = engine.automation.lock().unwrap();
        assert_eq!(
            runtime
                .position()
                .and_then(|position| position.target_order_no.as_deref()),
            Some("EXACT-TARGET-1")
        );
    }

    #[tokio::test]
    async fn 목표취소_정확상태의_조직번호가_다르면_종결로_인정하지_않는다() {
        let mut wrong_org_status = exact_exit_status(0, 17, 0, 0);
        wrong_org_status.order_no = "EXACT-TARGET-1".into();
        wrong_org_status.org_no = "OTHER-ORG".into();
        let broker = Arc::new(ExactExitBroker {
            sell_calls: AtomicUsize::new(0),
            account_qty_before_post: 17,
            account_qty_after_post: 17,
            status: Mutex::new(Some(wrong_org_status)),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        arm_exact_target_cancel(&engine);

        engine.start_target_cancel_reconcile("stop_loss", "EXACT-TARGET-1".into());
        tokio::time::sleep(Duration::from_millis(2_250)).await;

        assert_eq!(broker.sell_calls.load(Ordering::SeqCst), 0);
        let runtime = engine.automation.lock().unwrap();
        assert_eq!(
            runtime
                .position()
                .and_then(|position| position.target_order_no.as_deref()),
            Some("EXACT-TARGET-1")
        );
    }

    #[tokio::test]
    async fn 주문번호불명확_목표는_open목록에서_사라져도_exact종결전에는_지우지_않는다() {
        let now = now_kst_fake_epoch();
        let trading_date = crate::util::trading_date_from_fake_epoch(now).unwrap();
        let target_price = sell_target_price(10_005.0, 0.3, true);
        let fill = BrokerFill {
            fill_id: "unknown-target-fill-5".into(),
            order_no: "UNKNOWN-TARGET-1".into(),
            original_order_no: String::new(),
            org_no: "SAFETY-ORG".into(),
            code: "0193T0".into(),
            side: Side::Sell,
            qty: 5,
            price: target_price as f64,
            filled_at: now,
        };
        let status = BrokerOrderStatus {
            trading_date,
            order_no: fill.order_no.clone(),
            original_order_no: String::new(),
            org_no: fill.org_no.clone(),
            code: fill.code.clone(),
            side: Side::Sell,
            ordered_qty: 17,
            filled_qty: 5,
            cancelled_qty: 0,
            remaining_qty: 12,
            rejected_qty: 0,
            avg_fill_price: fill.price,
            ordered_at: now,
        };
        let broker = Arc::new(ReconcileSafetyBroker {
            account_qty: 12,
            fills: Mutex::new(vec![fill]),
            status: Mutex::new(Some(status.clone())),
            sell_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        let intent_id = "unknown-target-intent";
        engine
            .ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: intent_id.into(),
                session_id: None,
                trade_id: Some("unknown-target-trade".into()),
                decision_id: None,
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                side: LedgerSide::Sell,
                order_type: LedgerOrderType::Limit,
                qty: 17,
                price: Some(target_price),
                requested_at: now,
            })
            .unwrap();
        assert!(engine.ledger.begin_order_dispatch(intent_id, now).unwrap());
        engine
            .ledger
            .record_order_ack(
                intent_id,
                &OrderAcknowledgement {
                    broker_order_id: None,
                    broker_org_no: None,
                    original_order_id: None,
                    status: LedgerOrderStatus::Unknown,
                    message: Some("응답 유실".into()),
                    acknowledged_at: now,
                },
            )
            .unwrap();
        {
            let mut runtime = engine.automation.lock().unwrap();
            runtime.set_mode_after_cleanup(ControlMode::Auto, None, None);
            runtime.mark_holding(recovered_position(
                "unknown-target-trade",
                Some(intent_id.into()),
            ));
            assert!(runtime.begin_exit("stop_loss").is_some());
        }

        let first = engine
            .automation
            .lock()
            .unwrap()
            .position()
            .cloned()
            .unwrap();
        engine
            .reconcile_unknown_target_for_exit(&first)
            .await
            .unwrap();
        {
            let runtime = engine.automation.lock().unwrap();
            let position = runtime.position().unwrap();
            assert_eq!(position.qty, 12);
            assert_eq!(
                position.target_order_no.as_deref(),
                Some("UNKNOWN-TARGET-1")
            );
        }
        assert_eq!(broker.sell_calls.load(Ordering::SeqCst), 0);

        *broker.status.lock().unwrap() = Some(BrokerOrderStatus {
            cancelled_qty: 12,
            remaining_qty: 0,
            ..status
        });
        let second = engine
            .automation
            .lock()
            .unwrap()
            .position()
            .cloned()
            .unwrap();
        engine
            .reconcile_unknown_target_for_exit(&second)
            .await
            .unwrap();
        assert!(engine
            .automation
            .lock()
            .unwrap()
            .position()
            .unwrap()
            .target_order_no
            .is_none());
    }

    #[tokio::test]
    async fn post가_시작되지_않은_목표intent는_종결하고_시장가청산을_허용한다() {
        let now = now_kst_fake_epoch();
        let broker = Arc::new(ReconcileSafetyBroker {
            account_qty: 17,
            fills: Mutex::new(Vec::new()),
            status: Mutex::new(None),
            sell_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        let intent_id = "unsubmitted-target-intent";
        engine
            .ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: intent_id.into(),
                session_id: None,
                trade_id: Some("unsubmitted-target-trade".into()),
                decision_id: None,
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                side: LedgerSide::Sell,
                order_type: LedgerOrderType::Limit,
                qty: 17,
                price: Some(sell_target_price(10_005.0, 0.3, true)),
                requested_at: now,
            })
            .unwrap();
        {
            let mut runtime = engine.automation.lock().unwrap();
            runtime.set_mode_after_cleanup(ControlMode::Auto, None, None);
            runtime.mark_holding(recovered_position(
                "unsubmitted-target-trade",
                Some(intent_id.into()),
            ));
            assert!(runtime.begin_exit("target_order_failed").is_some());
        }

        let snapshot = engine
            .automation
            .lock()
            .unwrap()
            .position()
            .cloned()
            .unwrap();
        engine
            .reconcile_unknown_target_for_exit(&snapshot)
            .await
            .unwrap();

        assert!(engine
            .automation
            .lock()
            .unwrap()
            .position()
            .unwrap()
            .target_intent_id
            .is_none());
        assert_eq!(
            engine.ledger.get_order(intent_id).unwrap().unwrap().status,
            LedgerOrderStatus::Cancelled
        );
    }

    fn pending_entry(intent_id: &str, trade_id: &str) -> PendingAutoEntry {
        PendingAutoEntry {
            intent_id: intent_id.into(),
            trade_id: trade_id.into(),
            baseline_qty: 0,
            product: ProductKind::Leverage,
            target_return_pct: 0.3,
        }
    }

    fn prepare_flatten_pending_entry(
        engine: &Engine,
        intent_id: &str,
        trade_id: &str,
        broker_identity: Option<(&str, &str)>,
    ) -> i64 {
        let requested_at = test_automation_now();
        engine
            .ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: intent_id.into(),
                session_id: None,
                trade_id: Some(trade_id.into()),
                decision_id: None,
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                side: LedgerSide::Buy,
                order_type: LedgerOrderType::IocLimit,
                qty: 95,
                price: Some(10_300),
                requested_at,
            })
            .unwrap();
        assert!(engine
            .ledger
            .begin_order_dispatch(intent_id, requested_at)
            .unwrap());
        if let Some((order_no, org_no)) = broker_identity {
            engine
                .ledger
                .record_order_ack(
                    intent_id,
                    &OrderAcknowledgement {
                        broker_order_id: Some(order_no.into()),
                        broker_org_no: Some(org_no.into()),
                        original_order_id: None,
                        status: LedgerOrderStatus::Submitted,
                        message: Some("테스트용 정확 ACK".into()),
                        acknowledged_at: requested_at,
                    },
                )
                .unwrap();
        }
        *engine.automation.lock().unwrap() = AutomationRuntime::new(
            PersistedAutomation {
                mode: ControlMode::Auto,
                session_id: None,
                position: None,
                pending_entry: Some(pending_entry(intent_id, trade_id)),
                shadow_cash: None,
                next_decision_at: None,
                last_decision_slot: None,
            },
            None,
        );
        let close = requested_at.div_euclid(86_400) * 86_400 + 15 * 3_600 + 15 * 60;
        engine
            .automation_now_override
            .store(close, Ordering::SeqCst);
        engine
            .account_refreshed_gen
            .store(engine.account_gen.load(Ordering::SeqCst), Ordering::SeqCst);
        requested_at
    }

    #[test]
    fn 마감_조정에서_미체결과_보유가_없으면_pending_진입을_종결한다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        let intent_id = "empty-pending-entry";
        let trade_id = "empty-pending-trade";
        let requested_at = test_automation_now();
        engine
            .ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: intent_id.into(),
                session_id: None,
                trade_id: Some(trade_id.into()),
                decision_id: None,
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                side: LedgerSide::Buy,
                order_type: LedgerOrderType::IocLimit,
                qty: 95,
                price: Some(10_300),
                requested_at,
            })
            .unwrap();
        assert!(engine
            .ledger
            .begin_order_dispatch(intent_id, requested_at)
            .unwrap());
        engine
            .ledger
            .record_order_ack(
                intent_id,
                &OrderAcknowledgement {
                    broker_order_id: Some("LATE-AUTO-BUY".into()),
                    broker_org_no: Some("LATE-AUTO-ORG".into()),
                    original_order_id: None,
                    status: LedgerOrderStatus::Submitted,
                    message: Some("테스트용 정확 ACK".into()),
                    acknowledged_at: requested_at,
                },
            )
            .unwrap();
        *engine.automation.lock().unwrap() = AutomationRuntime::new(
            PersistedAutomation {
                mode: ControlMode::Auto,
                session_id: None,
                position: None,
                pending_entry: Some(pending_entry(intent_id, trade_id)),
                shadow_cash: None,
                next_decision_at: None,
                last_decision_slot: None,
            },
            None,
        );
        let close = requested_at.div_euclid(86_400) * 86_400 + 15 * 3_600 + 15 * 60;
        engine
            .automation_now_override
            .store(close, Ordering::SeqCst);
        engine
            .account_refreshed_gen
            .store(engine.account_gen.load(Ordering::SeqCst), Ordering::SeqCst);

        assert!(engine
            .finish_empty_pending_entry_for_flatten(&AccountSnapshot {
                cash: 1_000_000,
                positions: Vec::new(),
            })
            .unwrap());

        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::Idle);
        assert!(runtime.pending_entry().is_none());
        assert!(runtime
            .snapshot(
                &engine.runtime_id,
                engine.runtime_generation,
                &engine.settings.read().unwrap().auto_symbols,
                engine.monotonic_now(),
                MarketDayStatus::Open,
                None,
            )
            .error
            .as_deref()
            .is_some_and(|error| error.contains("노출이 없음")));
        drop(runtime);
        let order = engine.ledger.get_order(intent_id).unwrap().unwrap();
        assert_eq!(order.status, LedgerOrderStatus::Unknown);
        assert!(engine
            .ledger
            .get_runtime_state::<LateAutoEntryWatch>(LATE_AUTO_ENTRY_WATCH_KEY)
            .unwrap()
            .is_some());

        let late_fill = crate::types::FillEvent {
            fill_id: "late-entry-notice".into(),
            order_no: "LATE-AUTO-BUY".into(),
            original_order_no: String::new(),
            org_no: "LATE-AUTO-ORG".into(),
            code: "0193T0".into(),
            side: Side::Buy,
            qty: 17,
            price: 10_000.0,
            filled_at: requested_at + 1,
            status: "filled".into(),
        };

        let external_fill = crate::types::FillEvent {
            fill_id: "external-buy".into(),
            order_no: "EXTERNAL-BUY".into(),
            org_no: "EXTERNAL-ORG".into(),
            ..late_fill.clone()
        };
        assert!(!engine.restore_late_auto_entry_notice(&external_fill));
        assert!(engine.automation.lock().unwrap().pending_entry().is_none());
        assert_eq!(
            engine.ledger.get_order(intent_id).unwrap().unwrap().status,
            LedgerOrderStatus::Unknown
        );

        engine
            .automation
            .lock()
            .unwrap()
            .set_mode_after_cleanup(ControlMode::Manual, None, None);
        assert!(!engine.restore_late_auto_entry_notice(&late_fill));
        assert_eq!(
            engine.ledger.get_order(intent_id).unwrap().unwrap().status,
            LedgerOrderStatus::Unknown
        );
        engine
            .automation
            .lock()
            .unwrap()
            .set_mode_after_cleanup(ControlMode::Auto, None, None);

        assert!(engine.restore_late_auto_entry_notice(&late_fill));
        assert!(engine.automation.lock().unwrap().pending_entry().is_some());
        assert!(engine
            .ledger
            .get_runtime_state::<LateAutoEntryWatch>(LATE_AUTO_ENTRY_WATCH_KEY)
            .unwrap()
            .is_none());
        let restored = engine.ledger.get_order(intent_id).unwrap().unwrap();
        assert_eq!(restored.broker_order_id.as_deref(), Some("LATE-AUTO-BUY"));
        assert_eq!(restored.broker_org_no.as_deref(), Some("LATE-AUTO-ORG"));
    }

    #[test]
    fn 정확한_주문identity가_없는_pending은_tombstone으로_내리지_않는다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        prepare_flatten_pending_entry(&engine, "no-identity-pending", "no-identity-trade", None);

        assert!(!engine
            .finish_empty_pending_entry_for_flatten(&AccountSnapshot {
                cash: 1_000_000,
                positions: Vec::new(),
            })
            .unwrap());
        assert!(engine.automation.lock().unwrap().pending_entry().is_some());
        assert!(engine
            .ledger
            .get_runtime_state::<LateAutoEntryWatch>(LATE_AUTO_ENTRY_WATCH_KEY)
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn ws통보가_없어도_tombstone을_rest로_복원해_마감청산한다() {
        let requested_at = test_automation_now();
        let trading_date = crate::util::trading_date_from_fake_epoch(requested_at).unwrap();
        let fill = BrokerFill {
            fill_id: "late-rest-fill-17".into(),
            order_no: "LATE-REST-BUY".into(),
            original_order_no: String::new(),
            org_no: "LATE-REST-ORG".into(),
            code: "0193T0".into(),
            side: Side::Buy,
            qty: 17,
            price: 10_000.0,
            filled_at: requested_at,
        };
        let broker = Arc::new(ReconcileSafetyBroker {
            account_qty: 17,
            fills: Mutex::new(vec![fill.clone()]),
            status: Mutex::new(Some(BrokerOrderStatus {
                trading_date,
                order_no: fill.order_no.clone(),
                original_order_no: String::new(),
                org_no: fill.org_no.clone(),
                code: fill.code.clone(),
                side: Side::Buy,
                ordered_qty: 95,
                filled_qty: 17,
                cancelled_qty: 78,
                remaining_qty: 0,
                rejected_qty: 0,
                avg_fill_price: fill.price,
                ordered_at: requested_at,
            })),
            sell_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        prepare_flatten_pending_entry(
            &engine,
            "late-rest-pending",
            "late-rest-trade",
            Some(("LATE-REST-BUY", "LATE-REST-ORG")),
        );
        assert!(engine
            .finish_empty_pending_entry_for_flatten(&AccountSnapshot {
                cash: 1_000_000,
                positions: Vec::new(),
            })
            .unwrap());

        assert!(engine.refresh_account().await);
        assert!(engine.reconcile_late_auto_entry_watch_from_rest(None).await);
        assert!(engine.automation.lock().unwrap().pending_entry().is_some());
        assert!(engine
            .ledger
            .get_runtime_state::<LateAutoEntryWatch>(LATE_AUTO_ENTRY_WATCH_KEY)
            .unwrap()
            .is_none());

        engine.start_pending_entry_reconcile();
        tokio::time::timeout(Duration::from_secs(8), async {
            while broker.sell_calls.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("REST로 복원한 늦은 IOC 체결은 15:15 시장가 청산으로 이어져야 함");
        assert_eq!(broker.sell_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn rest_누적체결은_수량과_체결대금을_delta로_정확히_역산한다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        let filled_at = now_kst_fake_epoch();
        let first = crate::types::FillEvent {
            fill_id: "rest-cumulative-4".into(),
            order_no: "CUMULATIVE-ORDER".into(),
            original_order_no: String::new(),
            org_no: "CUMULATIVE-ORG".into(),
            code: "0193T0".into(),
            side: Side::Buy,
            qty: 4,
            price: 100.0,
            filled_at,
            status: "filled".into(),
        };
        let second = crate::types::FillEvent {
            fill_id: "rest-cumulative-10".into(),
            qty: 10,
            // 누적대금 1,060 = 기존 400 + 신규 660, 따라서 신규 6주의 평균은 110.
            price: 106.0,
            ..first.clone()
        };

        let first_delta = engine.record_reconciled_fill(&first).unwrap();
        assert_eq!(first_delta.qty, 4);
        assert!((first_delta.price - 100.0).abs() < f64::EPSILON);
        let second_delta = engine.record_reconciled_fill(&second).unwrap();
        assert_eq!(second_delta.qty, 6);
        assert!((second_delta.price - 110.0).abs() < 1e-9);
        assert!(engine.record_reconciled_fill(&second).is_none());

        let key = BrokerOrderKey::from_fake_epoch(filled_at, "CUMULATIVE-ORG", "CUMULATIVE-ORDER")
            .unwrap();
        let (qty, value) = engine.ledger.fill_totals_for_broker_order(&key).unwrap();
        assert_eq!(qty, 10);
        assert!((value - 1_060.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn rest가_먼저_반영된_뒤늦은_ws_통보는_수량을_다시_요구하지_않는다() {
        let filled_at = now_kst_fake_epoch();
        let cumulative = BrokerFill {
            fill_id: "rest-first-4".into(),
            order_no: "REST-FIRST-ORDER".into(),
            original_order_no: String::new(),
            org_no: "REST-FIRST-ORG".into(),
            code: "0193T0".into(),
            side: Side::Buy,
            qty: 4,
            price: 10_005.0,
            filled_at,
        };
        let broker = Arc::new(ReconcileSafetyBroker {
            account_qty: 4,
            fills: Mutex::new(vec![cumulative.clone()]),
            status: Mutex::new(None),
            sell_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        let rest_event = crate::types::FillEvent {
            fill_id: cumulative.fill_id.clone(),
            order_no: cumulative.order_no.clone(),
            original_order_no: String::new(),
            org_no: cumulative.org_no.clone(),
            code: cumulative.code.clone(),
            side: cumulative.side,
            qty: cumulative.qty,
            price: cumulative.price,
            filled_at,
            status: "filled".into(),
        };
        assert!(engine.apply_reconciled_fill(&rest_event));

        engine.start_fill_notice_reconcile(crate::types::FillEvent {
            fill_id: "delayed-ws-notice".into(),
            org_no: String::new(),
            ..rest_event
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if engine
                    .fill_reconcile_pending
                    .lock()
                    .unwrap()
                    .values()
                    .all(|state| !state.running)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("REST 선반영 통보 조정은 불가능한 8주 목표를 기다리지 않고 끝나야 함");

        let key = BrokerOrderKey::from_fake_epoch(filled_at, "REST-FIRST-ORG", "REST-FIRST-ORDER")
            .unwrap();
        assert_eq!(engine.ledger.filled_qty_for_broker_order(&key).unwrap(), 4);
    }

    #[tokio::test]
    async fn 같은날_같은_주문번호의_다른_조직체결을_ws_조정에_섞지_않는다() {
        let filled_at = now_kst_fake_epoch();
        let fills = vec![
            BrokerFill {
                fill_id: "org-a-cumulative-3".into(),
                order_no: "REUSED-ORDER".into(),
                original_order_no: String::new(),
                org_no: "ORG-A".into(),
                code: "0193T0".into(),
                side: Side::Buy,
                qty: 3,
                price: 10_005.0,
                filled_at,
            },
            BrokerFill {
                fill_id: "org-b-cumulative-7".into(),
                order_no: "REUSED-ORDER".into(),
                original_order_no: String::new(),
                org_no: "ORG-B".into(),
                code: "0193T0".into(),
                side: Side::Buy,
                qty: 7,
                price: 9_995.0,
                filled_at,
            },
        ];
        let broker = Arc::new(ReconcileSafetyBroker {
            account_qty: 3,
            fills: Mutex::new(fills),
            status: Mutex::new(None),
            sell_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        let (intent_id, _) = engine
            .record_manual_order_intent(
                "0193T0",
                LedgerSide::Buy,
                LedgerOrderType::IocLimit,
                3,
                Some(10_300),
            )
            .unwrap();
        engine.record_broker_ack(
            &intent_id,
            &OrderAck {
                order_no: "REUSED-ORDER".into(),
                org_no: "ORG-A".into(),
                message: "IOC 접수".into(),
            },
            LedgerOrderStatus::Submitted,
        );

        engine.start_fill_notice_reconcile(crate::types::FillEvent {
            fill_id: "ws-reused-order".into(),
            order_no: "REUSED-ORDER".into(),
            original_order_no: String::new(),
            org_no: String::new(),
            code: "0193T0".into(),
            side: Side::Buy,
            qty: 3,
            price: 10_005.0,
            filled_at,
            status: "filled".into(),
        });

        let org_a = BrokerOrderKey::from_fake_epoch(filled_at, "ORG-A", "REUSED-ORDER").unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while engine.ledger.filled_qty_for_broker_order(&org_a).unwrap() != 3 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("ACK의 정확 조직번호 체결만 반영돼야 함");

        let org_b = BrokerOrderKey::from_fake_epoch(filled_at, "ORG-B", "REUSED-ORDER").unwrap();
        assert_eq!(
            engine.ledger.filled_qty_for_broker_order(&org_a).unwrap(),
            3
        );
        assert_eq!(
            engine.ledger.filled_qty_for_broker_order(&org_b).unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn 수동_주문은_ws_통보가_없어도_30초_rest_조정으로_한번만_기록된다() {
        let filled_at = now_kst_fake_epoch();
        let fill = BrokerFill {
            fill_id: "manual-rest-cumulative-4".into(),
            order_no: "MANUAL-BUY-1".into(),
            original_order_no: String::new(),
            org_no: "MANUAL-ORG".into(),
            code: "0193T0".into(),
            side: Side::Buy,
            qty: 4,
            price: 10_005.0,
            filled_at,
        };
        let broker = Arc::new(ReconcileSafetyBroker {
            account_qty: 4,
            fills: Mutex::new(vec![fill]),
            status: Mutex::new(None),
            sell_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        let (intent_id, _) = engine
            .record_manual_order_intent(
                "0193T0",
                LedgerSide::Buy,
                LedgerOrderType::IocLimit,
                4,
                Some(10_300),
            )
            .unwrap();
        engine.record_broker_ack(
            &intent_id,
            &OrderAck {
                order_no: "MANUAL-BUY-1".into(),
                org_no: "MANUAL-ORG".into(),
                message: "IOC 접수".into(),
            },
            LedgerOrderStatus::Submitted,
        );

        // WebSocket 알림 없이 실제 30초 타이머가 호출하는 한 회차를 두 번 실행한다.
        periodic_refresh_once(&engine).await;
        periodic_refresh_once(&engine).await;

        let key = BrokerOrderKey::from_fake_epoch(filled_at, "MANUAL-ORG", "MANUAL-BUY-1").unwrap();
        let (qty, value) = engine.ledger.fill_totals_for_broker_order(&key).unwrap();
        assert_eq!(qty, 4);
        assert!((value - 40_020.0).abs() < 1e-9);

        let trade = engine
            .ledger
            .get_runtime_state::<ManualTradeAccumulator>("manual_trade:0193T0")
            .unwrap()
            .expect("수동 거래 누적기도 생성되어야 함");
        assert_eq!(trade.entry_qty, 4);
        assert!((trade.entry_value - 40_020.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn 연속_ws_통보는_rest_누적반영이_늦어도_요구수량까지_계속_조정한다() {
        let broker = auto_entry_broker(10, Some(4));
        broker.buy_calls.store(1, Ordering::SeqCst);
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        let filled_at = now_kst_fake_epoch();
        let first = crate::types::FillEvent {
            fill_id: "ws-fill-first".into(),
            order_no: "AUTO-BUY-1".into(),
            original_order_no: String::new(),
            org_no: String::new(),
            code: "0193T0".into(),
            side: Side::Buy,
            qty: 4,
            price: 10_005.0,
            filled_at,
            status: "filled".into(),
        };
        let second = crate::types::FillEvent {
            fill_id: "ws-fill-second".into(),
            qty: 6,
            ..first.clone()
        };

        engine.start_fill_notice_reconcile(first);
        engine.start_fill_notice_reconcile(second);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let key =
                    BrokerOrderKey::from_fake_epoch(filled_at, "AUTO-ORG", "AUTO-BUY-1").unwrap();
                if engine.ledger.filled_qty_for_broker_order(&key).unwrap() == 10 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("REST 누적체결이 요구수량까지 반영되어야 함");

        assert!(broker.today_fill_calls.load(Ordering::SeqCst) >= 4);
    }

    #[test]
    fn ack_전_auto_목표체결도_주문소유권을_먼저_결합해_한번만_반영한다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        let now = engine.automation_now();
        let intent_id = "pre-ack-target-intent";
        let position = recovered_position("pre-ack-target-trade", Some(intent_id.into()));
        let target_price = position.target_price;
        engine
            .ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: intent_id.into(),
                session_id: None,
                trade_id: Some(position.trade_id.clone()),
                decision_id: None,
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: position.code.clone(),
                side: LedgerSide::Sell,
                order_type: LedgerOrderType::Limit,
                qty: position.qty,
                price: Some(target_price),
                requested_at: now,
            })
            .unwrap();
        assert!(engine.ledger.begin_order_dispatch(intent_id, now).unwrap());
        {
            let mut runtime = engine.automation.lock().unwrap();
            runtime.set_mode_after_cleanup(ControlMode::Auto, None, None);
            runtime.mark_holding(position);
        }
        let cumulative = crate::types::FillEvent {
            fill_id: "pre-ack-rest-fill".into(),
            order_no: "PRE-ACK-TARGET".into(),
            original_order_no: String::new(),
            org_no: "PRE-ACK-ORG".into(),
            code: "0193T0".into(),
            side: Side::Sell,
            qty: 5,
            price: target_price as f64,
            filled_at: now,
            status: "filled".into(),
        };

        engine.claim_pending_auto_sell_notice(&cumulative);
        assert!(engine.apply_reconciled_fill(&cumulative));
        assert!(!engine.apply_reconciled_fill(&cumulative));

        let runtime = engine.automation.lock().unwrap();
        let remaining = runtime.position().unwrap();
        assert_eq!(remaining.target_order_no.as_deref(), Some("PRE-ACK-TARGET"));
        assert_eq!(remaining.qty, 12);
        drop(runtime);
        let order = engine.ledger.get_order(intent_id).unwrap().unwrap();
        assert_eq!(order.broker_order_id.as_deref(), Some("PRE-ACK-TARGET"));
        assert_eq!(order.broker_org_no.as_deref(), Some("PRE-ACK-ORG"));

        engine
            .ledger
            .update_order_status(
                intent_id,
                LedgerOrderStatus::PartiallyFilled,
                Some("부분체결"),
                now,
            )
            .unwrap();
        engine.claim_pending_auto_sell_notice(&cumulative);
        assert_eq!(
            engine.ledger.get_order(intent_id).unwrap().unwrap().status,
            LedgerOrderStatus::PartiallyFilled,
            "반복 REST 스냅샷이 부분체결 상태를 Submitted로 강등하면 안 됨"
        );
    }

    #[test]
    fn 재시작한_dispatching_ioc는_주문번호복구뒤_정확종결을_기다린다() {
        let broker = Arc::new(ShadowPostCountingBroker {
            post_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        let now = now_kst_fake_epoch();
        let intent_id = "recover-entry";
        let trade_id = "recover-trade";
        engine
            .ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: intent_id.into(),
                session_id: None,
                trade_id: Some(trade_id.into()),
                decision_id: None,
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                side: LedgerSide::Buy,
                order_type: LedgerOrderType::IocLimit,
                qty: 95,
                price: Some(10_300),
                requested_at: now,
            })
            .unwrap();
        assert!(engine.ledger.begin_order_dispatch(intent_id, now).unwrap());
        *engine.automation.lock().unwrap() = AutomationRuntime::new(
            PersistedAutomation {
                mode: ControlMode::Auto,
                session_id: None,
                position: None,
                pending_entry: Some(pending_entry(intent_id, trade_id)),
                shadow_cash: None,
                next_decision_at: None,
                last_decision_slot: None,
            },
            None,
        );
        let account = AccountSnapshot {
            cash: 800_000,
            positions: vec![crate::types::Position {
                code: "0193T0".into(),
                qty: 17,
                avg_price: 10_005.0,
                eval_pnl: 0.0,
                pnl_rate: 0.0,
            }],
        };
        let fill = BrokerFill {
            fill_id: "recover-fill".into(),
            order_no: "RECOVER-BUY".into(),
            original_order_no: String::new(),
            org_no: "RECOVER-ORG".into(),
            code: "0193T0".into(),
            side: Side::Buy,
            qty: 17,
            price: 10_005.0,
            filled_at: now,
        };

        let waiting = engine
            .reconcile_pending_auto_entry_startup(&account, &[], &[fill.clone()])
            .unwrap_err();
        assert!(waiting.contains("정확한 terminal"));
        assert!(engine.automation.lock().unwrap().pending_entry().is_some());
        let recovered = engine.ledger.get_order(intent_id).unwrap().unwrap();
        assert_eq!(recovered.broker_order_id.as_deref(), Some("RECOVER-BUY"));
        assert_eq!(recovered.status, LedgerOrderStatus::Submitted);
        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);

        // 비동기 호출부의 exact order_status가 terminal 부분체결을 확인해 장부에
        // 반영한 다음에만 포지션 복구를 진행한다.
        engine
            .ledger
            .update_order_status(
                intent_id,
                LedgerOrderStatus::PartiallyFilled,
                Some("테스트 exact terminal 확인"),
                now,
            )
            .unwrap();
        engine
            .reconcile_pending_auto_entry_startup(&account, &[], &[fill])
            .unwrap();

        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::ExitPending);
        assert!(runtime.pending_entry().is_none());
        assert_eq!(runtime.position().map(|position| position.qty), Some(17));
        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            engine.ledger.get_order(intent_id).unwrap().unwrap().status,
            LedgerOrderStatus::PartiallyFilled
        );
    }

    #[test]
    fn 재시작한_pending_진입은_기존_목표주문을_결합해_holding으로_복원한다() {
        let broker = Arc::new(ShadowPostCountingBroker {
            post_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        let now = now_kst_fake_epoch();
        let entry_intent = "recover-protected-entry";
        let target_intent = "recover-protected-target";
        let trade_id = "recover-protected-trade";
        engine
            .ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: entry_intent.into(),
                session_id: None,
                trade_id: Some(trade_id.into()),
                decision_id: None,
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                side: LedgerSide::Buy,
                order_type: LedgerOrderType::IocLimit,
                qty: 95,
                price: Some(10_300),
                requested_at: now,
            })
            .unwrap();
        engine
            .ledger
            .record_order_ack(
                entry_intent,
                &OrderAcknowledgement {
                    broker_order_id: Some("PROTECTED-BUY".into()),
                    broker_org_no: Some("RECOVER-ORG".into()),
                    original_order_id: None,
                    status: LedgerOrderStatus::PartiallyFilled,
                    message: None,
                    acknowledged_at: now,
                },
            )
            .unwrap();
        let target_price = sell_target_price(10_005.0, 0.3, true);
        engine
            .ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: target_intent.into(),
                session_id: None,
                trade_id: Some(trade_id.into()),
                decision_id: None,
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                side: LedgerSide::Sell,
                order_type: LedgerOrderType::Limit,
                qty: 17,
                price: Some(target_price),
                requested_at: now,
            })
            .unwrap();
        assert!(engine
            .ledger
            .begin_order_dispatch(target_intent, now)
            .unwrap());
        *engine.automation.lock().unwrap() = AutomationRuntime::new(
            PersistedAutomation {
                mode: ControlMode::Auto,
                session_id: None,
                position: Some(recovered_position(trade_id, Some(target_intent.into()))),
                pending_entry: Some(pending_entry(entry_intent, trade_id)),
                shadow_cash: None,
                next_decision_at: None,
                last_decision_slot: None,
            },
            None,
        );
        let account = AccountSnapshot {
            cash: 800_000,
            positions: vec![crate::types::Position {
                code: "0193T0".into(),
                qty: 17,
                avg_price: 10_005.0,
                eval_pnl: 0.0,
                pnl_rate: 0.0,
            }],
        };
        let fills = [BrokerFill {
            fill_id: "protected-entry-fill".into(),
            order_no: "PROTECTED-BUY".into(),
            original_order_no: String::new(),
            org_no: "RECOVER-ORG".into(),
            code: "0193T0".into(),
            side: Side::Buy,
            qty: 17,
            price: 10_005.0,
            filled_at: now,
        }];
        let open_orders = [BrokerOpenOrder {
            order_no: "PROTECTED-TARGET".into(),
            original_order_no: String::new(),
            org_no: "RECOVER-ORG".into(),
            code: "0193T0".into(),
            side: Side::Sell,
            ordered_qty: 17,
            filled_qty: 0,
            cancelable_qty: 17,
            price: target_price as f64,
            ordered_at: now,
        }];

        engine
            .reconcile_pending_auto_entry_startup(&account, &open_orders, &fills)
            .unwrap();

        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::Holding);
        assert!(runtime.pending_entry().is_none());
        assert_eq!(
            runtime
                .position()
                .and_then(|position| position.target_order_no.as_deref()),
            Some("PROTECTED-TARGET")
        );
        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.get_reservations().len(), 1);
    }

    /// Auto 인계 취소 POST 시점에 내구성 표식이 이미 저장됐는지 검증하는 더블.
    struct DurableHandoffBroker {
        ledger: std::sync::OnceLock<Arc<Ledger>>,
        status: BrokerOrderStatus,
        account_qty: u64,
        cancel_calls: AtomicUsize,
        marker_seen_at_cancel: AtomicBool,
    }

    #[async_trait::async_trait]
    impl Broker for DurableHandoffBroker {
        async fn candles_1m(&self, _code: &str) -> AppResult<Vec<Candle>> {
            Ok(Vec::new())
        }

        async fn account(&self) -> AppResult<AccountSnapshot> {
            Ok(AccountSnapshot {
                cash: 1_000_000,
                positions: (self.account_qty > 0)
                    .then(|| crate::types::Position {
                        code: self.status.code.clone(),
                        qty: self.account_qty,
                        avg_price: 10_000.0,
                        eval_pnl: 0.0,
                        pnl_rate: 0.0,
                    })
                    .into_iter()
                    .collect(),
            })
        }

        async fn snapshot(&self, _code: &str) -> AppResult<Quote> {
            Err(AppError::Config("내구성 인계 테스트에서 미사용".into()))
        }

        async fn max_buy_qty(&self, _code: &str, _limit_price: u64) -> AppResult<u64> {
            Err(AppError::Order("내구성 인계 테스트에서 미사용".into()))
        }

        async fn place_buy(
            &self,
            _code: &str,
            _qty: u64,
            _limit_price: u64,
            _ioc: bool,
        ) -> AppResult<OrderAck> {
            Err(AppError::Order("내구성 인계 테스트에서 매수 금지".into()))
        }

        async fn place_sell_market(&self, _code: &str, _qty: u64) -> AppResult<OrderAck> {
            Err(AppError::Order(
                "내구성 인계 테스트에서 시장가 매도 금지".into(),
            ))
        }

        async fn place_sell_limit(
            &self,
            _code: &str,
            _qty: u64,
            _limit_price: u64,
        ) -> AppResult<OrderAck> {
            Err(AppError::Order(
                "내구성 인계 테스트에서 지정가 매도 금지".into(),
            ))
        }

        async fn cancel_order(
            &self,
            _code: &str,
            order_no: &str,
            _org_no: &str,
        ) -> AppResult<OrderAck> {
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
            let marker_seen = self.ledger.get().is_some_and(|ledger| {
                ledger
                    .get_runtime_state::<PendingAutoHandoff>(AUTO_HANDOFF_STATE_KEY)
                    .ok()
                    .flatten()
                    .is_some_and(|handoff| handoff.target_mode == ControlMode::Manual)
                    && ledger
                        .get_runtime_state::<PersistedAutomationBundle>(AUTOMATION_BUNDLE_STATE_KEY)
                        .ok()
                        .flatten()
                        .is_some_and(|bundle| {
                            bundle.automation.mode == ControlMode::Auto
                                && bundle.automation.position.is_some()
                        })
            });
            self.marker_seen_at_cancel
                .store(marker_seen, Ordering::SeqCst);
            Ok(OrderAck {
                order_no: format!("CANCEL-{order_no}"),
                org_no: self.status.org_no.clone(),
                message: "취소 접수".into(),
            })
        }

        async fn open_orders(&self) -> AppResult<Vec<BrokerOpenOrder>> {
            Ok(Vec::new())
        }

        async fn today_fills(&self) -> AppResult<Vec<BrokerFill>> {
            Ok(Vec::new())
        }

        async fn order_status(
            &self,
            trading_date: &str,
            order_no: &str,
        ) -> AppResult<Option<BrokerOrderStatus>> {
            Ok(
                (self.status.trading_date == trading_date && self.status.order_no == order_no)
                    .then(|| self.status.clone()),
            )
        }

        async fn start_feed(
            &self,
            _codes: Vec<String>,
            _tx: mpsc::Sender<FeedEvent>,
            _reconnect: std::sync::Arc<tokio::sync::Notify>,
        ) -> AppResult<Vec<JoinHandle<()>>> {
            Ok(Vec::new())
        }
    }

    /// 목표 취소 요청과 동시에 전량 체결된 상황을 재현한다.
    struct HandoffTargetFilledBroker {
        fill: BrokerFill,
        cancel_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Broker for HandoffTargetFilledBroker {
        async fn candles_1m(&self, _code: &str) -> AppResult<Vec<Candle>> {
            Ok(Vec::new())
        }

        async fn account(&self) -> AppResult<AccountSnapshot> {
            Ok(AccountSnapshot {
                cash: 1_100_000,
                positions: Vec::new(),
            })
        }

        async fn snapshot(&self, _code: &str) -> AppResult<Quote> {
            Err(AppError::Config("인계 테스트에서 미사용".into()))
        }

        async fn max_buy_qty(&self, _code: &str, _limit_price: u64) -> AppResult<u64> {
            Err(AppError::Order("인계 테스트에서 미사용".into()))
        }

        async fn place_buy(
            &self,
            _code: &str,
            _qty: u64,
            _limit_price: u64,
            _ioc: bool,
        ) -> AppResult<OrderAck> {
            Err(AppError::Order("인계 테스트에서 미사용".into()))
        }

        async fn place_sell_market(&self, _code: &str, _qty: u64) -> AppResult<OrderAck> {
            Err(AppError::Order("인계 테스트에서 미사용".into()))
        }

        async fn place_sell_limit(
            &self,
            _code: &str,
            _qty: u64,
            _limit_price: u64,
        ) -> AppResult<OrderAck> {
            Err(AppError::Order("인계 테스트에서 미사용".into()))
        }

        async fn cancel_order(
            &self,
            _code: &str,
            _order_no: &str,
            _org_no: &str,
        ) -> AppResult<OrderAck> {
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
            Ok(OrderAck {
                order_no: "CANCEL-ACK".into(),
                org_no: "ORG-1".into(),
                message: "취소 접수와 동시에 전량 체결".into(),
            })
        }

        async fn open_orders(&self) -> AppResult<Vec<BrokerOpenOrder>> {
            Ok(Vec::new())
        }

        async fn today_fills(&self) -> AppResult<Vec<BrokerFill>> {
            Ok(vec![self.fill.clone()])
        }

        async fn order_status(
            &self,
            trading_date: &str,
            order_no: &str,
        ) -> AppResult<Option<BrokerOrderStatus>> {
            if order_no != self.fill.order_no {
                return Ok(None);
            }
            Ok(Some(BrokerOrderStatus {
                trading_date: trading_date.into(),
                order_no: self.fill.order_no.clone(),
                original_order_no: self.fill.original_order_no.clone(),
                org_no: self.fill.org_no.clone(),
                code: self.fill.code.clone(),
                side: self.fill.side,
                ordered_qty: self.fill.qty,
                filled_qty: self.fill.qty,
                cancelled_qty: 0,
                remaining_qty: 0,
                rejected_qty: 0,
                avg_fill_price: self.fill.price,
                ordered_at: self.fill.filled_at,
            }))
        }

        async fn start_feed(
            &self,
            _codes: Vec<String>,
            _tx: mpsc::Sender<FeedEvent>,
            _reconnect: std::sync::Arc<tokio::sync::Notify>,
        ) -> AppResult<Vec<JoinHandle<()>>> {
            Ok(Vec::new())
        }
    }

    #[async_trait::async_trait]
    impl Broker for ShadowPostCountingBroker {
        async fn candles_1m(&self, _code: &str) -> AppResult<Vec<Candle>> {
            Ok(Vec::new())
        }
        async fn account(&self) -> AppResult<AccountSnapshot> {
            Ok(AccountSnapshot {
                cash: 1_000_000,
                positions: Vec::new(),
            })
        }
        async fn snapshot(&self, _code: &str) -> AppResult<Quote> {
            Err(AppError::Config("섀도 경로에서 미사용".into()))
        }
        async fn max_buy_qty(&self, _code: &str, _limit_price: u64) -> AppResult<u64> {
            Err(AppError::Order("섀도 경로에서 미사용".into()))
        }
        async fn place_buy(
            &self,
            _code: &str,
            _qty: u64,
            _price: u64,
            _ioc: bool,
        ) -> AppResult<OrderAck> {
            self.post_calls.fetch_add(1, Ordering::SeqCst);
            Err(AppError::Order("섀도에서 실주문을 호출하면 안 됨".into()))
        }
        async fn place_sell_market(&self, _code: &str, _qty: u64) -> AppResult<OrderAck> {
            self.post_calls.fetch_add(1, Ordering::SeqCst);
            Err(AppError::Order("섀도에서 실주문을 호출하면 안 됨".into()))
        }
        async fn place_sell_limit(
            &self,
            _code: &str,
            _qty: u64,
            _price: u64,
        ) -> AppResult<OrderAck> {
            self.post_calls.fetch_add(1, Ordering::SeqCst);
            Err(AppError::Order("섀도에서 실주문을 호출하면 안 됨".into()))
        }
        async fn cancel_order(
            &self,
            _code: &str,
            _order_no: &str,
            _org_no: &str,
        ) -> AppResult<OrderAck> {
            self.post_calls.fetch_add(1, Ordering::SeqCst);
            Err(AppError::Order("섀도에서 실주문을 호출하면 안 됨".into()))
        }
        async fn start_feed(
            &self,
            _codes: Vec<String>,
            _tx: mpsc::Sender<FeedEvent>,
            _reconnect: std::sync::Arc<tokio::sync::Notify>,
        ) -> AppResult<Vec<JoinHandle<()>>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn auto_mode_rejects_manual_order_before_broker_post() {
        let broker = Arc::new(ShadowPostCountingBroker {
            post_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine
            .automation
            .lock()
            .unwrap()
            .set_mode_after_cleanup(ControlMode::Auto, None, None);

        let buy = engine.buy_max("0193T0").await;
        let sell = engine.sell_all("0193T0").await;
        let reserve = engine.place_reserved_sell("0193T0", 0.3).await;

        assert!(!buy.ok && buy.message.contains("Auto 모드"));
        assert!(!sell.ok && sell.message.contains("Auto 모드"));
        assert!(!reserve.ok && reserve.message.contains("Auto 모드"));
        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn shadow_entry_never_calls_broker_post_and_uses_unique_virtual_ids() {
        let broker = Arc::new(ShadowPostCountingBroker {
            post_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.account.write().unwrap().cash = 1_000_000;
        seed_shadow_quotes(&engine, 10, 10);
        let triggered = arm_shadow_trigger(&engine);

        engine.execute_shadow_entry(triggered).await;

        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);
        let position = engine
            .automation
            .lock()
            .unwrap()
            .position()
            .cloned()
            .expect("섀도 포지션");
        assert!(position.shadow);
        let orders = engine
            .shadow
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .orders()
            .to_vec();
        assert_eq!(orders.len(), 2);
        for order in &orders {
            let intent_id = shadow_intent_for_order(&position, order);
            assert!(intent_id.contains(&position.trade_id));
            assert!(
                engine.ledger.get_order(&intent_id).unwrap().is_some(),
                "가상 주문 장부 누락: {intent_id}"
            );
        }
    }

    #[tokio::test]
    async fn shadow_연속거래는_두번째_청산가와_수익률을_현재거래로만_기록한다() {
        let broker = Arc::new(ShadowPostCountingBroker {
            post_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.account.write().unwrap().cash = 1_000_000;
        seed_shadow_quotes(&engine, 10, 10);

        engine
            .execute_shadow_entry(arm_shadow_trigger(&engine))
            .await;
        let first_position = engine
            .automation
            .lock()
            .unwrap()
            .position()
            .cloned()
            .expect("첫 번째 섀도 포지션");
        let first_close = engine
            .shadow
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .on_trade_tick(ShadowTradeTick {
                product: ShadowProduct::Leverage,
                sequence: 1,
                price: 10_030,
                volume: 10,
                at: engine.automation_now() + 1,
            })
            .unwrap();
        assert!(first_close.position_closed);
        engine.apply_shadow_update(first_close);

        {
            let now = engine.automation_now();
            let mut quotes = engine.quotes.write().unwrap();
            let leverage = quotes.get_mut("0193T0").unwrap();
            leverage.price = 20_000.0;
            leverage.ask1 = 20_000.0;
            leverage.bid1 = 19_995.0;
            leverage.ask1_qty = 5;
            leverage.bid1_qty = 5;
            leverage.trade_ts = now;
            leverage.book_ts = now;
        }
        engine
            .execute_shadow_entry(arm_shadow_trigger(&engine))
            .await;
        let second_position = engine
            .automation
            .lock()
            .unwrap()
            .position()
            .cloned()
            .expect("두 번째 섀도 포지션");
        assert_ne!(second_position.trade_id, first_position.trade_id);
        assert_eq!(second_position.entry_qty, 5);

        let bundle = engine
            .ledger
            .get_runtime_state::<PersistedAutomationBundle>(AUTOMATION_BUNDLE_STATE_KEY)
            .unwrap()
            .unwrap();
        let persisted = bundle.automation.position.unwrap();
        assert_eq!(persisted.trade_id, second_position.trade_id);
        assert_eq!(persisted.exit_qty, 0);
        assert_eq!(persisted.exit_value, 0.0);

        let second_close = engine
            .shadow
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .on_trade_tick(ShadowTradeTick {
                product: ShadowProduct::Leverage,
                sequence: 1,
                price: 20_060,
                volume: 5,
                at: engine.automation_now() + 2,
            })
            .unwrap();
        assert!(second_close.position_closed);
        engine.apply_shadow_update(second_close);

        let trades = engine
            .ledger
            .list_trades(&crate::ledger::TradeQuery::default(), None, 10)
            .unwrap();
        let first = trades
            .items
            .iter()
            .find(|trade| trade.trade_id == first_position.trade_id)
            .unwrap();
        assert_eq!(first.exit_qty, 10);
        assert_eq!(first.exit_avg_price, Some(10_030.0));
        assert!((first.pnl_rate.unwrap() - 0.3).abs() < 1e-9);

        let second = trades
            .items
            .iter()
            .find(|trade| trade.trade_id == second_position.trade_id)
            .unwrap();
        assert_eq!(second.exit_qty, 5);
        assert_eq!(second.exit_avg_price, Some(20_060.0));
        assert!((second.pnl_rate.unwrap() - 0.3).abs() < 1e-9);

        let shadow = engine.shadow.lock().unwrap();
        let session = shadow.as_ref().unwrap();
        assert_eq!(session.exit_summary(), (5, 100_300.0));
        assert_eq!(session.orders().len(), 4);
        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn shadow_과거_강제청산_주문은_다음거래의_intent로_동기화하지_않는다() {
        let broker = Arc::new(ShadowPostCountingBroker {
            post_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.account.write().unwrap().cash = 1_000_000;
        seed_shadow_quotes(&engine, 10, 10);

        engine
            .execute_shadow_entry(arm_shadow_trigger(&engine))
            .await;
        let target_partial = engine
            .shadow
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .on_trade_tick(ShadowTradeTick {
                product: ShadowProduct::Leverage,
                sequence: 1,
                price: 10_030,
                volume: 2,
                at: engine.automation_now() + 1,
            })
            .unwrap();
        assert!(!target_partial.position_closed);
        engine.apply_shadow_update(target_partial);

        let forced_close = engine
            .shadow
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .request_mode_exit(engine.automation_now() + 2)
            .unwrap();
        assert!(forced_close.position_closed);
        engine.apply_shadow_update(forced_close);
        let old_forced_order_id = engine
            .shadow
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .orders()
            .iter()
            .find_map(|order| {
                matches!(order.kind, ShadowOrderKind::ForcedExit(_)).then_some(order.id)
            })
            .expect("첫 거래 강제청산 주문");

        {
            let now = engine.automation_now();
            let mut quotes = engine.quotes.write().unwrap();
            let leverage = quotes.get_mut("0193T0").unwrap();
            leverage.price = 20_000.0;
            leverage.ask1 = 20_000.0;
            leverage.bid1 = 19_995.0;
            leverage.ask1_qty = 5;
            leverage.bid1_qty = 5;
            leverage.trade_ts = now;
            leverage.book_ts = now;
        }
        engine
            .execute_shadow_entry(arm_shadow_trigger(&engine))
            .await;
        let second_position = engine
            .automation
            .lock()
            .unwrap()
            .position()
            .cloned()
            .expect("두 번째 섀도 포지션");

        let no_fill = engine
            .shadow
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .on_trade_tick(ShadowTradeTick {
                product: ShadowProduct::Leverage,
                sequence: 1,
                price: 20_000,
                volume: 1,
                at: engine.automation_now() + 3,
            })
            .unwrap();
        assert!(no_fill.fill.is_none());
        engine.apply_shadow_update(no_fill);

        let wrong_intent = shadow_order_key(
            &second_position.trade_id,
            ShadowOrderKind::ForcedExit(ShadowExitReason::ModeExit),
            old_forced_order_id,
        );
        assert!(engine.ledger.get_order(&wrong_intent).unwrap().is_none());
        let target_intent = second_position.target_intent_id.unwrap();
        assert_eq!(
            engine
                .ledger
                .get_order(&target_intent)
                .unwrap()
                .unwrap()
                .status,
            LedgerOrderStatus::Submitted
        );
        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn manual_transition_wins_actor_race_and_discards_shadow_commit() {
        let broker = Arc::new(ShadowPostCountingBroker {
            post_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.account.write().unwrap().cash = 1_000_000;
        seed_shadow_quotes(&engine, 10, 10);
        let triggered = arm_shadow_trigger(&engine);

        let actor = engine.order_actor.lock().await;
        let transition_engine = Arc::clone(&engine);
        let transition = tokio::spawn(async move {
            transition_engine
                .set_control_mode(ControlMode::Manual)
                .await
        });
        tokio::task::yield_now().await;
        let entry_engine = Arc::clone(&engine);
        let entry = tokio::spawn(async move {
            entry_engine.execute_shadow_entry(triggered).await;
        });
        tokio::task::yield_now().await;
        drop(actor);

        transition.await.unwrap().unwrap();
        entry.await.unwrap();
        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.mode(), ControlMode::Manual);
        assert!(runtime.position().is_none());
        drop(runtime);
        assert!(engine.shadow.lock().unwrap().is_none());
        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);
        let decisions = engine
            .ledger
            .list_decisions(&crate::ledger::DecisionQuery::default(), None, 10)
            .unwrap();
        let scenario = &decisions.items[0].scenarios[0];
        assert_eq!(decisions.items[0].status, LedgerDecisionStatus::Triggered);
        assert_eq!(scenario.status, LedgerScenarioStatus::Triggered);
        assert_eq!(
            scenario.terminal_reason.as_deref(),
            Some("제어 모드 전환으로 주문 전 진입 폐기")
        );
    }

    #[tokio::test]
    async fn manual_transition_wins_auto_trigger_race_before_broker_post() {
        let broker = Arc::new(ShadowPostCountingBroker {
            post_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.account.write().unwrap().cash = 1_000_000;
        seed_shadow_quotes(&engine, 10, 10);
        let triggered = arm_auto_dual_trigger(&engine);

        let actor = engine.order_actor.lock().await;
        let transition_engine = Arc::clone(&engine);
        let transition = tokio::spawn(async move {
            transition_engine
                .set_control_mode(ControlMode::Manual)
                .await
        });
        tokio::task::yield_now().await;
        let entry_engine = Arc::clone(&engine);
        let entry = tokio::spawn(async move { entry_engine.execute_real_entry(triggered).await });
        tokio::task::yield_now().await;
        drop(actor);

        transition.await.unwrap().unwrap();
        entry.await.unwrap();
        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.mode(), ControlMode::Manual);
        assert!(runtime.position().is_none());
        assert!(runtime.pending_entry().is_none());
        drop(runtime);
        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);

        let decisions = engine
            .ledger
            .list_decisions(&crate::ledger::DecisionQuery::default(), None, 10)
            .unwrap();
        let decision = &decisions.items[0];
        let winner = decision
            .scenarios
            .iter()
            .find(|scenario| scenario.product == LedgerProductKind::Leverage)
            .unwrap();
        let loser = decision
            .scenarios
            .iter()
            .find(|scenario| scenario.product == LedgerProductKind::Inverse)
            .unwrap();
        assert_eq!(decision.status, LedgerDecisionStatus::Triggered);
        assert_eq!(winner.status, LedgerScenarioStatus::Triggered);
        assert_eq!(
            winner.terminal_reason.as_deref(),
            Some("제어 모드 전환으로 주문 전 진입 폐기")
        );
        assert_eq!(loser.status, LedgerScenarioStatus::CancelledByOco);
        assert_eq!(
            loser.terminal_reason.as_deref(),
            Some("반대 시나리오 진입 확정")
        );
    }

    #[tokio::test]
    async fn manual에서_auto와_shadow는_최종_상태를_저장한_뒤_확정_스냅샷을_반환한다() {
        for target in [ControlMode::Auto, ControlMode::Shadow] {
            let mut settings = Settings::default();
            settings.openai_api_key = "test-openai-key".into();
            let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);

            let snapshot = engine.set_control_mode(target).await.unwrap();

            assert_eq!(snapshot.mode, target);
            let runtime = engine.automation.lock().unwrap();
            assert_eq!(runtime.mode(), target);
            assert!(runtime.session_id().is_some());
            drop(runtime);
            assert_eq!(
                engine.ledger.get_control_mode().unwrap(),
                Some(ledger_control_mode(target))
            );
        }
    }

    #[tokio::test]
    async fn auto_사전조회_실패는_기존_manual_세션을_변경하지_않는다() {
        let broker = Arc::new(HandoffOpenOrdersFailBroker {
            open_orders_calls: AtomicUsize::new(0),
        });
        let mut settings = Settings::default();
        settings.openai_api_key = "test-openai-key".into();
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, settings);
        let before_revision = engine.automation.lock().unwrap().revision();

        let error = engine
            .set_control_mode(ControlMode::Auto)
            .await
            .unwrap_err();

        assert!(error.contains("미체결 조회 실패"));
        assert_eq!(broker.open_orders_calls.load(Ordering::SeqCst), 1);
        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.mode(), ControlMode::Manual);
        assert!(runtime.session_id().is_none());
        assert_eq!(runtime.revision(), before_revision);
        drop(runtime);
        assert_eq!(engine.ledger.get_control_mode().unwrap(), None);
    }

    #[test]
    fn 폐기된_엔진은_런타임을_장부에_다시_저장하지_않는다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine
            .automation
            .lock()
            .unwrap()
            .set_mode_after_cleanup(ControlMode::Auto, None, None);

        engine.deactivate();
        let error = engine.persist_automation_required().unwrap_err();

        assert!(error.contains("폐기된 엔진"));
        assert_eq!(engine.ledger.get_control_mode().unwrap(), None);
    }

    #[tokio::test]
    async fn mode_transition_replaces_armed_scenario_with_audit_reason() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        prepare_shadow_scenario(&engine);

        engine.set_control_mode(ControlMode::Manual).await.unwrap();

        let decisions = engine
            .ledger
            .list_decisions(&crate::ledger::DecisionQuery::default(), None, 10)
            .unwrap();
        let decision = &decisions.items[0];
        assert_eq!(decision.status, LedgerDecisionStatus::Replaced);
        assert_eq!(decision.scenarios[0].status, LedgerScenarioStatus::Replaced);
        assert_eq!(
            decision.scenarios[0].terminal_reason.as_deref(),
            Some("제어 모드 전환으로 폐기")
        );
        let snapshot = engine.automation_snapshot();
        assert_eq!(snapshot.mode, ControlMode::Manual);
        assert_eq!(snapshot.phase, crate::types::AutomationPhase::Idle);
        assert!(snapshot.scenarios.is_empty());
    }

    #[tokio::test]
    async fn pending_auto_entry_still_blocks_mode_transition() {
        let broker = Arc::new(ShadowPostCountingBroker {
            post_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        arm_auto_dual_trigger(&engine);
        assert!(engine
            .automation
            .lock()
            .unwrap()
            .set_pending_entry(pending_entry("pending-intent", "pending-trade")));
        let revision = engine.automation.lock().unwrap().revision();

        let error = engine
            .set_control_mode(ControlMode::Manual)
            .await
            .unwrap_err();

        assert!(error.contains("자동 진입 주문을 조정 중"));
        let snapshot = engine.automation_snapshot();
        assert_eq!(snapshot.mode, ControlMode::Auto);
        assert_eq!(snapshot.phase, crate::types::AutomationPhase::EntryPending);
        assert_eq!(snapshot.revision, revision);
        assert!(snapshot
            .scenarios
            .iter()
            .any(|scenario| scenario.status == crate::types::ScenarioStatus::Triggered));
        assert!(engine.automation.lock().unwrap().pending_entry().is_some());
        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn auto_to_manual_applies_target_fill_before_handoff() {
        let date = crate::util::now_kst().format("%Y%m%d").to_string();
        let now = crate::util::kst_str_to_fake_epoch(&date, "100000").unwrap();
        let broker = Arc::new(HandoffTargetFilledBroker {
            fill: BrokerFill {
                fill_id: "TARGET-FILL-1".into(),
                order_no: "TARGET-ORDER-1".into(),
                original_order_no: String::new(),
                org_no: "ORG-1".into(),
                code: "0193T0".into(),
                side: Side::Sell,
                qty: 10,
                price: 10_030.0,
                filled_at: now,
            },
            cancel_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine
            .account
            .write()
            .unwrap()
            .positions
            .push(crate::types::Position {
                code: "0193T0".into(),
                qty: 10,
                avg_price: 10_000.0,
                eval_pnl: 0.0,
                pnl_rate: 0.0,
            });

        let target_intent_id = "handoff-target-intent".to_string();
        engine
            .ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: target_intent_id.clone(),
                session_id: None,
                trade_id: Some("handoff-trade".into()),
                decision_id: None,
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                side: LedgerSide::Sell,
                order_type: LedgerOrderType::Limit,
                qty: 10,
                price: Some(10_030),
                requested_at: now,
            })
            .unwrap();
        engine
            .ledger
            .record_order_ack(
                &target_intent_id,
                &OrderAcknowledgement {
                    broker_order_id: Some("TARGET-ORDER-1".into()),
                    broker_org_no: Some("ORG-1".into()),
                    original_order_id: None,
                    status: LedgerOrderStatus::Submitted,
                    message: None,
                    acknowledged_at: now,
                },
            )
            .unwrap();
        engine.reservations.write().unwrap().insert(
            "0193T0".into(),
            Reservation {
                target_pct: 0.3,
                target_price: 10_030,
                qty: 10,
                order_no: "TARGET-ORDER-1".into(),
                org_no: "ORG-1".into(),
            },
        );
        {
            let mut runtime = engine.automation.lock().unwrap();
            runtime.set_mode_after_cleanup(ControlMode::Auto, None, None);
            runtime.mark_holding(OwnedPosition {
                product: ProductKind::Leverage,
                code: "0193T0".into(),
                entry_qty: 10,
                qty: 10,
                avg_price: 10_000.0,
                last_price: 10_000.0,
                target_return_pct: 0.3,
                target_price: 10_030,
                first_fill_at: now - 60,
                exit_deadline: now + 540,
                target_order_no: Some("TARGET-ORDER-1".into()),
                target_org_no: Some("ORG-1".into()),
                target_intent_id: Some(target_intent_id.clone()),
                exit_order_no: None,
                exit_order_org_no: None,
                exit_intent_id: None,
                exit_requested_at: None,
                pending_exit_reason: None,
                exit_qty: 0,
                exit_value: 0.0,
                trade_id: "handoff-trade".into(),
                shadow: false,
                profit_guard_armed: false,
            });
        }

        engine.set_control_mode(ControlMode::Manual).await.unwrap();

        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.mode(), ControlMode::Manual);
        assert!(
            runtime.position().is_none(),
            "전량 체결 포지션을 인계하면 안 됨"
        );
        drop(runtime);
        assert_eq!(broker.cancel_calls.load(Ordering::SeqCst), 1);
        assert!(engine.reservations.read().unwrap().is_empty());
        assert!(engine
            .ledger
            .get_runtime_state::<ManualTradeAccumulator>("manual_trade:0193T0")
            .unwrap()
            .is_none());
        assert_eq!(
            engine
                .ledger
                .get_order(&target_intent_id)
                .unwrap()
                .unwrap()
                .status,
            LedgerOrderStatus::Filled
        );
        let trades = engine
            .ledger
            .list_trades(&crate::ledger::TradeQuery::default(), None, 10)
            .unwrap();
        let trade = trades
            .items
            .iter()
            .find(|trade| trade.trade_id == "handoff-trade")
            .unwrap();
        assert_eq!(trade.status, LedgerTradeStatus::Closed);
        assert_eq!(trade.origin, LedgerOrigin::Auto);
        assert_eq!(trade.exit_qty, 10);
        assert_eq!(trade.exit_reason.as_deref(), Some("target"));
    }

    #[tokio::test]
    async fn auto_인계는_목표취소_post보다_먼저_복구표식과_상태를_저장한다() {
        let date = crate::util::now_kst().format("%Y%m%d").to_string();
        let now = crate::util::kst_str_to_fake_epoch(&date, "100000").unwrap();
        let target_intent_id = "durable-handoff-target".to_string();
        let target_order_no = "DURABLE-TARGET".to_string();
        let broker = Arc::new(DurableHandoffBroker {
            ledger: std::sync::OnceLock::new(),
            status: BrokerOrderStatus {
                trading_date: date,
                order_no: target_order_no.clone(),
                original_order_no: String::new(),
                org_no: "DURABLE-ORG".into(),
                code: "0193T0".into(),
                side: Side::Sell,
                ordered_qty: 10,
                filled_qty: 0,
                cancelled_qty: 10,
                remaining_qty: 0,
                rejected_qty: 0,
                avg_fill_price: 0.0,
                ordered_at: now,
            },
            account_qty: 10,
            cancel_calls: AtomicUsize::new(0),
            marker_seen_at_cancel: AtomicBool::new(false),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        assert!(broker.ledger.set(Arc::clone(&engine.ledger)).is_ok());
        engine
            .ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: target_intent_id.clone(),
                session_id: None,
                trade_id: Some("durable-handoff-trade".into()),
                decision_id: None,
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                side: LedgerSide::Sell,
                order_type: LedgerOrderType::Limit,
                qty: 10,
                price: Some(10_030),
                requested_at: now,
            })
            .unwrap();
        engine
            .ledger
            .record_order_ack(
                &target_intent_id,
                &OrderAcknowledgement {
                    broker_order_id: Some(target_order_no.clone()),
                    broker_org_no: Some("DURABLE-ORG".into()),
                    original_order_id: None,
                    status: LedgerOrderStatus::Submitted,
                    message: None,
                    acknowledged_at: now,
                },
            )
            .unwrap();
        {
            let mut runtime = engine.automation.lock().unwrap();
            runtime.set_mode_after_cleanup(ControlMode::Auto, None, None);
            runtime.mark_holding(OwnedPosition {
                product: ProductKind::Leverage,
                code: "0193T0".into(),
                entry_qty: 10,
                qty: 10,
                avg_price: 10_000.0,
                last_price: 10_000.0,
                target_return_pct: 0.3,
                target_price: 10_030,
                first_fill_at: now - 60,
                exit_deadline: now + 540,
                target_order_no: Some(target_order_no),
                target_org_no: Some("DURABLE-ORG".into()),
                target_intent_id: Some(target_intent_id),
                exit_order_no: None,
                exit_order_org_no: None,
                exit_intent_id: None,
                exit_requested_at: None,
                pending_exit_reason: None,
                exit_qty: 0,
                exit_value: 0.0,
                trade_id: "durable-handoff-trade".into(),
                shadow: false,
                profit_guard_armed: false,
            });
        }

        engine.set_control_mode(ControlMode::Manual).await.unwrap();

        assert_eq!(broker.cancel_calls.load(Ordering::SeqCst), 1);
        assert!(broker.marker_seen_at_cancel.load(Ordering::SeqCst));
        assert!(engine
            .ledger
            .get_runtime_state::<PendingAutoHandoff>(AUTO_HANDOFF_STATE_KEY)
            .unwrap()
            .is_none());
        assert_eq!(
            engine.automation.lock().unwrap().mode(),
            ControlMode::Manual
        );
    }

    #[tokio::test]
    async fn 재시작한_auto_인계는_스케줄러전에_manual_소유권이전을_완료한다() {
        let date = crate::util::now_kst().format("%Y%m%d").to_string();
        let now = crate::util::kst_str_to_fake_epoch(&date, "100000").unwrap();
        let broker = Arc::new(DurableHandoffBroker {
            ledger: std::sync::OnceLock::new(),
            status: BrokerOrderStatus {
                trading_date: date,
                order_no: "ALREADY-CANCELLED".into(),
                original_order_no: String::new(),
                org_no: "DURABLE-ORG".into(),
                code: "0193T0".into(),
                side: Side::Sell,
                ordered_qty: 17,
                filled_qty: 0,
                cancelled_qty: 17,
                remaining_qty: 0,
                rejected_qty: 0,
                avg_fill_price: 0.0,
                ordered_at: now,
            },
            account_qty: 17,
            cancel_calls: AtomicUsize::new(0),
            marker_seen_at_cancel: AtomicBool::new(false),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        assert!(broker.ledger.set(Arc::clone(&engine.ledger)).is_ok());
        {
            let mut runtime = engine.automation.lock().unwrap();
            runtime.set_mode_after_cleanup(ControlMode::Auto, None, None);
            runtime.mark_holding(recovered_position("restart-handoff", None));
        }
        engine.persist_automation_required().unwrap();
        engine
            .ledger
            .set_runtime_state(
                AUTO_HANDOFF_STATE_KEY,
                &PendingAutoHandoff {
                    target_mode: ControlMode::Manual,
                    requested_at: now,
                },
                now,
            )
            .unwrap();

        // 저장된 bundle로 런타임을 다시 만든 뒤, 시작 조정이 불일치로 Suspended에 둔
        // 상태를 재현한다. 인계 복구는 이 포지션을 Auto 마감청산으로 넘기지 않는다.
        let saved = engine
            .ledger
            .get_runtime_state::<PersistedAutomationBundle>(AUTOMATION_BUNDLE_STATE_KEY)
            .unwrap()
            .unwrap()
            .automation;
        let mut restored = AutomationRuntime::new(saved, None);
        restored.finish_reconcile(Some("목표 주문은 이미 종결됨".into()));
        *engine.automation.lock().unwrap() = restored;

        engine.resume_pending_auto_handoff().await.unwrap();

        let mut runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.mode(), ControlMode::Manual);
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::Idle);
        assert!(runtime.position().is_none());
        assert!(runtime.begin_market_close_exit().is_none());
        drop(runtime);
        assert_eq!(broker.cancel_calls.load(Ordering::SeqCst), 0);
        assert!(engine
            .ledger
            .get_runtime_state::<PendingAutoHandoff>(AUTO_HANDOFF_STATE_KEY)
            .unwrap()
            .is_none());
        assert!(engine
            .ledger
            .get_runtime_state::<ManualTradeAccumulator>("manual_trade:0193T0")
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn auto_to_manual_rejects_unresolved_target_order_identity() {
        let broker = Arc::new(ShadowPostCountingBroker {
            post_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        let now = now_kst_fake_epoch();
        {
            let mut runtime = engine.automation.lock().unwrap();
            runtime.set_mode_after_cleanup(ControlMode::Auto, None, None);
            runtime.mark_holding(OwnedPosition {
                product: ProductKind::Leverage,
                code: "0193T0".into(),
                entry_qty: 10,
                qty: 10,
                avg_price: 10_000.0,
                last_price: 10_000.0,
                target_return_pct: 0.3,
                target_price: 10_030,
                first_fill_at: now,
                exit_deadline: now + 600,
                target_order_no: None,
                target_org_no: None,
                target_intent_id: Some("unresolved-target-intent".into()),
                exit_order_no: None,
                exit_order_org_no: None,
                exit_intent_id: None,
                exit_requested_at: None,
                pending_exit_reason: None,
                exit_qty: 0,
                exit_value: 0.0,
                trade_id: "unresolved-target-trade".into(),
                shadow: false,
                profit_guard_armed: false,
            });
        }

        let error = engine
            .set_control_mode(ControlMode::Manual)
            .await
            .unwrap_err();

        assert!(error.contains("목표 주문 응답을 조정 중"));
        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.mode(), ControlMode::Auto);
        assert!(runtime.position().is_some());
        drop(runtime);
        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn auto_인계중_미체결조회가_실패하면_manual_shadow_모두_holding을_복원한다() {
        for target_mode in [ControlMode::Manual, ControlMode::Shadow] {
            let broker = Arc::new(HandoffOpenOrdersFailBroker {
                open_orders_calls: AtomicUsize::new(0),
            });
            let mut settings = Settings::default();
            settings.openai_api_key = "test-openai-key".into();
            let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, settings);
            {
                let mut runtime = engine.automation.lock().unwrap();
                runtime.set_mode_after_cleanup(ControlMode::Auto, None, None);
                runtime.mark_holding(recovered_position("handoff-failure", None));
            }

            let error = engine.set_control_mode(target_mode).await.unwrap_err();

            assert!(error.contains("미체결 조회 실패"));
            assert_eq!(broker.open_orders_calls.load(Ordering::SeqCst), 1);
            let runtime = engine.automation.lock().unwrap();
            assert_eq!(runtime.mode(), ControlMode::Auto);
            assert_eq!(runtime.phase(), crate::types::AutomationPhase::Holding);
            assert!(runtime.position().is_some());
            assert!(!runtime.can_begin_analysis());
        }
    }

    #[tokio::test]
    async fn auto_무포지션_인계실패는_idle이_아닌_suspended로_신규진입을_막는다() {
        let broker = Arc::new(HandoffOpenOrdersFailBroker {
            open_orders_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine
            .automation
            .lock()
            .unwrap()
            .set_mode_after_cleanup(ControlMode::Auto, None, None);

        let error = engine
            .set_control_mode(ControlMode::Manual)
            .await
            .unwrap_err();

        assert!(error.contains("미체결 조회 실패"));
        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.mode(), ControlMode::Auto);
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::Suspended);
        assert!(runtime.position().is_none());
        assert!(!runtime.can_begin_analysis());
    }

    #[tokio::test]
    async fn shadow_forced_exit_records_intent_before_fill_and_syncs_statuses() {
        let broker = Arc::new(ShadowPostCountingBroker {
            post_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.account.write().unwrap().cash = 1_000_000;
        seed_shadow_quotes(&engine, 10, 2);
        let triggered = arm_shadow_trigger(&engine);
        engine.execute_shadow_entry(triggered).await;

        let position = engine
            .automation
            .lock()
            .unwrap()
            .position()
            .cloned()
            .unwrap();
        let target_intent = position.target_intent_id.clone().unwrap();
        let now = engine.automation_now();
        let first = engine
            .shadow
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .request_mode_exit(now)
            .unwrap();
        assert_eq!(first.fill.as_ref().map(|fill| fill.qty), Some(2));
        engine.apply_shadow_update(first);

        let forced = engine
            .shadow
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .orders()
            .iter()
            .find(|order| matches!(order.kind, ShadowOrderKind::ForcedExit(_)))
            .cloned()
            .unwrap();
        let forced_intent = shadow_intent_for_order(&position, &forced);
        assert_eq!(
            engine
                .ledger
                .get_order(&target_intent)
                .unwrap()
                .unwrap()
                .status,
            LedgerOrderStatus::Cancelled
        );
        assert_eq!(
            engine
                .ledger
                .get_order(&forced_intent)
                .unwrap()
                .unwrap()
                .status,
            LedgerOrderStatus::PartiallyFilled
        );

        let final_update = engine
            .shadow
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .on_book(ShadowBook {
                product: ShadowProduct::Leverage,
                sequence: u64::MAX,
                ask_price: 10_000,
                ask_qty: 10,
                bid_price: 9_995,
                bid_qty: 100,
                observed_at: now,
            })
            .unwrap();
        assert!(final_update.position_closed);
        engine.apply_shadow_update(final_update);
        assert_eq!(
            engine
                .ledger
                .get_order(&forced_intent)
                .unwrap()
                .unwrap()
                .status,
            LedgerOrderStatus::Filled
        );
        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn shadow_partial_exit_restart_restores_totals_session_and_orders() {
        let broker = Arc::new(ShadowPostCountingBroker {
            post_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.account.write().unwrap().cash = 1_000_000;
        seed_shadow_quotes(&engine, 10, 2);
        let triggered = arm_shadow_trigger(&engine);
        engine.execute_shadow_entry(triggered).await;

        let now = engine.automation_now();
        let first = engine
            .shadow
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .request_mode_exit(now)
            .unwrap();
        engine.apply_shadow_update(first);

        let bundle = engine
            .ledger
            .get_runtime_state::<PersistedAutomationBundle>(AUTOMATION_BUNDLE_STATE_KEY)
            .unwrap()
            .expect("부분청산 통합 스냅샷");
        let saved_position = bundle.automation.position.as_ref().unwrap();
        assert_eq!(saved_position.qty, 8);
        assert_eq!(saved_position.exit_qty, 2);
        assert_eq!(saved_position.exit_value, 19_990.0);
        let saved_exit_reason = saved_position
            .pending_exit_reason
            .clone()
            .expect("강제청산 사유 저장");
        assert!(saved_position.exit_intent_id.is_some());
        assert_eq!(bundle.automation.shadow_cash, Some(919_990));
        let saved_trade_id = saved_position.trade_id.clone();

        let mut restored_session = bundle.shadow.expect("섬도 세션 스냅샷");
        let before_restart_orders = restored_session.orders().to_vec();
        assert_eq!(restored_session.exit_summary(), (2, 19_990.0));
        assert!(before_restart_orders
            .iter()
            .any(|order| order.status == ShadowOrderStatus::PartiallyFilled));
        restored_session.prepare_for_restart();

        let mut restored_runtime = AutomationRuntime::new(bundle.automation, None);
        restored_runtime.finish_reconcile(None);
        assert_eq!(
            restored_runtime.phase(),
            crate::types::AutomationPhase::ExitPending
        );
        *engine.automation.lock().unwrap() = restored_runtime;
        *engine.shadow.lock().unwrap() = Some(restored_session);

        // 새 프로세스의 체결 순번은 1부터 다시 시작해도 정상적으로 남은 수량을 청산한다.
        let completed = engine
            .shadow
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .on_book(ShadowBook {
                product: ShadowProduct::Leverage,
                sequence: 1,
                ask_price: 10_000,
                ask_qty: 10,
                bid_price: 9_990,
                bid_qty: 100,
                observed_at: now + 1,
            })
            .unwrap();
        assert!(completed.position_closed);
        engine.apply_shadow_update(completed);

        let trades = engine
            .ledger
            .list_trades(&crate::ledger::TradeQuery::default(), None, 10)
            .unwrap();
        let closed = trades
            .items
            .iter()
            .find(|trade| trade.trade_id == saved_trade_id)
            .unwrap();
        assert_eq!(closed.exit_qty, 10);
        assert_eq!(closed.exit_avg_price, Some(9_991.0));
        assert_eq!(
            closed.exit_reason.as_deref(),
            Some(saved_exit_reason.as_str())
        );

        let final_bundle = engine
            .ledger
            .get_runtime_state::<PersistedAutomationBundle>(AUTOMATION_BUNDLE_STATE_KEY)
            .unwrap()
            .unwrap();
        assert!(final_bundle.automation.position.is_none());
        let final_session = final_bundle.shadow.unwrap();
        assert!(final_session.position().is_none());
        assert_eq!(final_session.exit_summary(), (10, 99_910.0));
        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);
    }

    #[async_trait::async_trait]
    impl Broker for CountingBroker {
        async fn market_days(&self, basis_date: &str) -> AppResult<Vec<BrokerMarketDay>> {
            let call = self.market_day_calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                if let Some(blocker) = &self.block_first_market_day {
                    blocker.notified().await;
                }
            }
            if self.market_error.load(Ordering::SeqCst) {
                return Err(AppError::Kis("휴장일 테스트 오류".into()));
            }
            Ok(vec![BrokerMarketDay {
                date: basis_date.to_string(),
                is_open: self.market_open.load(Ordering::SeqCst),
            }])
        }

        async fn candles_1m(&self, _code: &str) -> AppResult<Vec<Candle>> {
            Ok(Vec::new())
        }
        async fn account(&self) -> AppResult<AccountSnapshot> {
            self.account_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(AccountSnapshot {
                cash: 0,
                positions: Vec::new(),
            })
        }
        async fn snapshot(&self, _code: &str) -> AppResult<Quote> {
            Err(AppError::Config("테스트에서 미사용".into()))
        }
        async fn max_buy_qty(&self, _code: &str, _limit_price: u64) -> AppResult<u64> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn place_buy(&self, _c: &str, _q: u64, _p: u64, _i: bool) -> AppResult<OrderAck> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn place_sell_market(&self, _c: &str, _q: u64) -> AppResult<OrderAck> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn place_sell_limit(&self, _c: &str, _q: u64, _p: u64) -> AppResult<OrderAck> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn cancel_order(&self, _c: &str, _o: &str, _g: &str) -> AppResult<OrderAck> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn start_feed(
            &self,
            _codes: Vec<String>,
            _tx: mpsc::Sender<FeedEvent>,
            _reconnect: std::sync::Arc<tokio::sync::Notify>,
        ) -> AppResult<Vec<JoinHandle<()>>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn 개장일_조회는_같은날_한번만_호출하고_결과를_영속화한다() {
        let broker = Arc::new(CountingBroker {
            account_calls: AtomicUsize::new(0),
            market_day_calls: AtomicUsize::new(0),
            market_open: AtomicBool::new(true),
            market_error: AtomicBool::new(false),
            block_first_market_day: None,
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        let date = market_date_info(engine.automation_now()).unwrap().0;
        *engine.market_day.write().unwrap() = PersistedMarketDayState {
            date,
            status: MarketDayStatus::Unknown,
            message: None,
            last_attempt_at: 0,
        };

        engine.refresh_market_day_if_needed().await;
        engine.refresh_market_day_if_needed().await;

        assert_eq!(broker.market_day_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            engine.effective_market_day_state().status,
            MarketDayStatus::Open
        );
        let saved = engine
            .ledger
            .get_runtime_state::<PersistedMarketDayState>(MARKET_DAY_STATE_KEY)
            .unwrap()
            .unwrap();
        assert_eq!(saved.status, MarketDayStatus::Open);
    }

    #[tokio::test]
    async fn 개장일_조회실패는_재시도간격_동안_재호출하지_않고_신규진입을_막는다() {
        let broker = Arc::new(CountingBroker {
            account_calls: AtomicUsize::new(0),
            market_day_calls: AtomicUsize::new(0),
            market_open: AtomicBool::new(true),
            market_error: AtomicBool::new(true),
            block_first_market_day: None,
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        let date = market_date_info(engine.automation_now()).unwrap().0;
        *engine.market_day.write().unwrap() = PersistedMarketDayState {
            date,
            status: MarketDayStatus::Unknown,
            message: None,
            last_attempt_at: 0,
        };

        engine.refresh_market_day_if_needed().await;
        engine.refresh_market_day_if_needed().await;

        assert_eq!(broker.market_day_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            engine.effective_market_day_state().status,
            MarketDayStatus::Unknown
        );
        assert!(!engine.market_is_open());
    }

    #[test]
    fn 저장된_개장일_확인중_상태는_시작즉시_재조회가능하게_복구한다() {
        let ledger = Ledger::open_in_memory().unwrap();
        let now = crate::util::kst_str_to_fake_epoch("20260723", "100000").unwrap();
        let date = market_date_info(now).unwrap().0;
        ledger
            .set_runtime_state(
                MARKET_DAY_STATE_KEY,
                &PersistedMarketDayState {
                    date,
                    status: MarketDayStatus::Unknown,
                    message: Some("개장일 여부를 확인하고 있습니다".into()),
                    last_attempt_at: now,
                },
                now,
            )
            .unwrap();

        let restored = initial_market_day_state(&ledger, now).unwrap();

        assert_eq!(restored.status, MarketDayStatus::Unknown);
        assert_eq!(restored.last_attempt_at, 0);
        assert!(restored.message.unwrap().contains("다시 확인"));
    }

    #[tokio::test]
    async fn 취소된_개장일_조회는_확인중을_영속화하지_않고_즉시_재시도한다() {
        let blocker = Arc::new(tokio::sync::Notify::new());
        let broker = Arc::new(CountingBroker {
            account_calls: AtomicUsize::new(0),
            market_day_calls: AtomicUsize::new(0),
            market_open: AtomicBool::new(true),
            market_error: AtomicBool::new(false),
            block_first_market_day: Some(blocker),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        let now = crate::util::kst_str_to_fake_epoch("20260723", "100000").unwrap();
        engine.automation_now_override.store(now, Ordering::SeqCst);
        *engine.market_day.write().unwrap() = PersistedMarketDayState {
            date: market_date_info(now).unwrap().0,
            status: MarketDayStatus::Unknown,
            message: None,
            last_attempt_at: 0,
        };

        let running = tokio::spawn({
            let engine = Arc::clone(&engine);
            async move { engine.refresh_market_day_if_needed().await }
        });
        for _ in 0..100 {
            if broker.market_day_calls.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(broker.market_day_calls.load(Ordering::SeqCst), 1);
        assert!(engine
            .ledger
            .get_runtime_state::<PersistedMarketDayState>(MARKET_DAY_STATE_KEY)
            .unwrap()
            .is_none());

        running.abort();
        let _ = running.await;
        engine.refresh_market_day_if_needed().await;

        assert_eq!(broker.market_day_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            engine.effective_market_day_state().status,
            MarketDayStatus::Open
        );
    }

    #[tokio::test]
    async fn 개장일_unknown은_세종목의_신선한_krx시세로만_자동슬롯을_복구한다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        let now = crate::util::kst_str_to_fake_epoch("20260723", "100701").unwrap();
        engine.automation_now_override.store(now, Ordering::SeqCst);
        *engine.market_day.write().unwrap() = PersistedMarketDayState {
            date: market_date_info(now).unwrap().0,
            status: MarketDayStatus::Unknown,
            message: Some("KIS 개장일 확인 실패".into()),
            last_attempt_at: now,
        };
        seed_shadow_quotes(&engine, 10_000, 10_000);
        // seed 헬퍼가 주입 시각을 사용하므로 세 종목의 체결·호가가 모두 신선하다.
        engine
            .automation
            .lock()
            .unwrap()
            .set_mode_after_cleanup(ControlMode::Auto, None, None);

        assert_eq!(
            engine.effective_market_day_state().status,
            MarketDayStatus::OpenByQuotes
        );
        assert!(engine.market_is_open());
        engine.automation_tick().await;
        assert_eq!(
            engine.automation.lock().unwrap().next_decision_at(),
            schedule::next_decision_slot(now)
        );
        assert!(engine
            .ledger
            .get_runtime_state::<PersistedMarketDayState>(MARKET_DAY_STATE_KEY)
            .unwrap()
            .is_none());

        engine.quotes.write().unwrap().remove("0197X0");
        assert_eq!(
            engine.effective_market_day_state().status,
            MarketDayStatus::Unknown
        );
        assert!(!engine.market_is_open());

        seed_shadow_quotes(&engine, 10_000, 10_000);
        engine.connected.store(false, Ordering::SeqCst);
        assert_eq!(
            engine.effective_market_day_state().status,
            MarketDayStatus::Unknown
        );
    }

    #[test]
    fn 명시적_휴장과_주말은_신선한_시세로_덮어쓰지_않는다() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        let thursday = crate::util::kst_str_to_fake_epoch("20260723", "100000").unwrap();
        engine
            .automation_now_override
            .store(thursday, Ordering::SeqCst);
        seed_shadow_quotes(&engine, 10_000, 10_000);
        *engine.market_day.write().unwrap() = PersistedMarketDayState {
            date: market_date_info(thursday).unwrap().0,
            status: MarketDayStatus::Closed,
            message: Some("KRX 휴장일입니다".into()),
            last_attempt_at: thursday,
        };
        assert_eq!(
            engine.effective_market_day_state().status,
            MarketDayStatus::Closed
        );

        let saturday = crate::util::kst_str_to_fake_epoch("20260725", "100000").unwrap();
        engine
            .automation_now_override
            .store(saturday, Ordering::SeqCst);
        seed_shadow_quotes(&engine, 10_000, 10_000);
        assert_eq!(
            engine.effective_market_day_state().status,
            MarketDayStatus::Closed
        );
    }

    #[test]
    fn 주말은_kis_조회없이_즉시_휴장으로_판정한다() {
        let ledger = Ledger::open_in_memory().unwrap();
        let saturday = crate::util::kst_str_to_fake_epoch("20260725", "100000").unwrap();
        let state = initial_market_day_state(&ledger, saturday).unwrap();
        assert_eq!(state.status, MarketDayStatus::Closed);
        assert!(state.message.unwrap().contains("주말"));
    }

    #[tokio::test]
    async fn 휴장일에는_판단슬롯을_비우고_다음_개장일에_자동복구한다() {
        let broker = Arc::new(CountingBroker {
            account_calls: AtomicUsize::new(0),
            market_day_calls: AtomicUsize::new(0),
            market_open: AtomicBool::new(false),
            market_error: AtomicBool::new(false),
            block_first_market_day: None,
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        // 오늘이 금요일이면 단순 +1일은 주말이 되어 복구 경로가 아니라 주말 단락을
        // 타므로, 연속된 평일인 목요일과 금요일로 테스트 시각을 고정한다.
        let now = crate::util::kst_str_to_fake_epoch("20260723", "100000").unwrap();
        engine.automation_now_override.store(now, Ordering::SeqCst);
        let date = market_date_info(now).unwrap().0;
        engine.automation.lock().unwrap().set_mode_after_cleanup(
            ControlMode::Auto,
            schedule::next_decision_slot(now),
            None,
        );
        *engine.market_day.write().unwrap() = PersistedMarketDayState {
            date,
            status: MarketDayStatus::Unknown,
            message: None,
            last_attempt_at: 0,
        };

        engine.refresh_market_day_if_needed().await;
        assert_eq!(
            engine.effective_market_day_state().status,
            MarketDayStatus::Closed
        );
        assert!(engine
            .automation
            .lock()
            .unwrap()
            .next_decision_at()
            .is_none());

        broker.market_open.store(true, Ordering::SeqCst);
        engine
            .automation_now_override
            .store(now + 86_400, Ordering::SeqCst);
        engine.refresh_market_day_if_needed().await;
        assert_eq!(
            engine.effective_market_day_state().status,
            MarketDayStatus::Open
        );
        assert!(engine
            .automation
            .lock()
            .unwrap()
            .next_decision_at()
            .is_some());
        assert_eq!(broker.market_day_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn 휴장판정과_경합한_실전진입은_주문직전에_폐기한다() {
        let broker = auto_entry_broker(10, None);
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        seed_shadow_quotes(&engine, 100_000, 100_000);
        let triggered = arm_auto_dual_trigger(&engine);
        let date = market_date_info(engine.automation_now()).unwrap().0;
        *engine.market_day.write().unwrap() = PersistedMarketDayState {
            date,
            status: MarketDayStatus::Closed,
            message: Some("테스트 휴장일".into()),
            last_attempt_at: engine.automation_now(),
        };

        Arc::clone(&engine).execute_real_entry(triggered).await;

        assert_eq!(broker.buy_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            engine.automation.lock().unwrap().phase(),
            crate::types::AutomationPhase::Idle
        );
    }

    #[tokio::test]
    async fn 휴장중_청산필요상태는_보존하지만_새_post는_보내지_않는다() {
        let broker = Arc::new(ShadowPostCountingBroker {
            post_calls: AtomicUsize::new(0),
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        let now = engine.automation_now();
        {
            let mut runtime = engine.automation.lock().unwrap();
            runtime.set_mode_after_cleanup(ControlMode::Auto, None, None);
            runtime.mark_holding(recovered_position("closed-day-exit", None));
            assert!(runtime.begin_exit("profit_guard").is_some());
        }
        let date = market_date_info(now).unwrap().0;
        *engine.market_day.write().unwrap() = PersistedMarketDayState {
            date,
            status: MarketDayStatus::Closed,
            message: Some("테스트 휴장일".into()),
            last_attempt_at: now,
        };

        Arc::clone(&engine).execute_auto_exit("profit_guard").await;

        assert_eq!(broker.post_calls.load(Ordering::SeqCst), 0);
        let runtime = engine.automation.lock().unwrap();
        assert_eq!(runtime.phase(), crate::types::AutomationPhase::ExitPending);
        assert_eq!(runtime.exit_reason(), Some("profit_guard"));
        assert!(runtime.position().is_some());
    }

    /// 부분체결 연발 등으로 갱신 예약이 몰려도 실제 잔고 조회는 1회로 합류돼야 한다
    #[tokio::test]
    async fn burst_refresh_requests_coalesce_into_one() {
        let broker = Arc::new(CountingBroker {
            account_calls: AtomicUsize::new(0),
            market_day_calls: AtomicUsize::new(0),
            market_open: AtomicBool::new(true),
            market_error: AtomicBool::new(false),
            block_first_market_day: None,
        });
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());

        for _ in 0..5 {
            engine.schedule_account_refresh();
        }
        tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
        assert_eq!(
            broker
                .account_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "연속 예약은 1회 조회로 합류돼야 한다"
        );
    }

    /// 첫 매수는 거부하고 두 번째부터 접수하는 브로커 (거부 시 재주문 검증용)
    struct RejectFirstBuyBroker {
        psbl_qty: u64,
        reject_message: &'static str,
        buy_calls: AtomicUsize,
        psbl_calls: AtomicUsize,
        last_order: Mutex<Option<(u64, u64)>>, // (qty, price)
    }

    impl RejectFirstBuyBroker {
        fn new(psbl_qty: u64) -> Arc<Self> {
            Self::with_message(psbl_qty, REJECT_MSG)
        }

        fn with_message(psbl_qty: u64, reject_message: &'static str) -> Arc<Self> {
            Arc::new(Self {
                psbl_qty,
                reject_message,
                buy_calls: AtomicUsize::new(0),
                psbl_calls: AtomicUsize::new(0),
                last_order: Mutex::new(None),
            })
        }
    }

    const REJECT_MSG: &str = "주문가능금액을 초과했습니다";

    #[async_trait::async_trait]
    impl Broker for RejectFirstBuyBroker {
        async fn candles_1m(&self, _code: &str) -> AppResult<Vec<Candle>> {
            Ok(Vec::new())
        }
        async fn account(&self) -> AppResult<AccountSnapshot> {
            Ok(AccountSnapshot {
                cash: 1_000_000,
                positions: Vec::new(),
            })
        }
        async fn snapshot(&self, _code: &str) -> AppResult<Quote> {
            Err(AppError::Config("테스트에서 미사용".into()))
        }
        async fn max_buy_qty(&self, _code: &str, _limit_price: u64) -> AppResult<u64> {
            self.psbl_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.psbl_qty)
        }
        async fn place_buy(&self, _c: &str, qty: u64, price: u64, _i: bool) -> AppResult<OrderAck> {
            if self.buy_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(AppError::Order(self.reject_message.into()));
            }
            *self.last_order.lock().unwrap() = Some((qty, price));
            Ok(OrderAck {
                order_no: "RETRY-BUY".into(),
                org_no: "RETRY-ORG".into(),
                message: "주문 접수".into(),
            })
        }
        async fn place_sell_market(&self, _c: &str, _q: u64) -> AppResult<OrderAck> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn place_sell_limit(&self, _c: &str, _q: u64, _p: u64) -> AppResult<OrderAck> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn cancel_order(&self, _c: &str, _o: &str, _g: &str) -> AppResult<OrderAck> {
            Err(AppError::Order("테스트에서 미사용".into()))
        }
        async fn start_feed(
            &self,
            _codes: Vec<String>,
            _tx: mpsc::Sender<FeedEvent>,
            _reconnect: std::sync::Arc<tokio::sync::Notify>,
        ) -> AppResult<Vec<JoinHandle<()>>> {
            Ok(Vec::new())
        }
    }

    /// 캐시 예수금이 부풀려져 거부돼도 KIS 매수가능수량으로 1회 재주문해 체결시킨다
    #[tokio::test]
    async fn rejected_buy_retries_with_kis_psbl_qty() {
        let broker = RejectFirstBuyBroker::new(90); // 캐시 기준 94주 < 실제 허용 90주
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.refresh_account().await;
        seed_quote(&engine, 10_100.0);

        let result = engine.buy_max("0193T0").await;

        assert!(result.ok, "{}", result.message);
        assert_eq!(result.qty, 90, "KIS가 계산한 수량으로 재주문해야 한다");
        assert_eq!(result.price, 10_400);
        assert_eq!(broker.buy_calls.load(Ordering::SeqCst), 2);
        assert_eq!(broker.psbl_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*broker.last_order.lock().unwrap(), Some((90, 10_400)));
    }

    /// 매수가능수량이 0이면 재주문 없이 명확한 실패를 돌려준다
    #[tokio::test]
    async fn rejected_buy_with_zero_psbl_fails_without_retry() {
        let broker = RejectFirstBuyBroker::new(0);
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.refresh_account().await;
        seed_quote(&engine, 10_100.0);

        let result = engine.buy_max("0193T0").await;

        assert!(!result.ok);
        assert!(
            result.message.contains("주문가능금액 부족"),
            "{}",
            result.message
        );
        assert_eq!(
            broker.buy_calls.load(Ordering::SeqCst),
            1,
            "재주문하면 안 된다"
        );
        assert_eq!(broker.psbl_calls.load(Ordering::SeqCst), 1);
    }

    /// 자금 부족 거부여도 KIS 가능수량이 줄지 않으면 원래 거부를 그대로 노출한다
    #[tokio::test]
    async fn rejected_buy_without_reduced_psbl_keeps_original_error() {
        let broker = RejectFirstBuyBroker::new(1_000);
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.refresh_account().await;
        seed_quote(&engine, 10_100.0);

        let result = engine.buy_max("0193T0").await;

        assert!(!result.ok);
        assert!(result.message.contains(REJECT_MSG), "{}", result.message);
        assert_eq!(broker.buy_calls.load(Ordering::SeqCst), 1);
        assert_eq!(broker.psbl_calls.load(Ordering::SeqCst), 1);
    }

    /// 자금과 무관한 업무거부는 가능수량이 줄어도 조회·재주문하지 않는다
    #[tokio::test]
    async fn rejected_buy_for_other_reason_keeps_original_error() {
        const HALT_MESSAGE: &str = "매매정지 종목입니다";
        let broker = RejectFirstBuyBroker::with_message(90, HALT_MESSAGE);
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.refresh_account().await;
        seed_quote(&engine, 10_100.0);

        let result = engine.buy_max("0193T0").await;

        assert!(!result.ok);
        assert!(result.message.contains(HALT_MESSAGE), "{}", result.message);
        assert_eq!(
            broker.buy_calls.load(Ordering::SeqCst),
            1,
            "재주문하면 안 된다"
        );
        assert_eq!(
            broker.psbl_calls.load(Ordering::SeqCst),
            0,
            "자금 부족 거부가 아니면 KIS 가능수량도 조회하면 안 된다"
        );
    }

    #[tokio::test]
    async fn rate_limited_buy_does_not_retry_with_kis_psbl_qty() {
        let broker = RejectFirstBuyBroker::with_message(
            90,
            "KIS 게이트웨이 유량 제한으로 요청이 접수되지 않았습니다: EGW00201",
        );
        let engine = test_engine(Arc::clone(&broker) as Arc<dyn Broker>, Settings::default());
        engine.refresh_account().await;
        seed_quote(&engine, 10_100.0);

        let result = engine.buy_max("0193T0").await;

        assert!(!result.ok);
        assert!(result.message.contains("EGW00201"), "{}", result.message);
        assert_eq!(
            broker.buy_calls.load(Ordering::SeqCst),
            1,
            "유량 제한 뒤 매수 POST를 다시 보내면 안 된다"
        );
        assert_eq!(
            broker.psbl_calls.load(Ordering::SeqCst),
            0,
            "유량 제한 뒤 매수가능수량을 조회하면 안 된다"
        );
    }

    /// REST 스냅샷 폴백으로 받은 시세도 프론트로 emit돼야 화면 수익률이 갱신된다
    #[tokio::test]
    async fn snapshot_fallback_emits_quote_event() {
        let settings = Settings::default();
        let broker = Arc::new(MockBroker::new(&settings));
        let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&events);
        let emit: EmitFn = Box::new(move |event, _payload| {
            sink.lock().unwrap().push(event.to_string());
        });
        let engine = test_engine_with_emit(broker, settings, Some(emit));

        let q = engine.fresh_quote("0193T0").await;

        assert!(q.is_some());
        assert!(
            events.lock().unwrap().iter().any(|e| e == "quote"),
            "스냅샷 폴백 시 quote 이벤트가 emit돼야 한다"
        );
    }

    /// 예약 매도: 평단 기준 목표가(+0.3% 이상 첫 호가)에 걸리고 목록에 반영된다
    #[tokio::test]
    async fn reserved_sell_arms_at_target_above_avg() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine.refresh_account().await;

        let buy = engine.buy_max("0193T0").await;
        assert!(buy.ok, "{}", buy.message);

        let resv = engine.place_reserved_sell("0193T0", 0.3).await;
        assert!(resv.ok, "{}", resv.message);
        let avg = engine.cached_position("0193T0").1;
        assert!(avg > 0.0);
        // 목표가는 평단 +0.3% 이상이어야 한다
        assert!(
            resv.price as f64 >= avg * 1.003,
            "목표가 {} < 평단*1.003 {}",
            resv.price,
            avg * 1.003
        );
        assert_eq!(engine.get_reservations().len(), 1);
    }

    /// 매도 체결 통보가 오면 걸려 있던 예약이 자동 해제된다
    #[tokio::test]
    async fn reserved_sell_clears_on_fill() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine.refresh_account().await;
        let buy = engine.buy_max("0193T0").await;
        assert!(buy.ok, "{}", buy.message);
        assert!(engine.place_reserved_sell("0193T0", 0.5).await.ok);

        // 전량 체결 → filled 상태로 예약 제거
        let info = engine
            .on_sell_fill("0193T0", buy.qty)
            .expect("예약 상태 변화");
        assert_eq!(info.status, "filled");
        assert!(engine.get_reservations().is_empty());
    }

    /// 부분 체결이면 남은 수량으로 대기 상태를 유지한다
    #[tokio::test]
    async fn reserved_sell_partial_fill_keeps_waiting() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine.refresh_account().await;
        let buy = engine.buy_max("0193T0").await;
        assert!(buy.ok && buy.qty >= 2, "{}", buy.message);
        assert!(engine.place_reserved_sell("0193T0", 0.5).await.ok);

        let info = engine.on_sell_fill("0193T0", 1).expect("예약 상태 변화");
        assert_eq!(info.status, "waiting");
        assert_eq!(info.qty, buy.qty - 1);
        assert_eq!(engine.get_reservations().len(), 1);
    }

    /// 수동 전량 매도는 걸려 있던 예약을 먼저 취소한 뒤 시장가로 판다
    #[tokio::test]
    async fn manual_sell_cancels_reservation_first() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine.refresh_account().await;
        let buy = engine.buy_max("0193T0").await;
        assert!(buy.ok, "{}", buy.message);
        assert!(engine.place_reserved_sell("0193T0", 0.5).await.ok);
        assert_eq!(engine.get_reservations().len(), 1);

        let sell = engine.sell_all("0193T0").await;
        assert!(sell.ok, "매도 실패: {}", sell.message);
        assert!(
            engine.get_reservations().is_empty(),
            "매도 시 예약이 취소돼야 한다"
        );
    }

    /// 추가 매수는 평단이 바뀌므로 기존 예약을 취소한다
    #[tokio::test]
    async fn additional_buy_cancels_reservation() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine.refresh_account().await;
        assert!(engine.buy_max("0193T0").await.ok);
        assert!(engine.place_reserved_sell("0193T0", 0.5).await.ok);
        assert_eq!(engine.get_reservations().len(), 1);

        assert!(engine.buy_max("0193T0").await.ok);
        assert!(
            engine.get_reservations().is_empty(),
            "추가 매수 시 예약이 취소돼야 한다"
        );
    }

    /// 보유가 없으면 예약 매도는 거부된다
    #[tokio::test]
    async fn reserved_sell_without_holding_fails() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine.refresh_account().await;
        let resv = engine.place_reserved_sell("0193T0", 0.3).await;
        assert!(!resv.ok);
        assert!(resv.message.contains("보유 수량 없음"), "{}", resv.message);
    }

    /// 지정 체결가로 캐시 시세를 갈아끼운다 (손절 판정 경로 테스트용)
    fn set_quote_price(engine: &Engine, code: &str, price: f64) {
        engine.quotes.write().unwrap().insert(
            code.into(),
            Quote {
                code: code.into(),
                price,
                change_rate: 0.0,
                ask1: price,
                bid1: price,
                ask1_qty: 100_000,
                bid1_qty: 100_000,
                volume: 0.0,
                trade_sequence: 1,
                received_at_micros: crate::util::monotonic_now()
                    .as_micros()
                    .try_into()
                    .unwrap_or(u64::MAX),
                trade_ts: now_kst_fake_epoch(),
                book_ts: now_kst_fake_epoch(),
            },
        );
    }

    /// 손절선(-0.6%) 이하로 떨어지면 한 번만 발동해 전량 매도한다
    #[tokio::test]
    async fn stop_loss_arms_once_and_sells_entire_position() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine.refresh_account().await;
        let buy = engine.buy_max("0193T0").await;
        assert!(buy.ok, "{}", buy.message);
        engine.refresh_account().await; // 보유 캐시 적재
        let (qty, avg) = engine.cached_position("0193T0");
        assert!(qty > 0 && avg > 0.0);

        // 평단 대비 -1% 체결가 → 손절선(-0.6%) 이하
        let low = avg * 0.99;
        set_quote_price(&engine, "0193T0", low);

        // 첫 틱만 발동, 연속 틱은 중복 매도 금지
        assert!(engine.arm_stop_loss_if_breached("0193T0", low));
        assert!(
            !engine.arm_stop_loss_if_breached("0193T0", low),
            "이미 발동했으면 재발동 금지"
        );

        engine.force_stop_loss("0193T0").await;
        engine.refresh_account().await;
        assert_eq!(
            engine.cached_position("0193T0").0,
            0,
            "손절로 전량 매도돼야 한다"
        );
        // 포지션이 사라졌으니 손절 잠금도 풀려야 한다 (재무장)
        assert!(!engine.stop_loss_lock.lock().unwrap().contains_key("0193T0"));
    }

    /// 손절선 위(-0.6% 초과)에서는 발동하지 않는다
    #[tokio::test]
    async fn stop_loss_does_not_arm_above_threshold() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine.refresh_account().await;
        assert!(engine.buy_max("0193T0").await.ok);
        engine.refresh_account().await;
        let (_, avg) = engine.cached_position("0193T0");
        assert!(
            !engine.arm_stop_loss_if_breached("0193T0", avg),
            "0%에서는 발동 금지"
        );
        assert!(
            !engine.arm_stop_loss_if_breached("0193T0", avg * 0.995),
            "-0.5%는 발동 금지"
        );
    }

    /// 보유가 없으면 손절은 무시된다 (평단 부재)
    #[tokio::test]
    async fn stop_loss_ignored_without_holding() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine.refresh_account().await;
        assert!(!engine.arm_stop_loss_if_breached("0193T0", 1.0));
    }

    /// 재매수하면 평단이 바뀌므로 손절 발동 락이 풀려 다시 감시한다
    #[tokio::test]
    async fn stop_loss_rearms_after_rebuy() {
        let settings = Settings::default();
        let engine = test_engine(Arc::new(MockBroker::new(&settings)), settings);
        engine.refresh_account().await;
        // 직전 손절이 발동해 잠금이 걸린 상태를 흉내
        engine.stop_loss_lock.lock().unwrap().insert(
            "0193T0".into(),
            std::time::Instant::now() + STOP_LOSS_RELOCK,
        );

        assert!(engine.buy_max("0193T0").await.ok);
        assert!(
            !engine.stop_loss_lock.lock().unwrap().contains_key("0193T0"),
            "재매수 시 손절 잠금이 풀려야 한다"
        );
    }
}
