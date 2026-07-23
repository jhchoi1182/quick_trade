//! 자동·수동·섀도 매매의 영속 장부.
//!
//! 주문을 전송하기 전에 [`Ledger::record_order_intent`]를 호출하고, 브로커 응답과
//! 체결은 각각 `record_order_ack`, `record_fill_for_broker_order`로 이어서 기록한다.
//! KIS 주문번호는 일자별로 재사용될 수 있으므로 거래일·조직번호·주문번호를 항상
//! 함께 사용하고, 체결 키에는 체결 ID 또는 체결시각을 포함해야 한다.

use std::{
    collections::HashSet,
    fs,
    path::Path,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::{
    params,
    types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef},
    Connection, OptionalExtension, TransactionBehavior,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

const SCHEMA_VERSION: i64 = 5;
const CONTROL_MODE_KEY: &str = "control_mode";
const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PAGE_SIZE: usize = 200;

#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error("장부 디렉터리를 준비하지 못했습니다: {0}")]
    Io(#[from] std::io::Error),
    #[error("장부 데이터베이스 오류: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("장부 JSON 변환 오류: {0}")]
    Json(#[from] serde_json::Error),
    #[error("지원하지 않는 장부 스키마 버전입니다: {0}")]
    UnsupportedSchema(i64),
    #[error("장부 값이 허용 범위를 벗어났습니다: {0}")]
    OutOfRange(&'static str),
    #[error("장부 레코드를 찾지 못했습니다: {0}")]
    NotFound(String),
    #[error("잘못된 장부 입력입니다: {0}")]
    InvalidInput(String),
    #[error("장부 잠금이 손상되었습니다")]
    LockPoisoned,
}

pub type LedgerResult<T> = Result<T, LedgerError>;

/// KIS 주문번호는 거래일이 바뀌면 재사용될 수 있으므로 세 값을 함께 식별자로 쓴다.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerOrderKey {
    /// KIS 영업일 형식(`YYYYMMDD`).
    pub trading_date: String,
    /// 한국거래소전송주문조직번호. WebSocket이 제공하지 않으면 빈 문자열이다.
    pub org_no: String,
    pub order_no: String,
}

impl BrokerOrderKey {
    pub fn new(
        trading_date: impl Into<String>,
        org_no: impl Into<String>,
        order_no: impl Into<String>,
    ) -> LedgerResult<Self> {
        let key = Self {
            trading_date: trading_date.into(),
            org_no: org_no.into(),
            order_no: order_no.into(),
        };
        key.validate()?;
        Ok(key)
    }

    /// 앱의 KST 가짜 epoch에서 거래일을 뽑아 주문 식별자를 만든다.
    pub fn from_fake_epoch(
        timestamp: i64,
        org_no: impl Into<String>,
        order_no: impl Into<String>,
    ) -> LedgerResult<Self> {
        Self::new(trading_date_from_fake_epoch(timestamp)?, org_no, order_no)
    }

    fn validate(&self) -> LedgerResult<()> {
        if self.order_no.trim().is_empty() {
            return Err(LedgerError::InvalidInput(
                "브로커 주문번호는 비어 있을 수 없습니다".into(),
            ));
        }
        NaiveDate::parse_from_str(&self.trading_date, "%Y%m%d").map_err(|_| {
            LedgerError::InvalidInput(format!(
                "거래일은 YYYYMMDD 형식이어야 합니다: {}",
                self.trading_date
            ))
        })?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("저장된 {kind} 값이 올바르지 않습니다: {value}")]
struct InvalidEnumValue {
    kind: &'static str,
    value: String,
}

macro_rules! ledger_string_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $($variant:ident => $value:literal),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }

            fn from_stored(value: &str) -> Option<Self> {
                match value {
                    $($value => Some(Self::$variant)),+,
                    _ => None,
                }
            }
        }

        impl ToSql for $name {
            fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
                Ok(ToSqlOutput::Borrowed(ValueRef::Text(self.as_str().as_bytes())))
            }
        }

        impl FromSql for $name {
            fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                let stored = value.as_str()?;
                Self::from_stored(stored).ok_or_else(|| {
                    FromSqlError::Other(Box::new(InvalidEnumValue {
                        kind: stringify!($name),
                        value: stored.to_owned(),
                    }))
                })
            }
        }
    };
}

ledger_string_enum! {
    /// 앱의 실전 주문 제어 모드.
    pub enum LedgerControlMode {
        Manual => "manual",
        Auto => "auto",
        Shadow => "shadow",
    }
}

ledger_string_enum! {
    pub enum LedgerExecutionKind {
        Real => "real",
        Shadow => "shadow",
    }
}

ledger_string_enum! {
    pub enum LedgerProductKind {
        Leverage => "leverage",
        Inverse => "inverse",
    }
}

ledger_string_enum! {
    /// LLM이 분류한 상위 시간축 시장 국면.
    pub enum LedgerMarketRegime {
        Uptrend => "uptrend",
        Downtrend => "downtrend",
        Range => "range",
        Transition => "transition",
        Unclear => "unclear",
    }
}

ledger_string_enum! {
    /// 추세 지속 진입과 지지·저항 반전 진입을 구분한다.
    pub enum LedgerSetupType {
        Continuation => "continuation",
        Reversal => "reversal",
    }
}

ledger_string_enum! {
    pub enum LedgerOrigin {
        Manual => "manual",
        Auto => "auto",
        Shadow => "shadow",
        AutoHandoff => "auto_handoff",
        External => "external",
    }
}

ledger_string_enum! {
    pub enum LedgerSide {
        Buy => "buy",
        Sell => "sell",
    }
}

ledger_string_enum! {
    pub enum LedgerSessionStatus {
        Active => "active",
        Closed => "closed",
        Interrupted => "interrupted",
    }
}

ledger_string_enum! {
    pub enum LedgerDecisionStatus {
        Armed => "armed",
        Skipped => "skipped",
        Triggered => "triggered",
        Expired => "expired",
        Replaced => "replaced",
        Missed => "missed",
        Invalidated => "invalidated",
        Invalid => "invalid",
        Error => "error",
        Discarded => "discarded",
    }
}

ledger_string_enum! {
    pub enum LedgerScenarioStatus {
        Armed => "armed",
        Confirming => "confirming",
        Triggered => "triggered",
        Expired => "expired",
        Replaced => "replaced",
        CancelledByOco => "cancelled_by_oco",
        Missed => "missed",
        Invalidated => "invalidated",
        Invalid => "invalid",
    }
}

ledger_string_enum! {
    pub enum LedgerOrderType {
        Market => "market",
        Limit => "limit",
        IocLimit => "ioc_limit",
        Cancel => "cancel",
        Virtual => "virtual",
    }
}

ledger_string_enum! {
    pub enum LedgerOrderStatus {
        Intent => "intent",
        Dispatching => "dispatching",
        Submitted => "submitted",
        PartiallyFilled => "partially_filled",
        Filled => "filled",
        CancelPending => "cancel_pending",
        Cancelled => "cancelled",
        Rejected => "rejected",
        Unknown => "unknown",
    }
}

ledger_string_enum! {
    pub enum LedgerTradeStatus {
        Open => "open",
        Closed => "closed",
        HandedOff => "handed_off",
        Interrupted => "interrupted",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewSession {
    pub session_id: String,
    pub execution_kind: LedgerExecutionKind,
    pub control_mode: LedgerControlMode,
    pub starting_cash: Option<u64>,
    pub started_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewDecision {
    pub decision_id: String,
    pub session_id: Option<String>,
    pub control_mode: LedgerControlMode,
    pub revision: i64,
    pub as_of_ts: i64,
    pub expires_at: i64,
    pub underlying_price: f64,
    pub status: LedgerDecisionStatus,
    pub model: String,
    pub prompt_version: String,
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
    pub latency_ms: u64,
    pub input_hash: Option<String>,
    pub chart_hash: Option<String>,
    pub market_regime: Option<LedgerMarketRegime>,
    pub decision_summary_ko: Option<String>,
    pub error: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewDecisionScenario {
    pub product: LedgerProductKind,
    pub setup_type: Option<LedgerSetupType>,
    pub reference_price: Option<u64>,
    pub confirmation_price: Option<u64>,
    pub invalidation_price: Option<u64>,
    pub trigger_price: u64,
    pub target_return_pct: f64,
    pub rationale_ko: Option<String>,
    pub status: LedgerScenarioStatus,
    pub reference_observed_at: Option<i64>,
    pub terminal_reason: Option<String>,
}

/// 판단 상태와 함께 원자적으로 반영할 시나리오 상태 변경.
#[derive(Debug, Clone, Copy)]
pub struct ScenarioStatusUpdate<'a> {
    pub product: LedgerProductKind,
    pub status: LedgerScenarioStatus,
    pub confirmation_started_at: Option<i64>,
    pub confirmation_tick_count: u32,
    pub updated_at: i64,
    pub terminal_reason: Option<&'a str>,
    pub reference_observed_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioRecord {
    pub id: i64,
    pub decision_id: String,
    pub product: LedgerProductKind,
    pub setup_type: Option<LedgerSetupType>,
    pub reference_price: Option<u64>,
    pub confirmation_price: Option<u64>,
    pub invalidation_price: Option<u64>,
    pub trigger_price: u64,
    pub target_return_pct: f64,
    pub rationale_ko: Option<String>,
    pub status: LedgerScenarioStatus,
    pub reference_observed_at: Option<i64>,
    pub terminal_reason: Option<String>,
    pub confirmation_started_at: Option<i64>,
    pub confirmation_tick_count: u32,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DecisionRecord {
    pub id: i64,
    pub decision_id: String,
    pub session_id: Option<String>,
    pub control_mode: LedgerControlMode,
    pub revision: i64,
    pub as_of_ts: i64,
    pub expires_at: i64,
    pub underlying_price: f64,
    pub status: LedgerDecisionStatus,
    pub model: String,
    pub prompt_version: String,
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
    pub latency_ms: u64,
    pub input_hash: Option<String>,
    pub chart_hash: Option<String>,
    pub market_regime: Option<LedgerMarketRegime>,
    pub decision_summary_ko: Option<String>,
    pub error: Option<String>,
    pub created_at: i64,
    pub scenarios: Vec<ScenarioRecord>,
}

#[derive(Debug, Clone, Default)]
pub struct DecisionQuery {
    pub control_mode: Option<LedgerControlMode>,
    pub status: Option<LedgerDecisionStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewOrderIntent {
    pub intent_id: String,
    pub session_id: Option<String>,
    pub trade_id: Option<String>,
    pub decision_id: Option<String>,
    pub scenario_product: Option<LedgerProductKind>,
    pub execution_kind: LedgerExecutionKind,
    pub origin: LedgerOrigin,
    pub code: String,
    pub side: LedgerSide,
    pub order_type: LedgerOrderType,
    pub qty: u64,
    pub price: Option<u64>,
    pub requested_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderAcknowledgement {
    pub broker_order_id: Option<String>,
    pub broker_org_no: Option<String>,
    pub original_order_id: Option<String>,
    pub status: LedgerOrderStatus,
    pub message: Option<String>,
    pub acknowledged_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderRecord {
    pub id: i64,
    pub intent_id: String,
    pub session_id: Option<String>,
    pub trade_id: Option<String>,
    pub decision_id: Option<String>,
    pub scenario_product: Option<LedgerProductKind>,
    pub broker_order_id: Option<String>,
    pub broker_org_no: Option<String>,
    pub original_order_id: Option<String>,
    pub trading_date: String,
    pub execution_kind: LedgerExecutionKind,
    pub origin: LedgerOrigin,
    pub code: String,
    pub side: LedgerSide,
    pub order_type: LedgerOrderType,
    pub qty: u64,
    pub price: Option<u64>,
    pub status: LedgerOrderStatus,
    pub message: Option<String>,
    pub requested_at: i64,
    pub acknowledged_at: Option<i64>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewFill {
    /// REST와 WebSocket에서 동일하게 재구성할 수 있는 고유 키.
    pub fill_key: String,
    pub intent_id: Option<String>,
    pub broker_order_id: Option<String>,
    pub broker_fill_id: Option<String>,
    pub execution_kind: LedgerExecutionKind,
    pub origin: LedgerOrigin,
    pub code: String,
    pub side: LedgerSide,
    pub qty: u64,
    pub price: f64,
    pub filled_at: i64,
    pub raw_json: Option<String>,
}

/// WebSocket 체결통보 원문을 REST 누적체결과 별도로 보존한다.
///
/// H0STCNI0은 체결 건별 고유 ID나 주문조직번호를 주지 않으므로 동일 초·수량·가격의
/// 통보도 서로 다른 수신 건일 수 있다. `notice_id`는 앱이 수신 건마다 새로 만드는
/// 고유 ID이고, `source_notice_id`는 브로커가 제공하거나 원문에서 재구성한 식별 힌트다.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewFillNotice {
    pub notice_id: String,
    pub source_notice_id: Option<String>,
    pub broker_order_id: String,
    pub broker_org_no: Option<String>,
    pub code: String,
    pub side: LedgerSide,
    pub qty: u64,
    pub price: f64,
    pub notified_at: i64,
    pub received_at: i64,
    pub raw_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewTrade {
    pub trade_id: String,
    pub session_id: Option<String>,
    pub execution_kind: LedgerExecutionKind,
    pub origin: LedgerOrigin,
    pub code: String,
    pub entry_qty: u64,
    pub exit_qty: u64,
    pub entry_avg_price: f64,
    pub exit_avg_price: Option<f64>,
    /// 현재 화면과 동일한 단순 수익률. 수수료·세금은 포함하지 않는다.
    pub pnl_rate: Option<f64>,
    pub entered_at: i64,
    pub exited_at: Option<i64>,
    pub status: LedgerTradeStatus,
    pub exit_reason: Option<String>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeRecord {
    pub id: i64,
    pub trade_id: String,
    pub session_id: Option<String>,
    pub execution_kind: LedgerExecutionKind,
    pub origin: LedgerOrigin,
    pub code: String,
    pub entry_qty: u64,
    pub exit_qty: u64,
    pub entry_avg_price: f64,
    pub exit_avg_price: Option<f64>,
    pub pnl_rate: Option<f64>,
    pub entered_at: i64,
    pub exited_at: Option<i64>,
    pub status: LedgerTradeStatus,
    pub exit_reason: Option<String>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Default)]
pub struct TradeQuery {
    pub execution_kind: Option<LedgerExecutionKind>,
    pub origin: Option<LedgerOrigin>,
    pub status: Option<LedgerTradeStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CursorPage<T> {
    pub items: Vec<T>,
    /// 다음 조회에서는 이 값을 그대로 cursor로 전달한다.
    pub next_cursor: Option<i64>,
}

/// 내부 `Connection`을 mutex로 감싸 Tauri state에서 안전하게 공유한다.
pub struct Ledger {
    conn: Mutex<Connection>,
}

impl Ledger {
    /// `config_dir/trading.db`를 열고 WAL 및 스키마 마이그레이션을 적용한다.
    pub fn open(config_dir: impl AsRef<Path>) -> LedgerResult<Self> {
        let config_dir = config_dir.as_ref();
        fs::create_dir_all(config_dir)?;
        let path = config_dir.join("trading.db");
        let mut conn = Connection::open(&path)?;
        configure_connection(&mut conn)?;
        migrate(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// 테스트 또는 휘발성 검증용 장부를 연다.
    #[cfg(test)]
    pub fn open_in_memory() -> LedgerResult<Self> {
        let mut conn = Connection::open_in_memory()?;
        configure_connection(&mut conn)?;
        migrate(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    #[cfg(test)]
    pub fn schema_version(&self) -> LedgerResult<i64> {
        let conn = self.lock()?;
        Ok(conn.query_row("PRAGMA user_version", [], |row| row.get(0))?)
    }

    pub fn set_runtime_state<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        updated_at: i64,
    ) -> LedgerResult<()> {
        if key.trim().is_empty() {
            return Err(LedgerError::InvalidInput(
                "runtime_state 키는 비어 있을 수 없습니다".into(),
            ));
        }
        let value_json = serde_json::to_string(value)?;
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO runtime_state(key, value_json, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET
               value_json = excluded.value_json,
               updated_at = excluded.updated_at",
            params![key, value_json, updated_at],
        )?;
        Ok(())
    }

    pub fn get_runtime_state<T: DeserializeOwned>(&self, key: &str) -> LedgerResult<Option<T>> {
        let conn = self.lock()?;
        let value: Option<String> = conn
            .query_row(
                "SELECT value_json FROM runtime_state WHERE key = ?1",
                [key],
                |row| row.get(0),
            )
            .optional()?;
        value
            .map(|json| serde_json::from_str(&json).map_err(LedgerError::from))
            .transpose()
    }

    pub fn delete_runtime_state(&self, key: &str) -> LedgerResult<()> {
        if key.trim().is_empty() {
            return Err(LedgerError::InvalidInput(
                "runtime_state 키는 비어 있을 수 없습니다".into(),
            ));
        }
        let conn = self.lock()?;
        conn.execute("DELETE FROM runtime_state WHERE key = ?1", [key])?;
        Ok(())
    }

    /// 거래·주문 기록은 보존하면서 복구 불가능한 실행 상태만 수동 모드로 초기화한다.
    ///
    /// 활성 자동/섀도 세션과 그 세션의 열린 거래는 중단으로 표시해 기록 화면에서
    /// 정상 종료처럼 보이지 않게 한다. 런타임 캐시 삭제와 수동 모드 저장은 한
    /// 트랜잭션으로 묶어 앱이 중간에 종료돼도 Auto 상태가 되살아나지 않는다.
    pub fn reset_runtime_state_to_manual(&self, reset_at: i64) -> LedgerResult<()> {
        let mode_json = serde_json::to_string(&LedgerControlMode::Manual)?;
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "UPDATE trades
                SET status = ?1,
                    exit_reason = COALESCE(exit_reason, 'runtime_reset'),
                    updated_at = ?2
              WHERE status = ?3
                AND session_id IN (
                    SELECT session_id FROM sessions WHERE status = ?4
                )",
            params![
                LedgerTradeStatus::Interrupted,
                reset_at,
                LedgerTradeStatus::Open,
                LedgerSessionStatus::Active,
            ],
        )?;
        tx.execute(
            "UPDATE sessions
                SET status = ?1, ended_at = ?2
              WHERE status = ?3",
            params![
                LedgerSessionStatus::Interrupted,
                reset_at,
                LedgerSessionStatus::Active,
            ],
        )?;
        tx.execute("DELETE FROM runtime_state", [])?;
        tx.execute(
            "INSERT INTO runtime_state(key, value_json, updated_at)
             VALUES (?1, ?2, ?3)",
            params![CONTROL_MODE_KEY, mode_json, reset_at],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn set_control_mode(&self, mode: LedgerControlMode, updated_at: i64) -> LedgerResult<()> {
        self.set_runtime_state(CONTROL_MODE_KEY, &mode, updated_at)
    }

    pub fn control_mode(&self) -> LedgerResult<Option<LedgerControlMode>> {
        self.get_runtime_state(CONTROL_MODE_KEY)
    }

    pub fn get_control_mode(&self) -> LedgerResult<Option<LedgerControlMode>> {
        self.control_mode()
    }

    pub fn start_session(&self, session: &NewSession) -> LedgerResult<()> {
        validate_id("session_id", &session.session_id)?;
        let starting_cash = optional_u64_to_i64(session.starting_cash, "starting_cash")?;
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO sessions(
                session_id, execution_kind, control_mode, starting_cash,
                status, started_at, ended_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)",
            params![
                session.session_id,
                session.execution_kind,
                session.control_mode,
                starting_cash,
                LedgerSessionStatus::Active,
                session.started_at,
            ],
        )?;
        Ok(())
    }

    pub fn end_session(
        &self,
        session_id: &str,
        status: LedgerSessionStatus,
        ended_at: i64,
    ) -> LedgerResult<()> {
        if status == LedgerSessionStatus::Active {
            return Err(LedgerError::InvalidInput(
                "세션 종료 상태로 active를 사용할 수 없습니다".into(),
            ));
        }
        let conn = self.lock()?;
        let changed = conn.execute(
            "UPDATE sessions SET status = ?2, ended_at = ?3 WHERE session_id = ?1",
            params![session_id, status, ended_at],
        )?;
        ensure_changed(changed, "session", session_id)
    }

    /// 결정과 최대 두 개 시나리오를 하나의 트랜잭션으로 기록한다.
    pub fn record_decision(
        &self,
        decision: &NewDecision,
        scenarios: &[NewDecisionScenario],
    ) -> LedgerResult<i64> {
        validate_id("decision_id", &decision.decision_id)?;
        validate_scenarios(scenarios)?;

        let input_tokens = u64_to_i64(decision.input_tokens, "input_tokens")?;
        let cached_input_tokens = u64_to_i64(decision.cached_input_tokens, "cached_input_tokens")?;
        let cache_write_tokens = u64_to_i64(decision.cache_write_tokens, "cache_write_tokens")?;
        let output_tokens = u64_to_i64(decision.output_tokens, "output_tokens")?;
        let reasoning_tokens = u64_to_i64(decision.reasoning_tokens, "reasoning_tokens")?;
        let latency_ms = u64_to_i64(decision.latency_ms, "latency_ms")?;

        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO llm_decisions(
                decision_id, session_id, control_mode, revision, as_of_ts, expires_at,
                underlying_price, status, model, prompt_version, input_tokens,
                cached_input_tokens, cache_write_tokens, output_tokens, reasoning_tokens, latency_ms,
                input_hash, chart_hash, market_regime, decision_summary_ko, error, created_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22
             )",
            params![
                decision.decision_id,
                decision.session_id,
                decision.control_mode,
                decision.revision,
                decision.as_of_ts,
                decision.expires_at,
                decision.underlying_price,
                decision.status,
                decision.model,
                decision.prompt_version,
                input_tokens,
                cached_input_tokens,
                cache_write_tokens,
                output_tokens,
                reasoning_tokens,
                latency_ms,
                decision.input_hash,
                decision.chart_hash,
                decision.market_regime,
                decision.decision_summary_ko,
                decision.error,
                decision.created_at,
            ],
        )?;
        let row_id = tx.last_insert_rowid();

        for scenario in scenarios {
            tx.execute(
                "INSERT INTO decision_scenarios(
                    decision_id, product, setup_type, reference_price, confirmation_price,
                    invalidation_price, trigger_price, target_return_pct, rationale_ko, status,
                    reference_observed_at, terminal_reason, confirmation_started_at,
                    confirmation_tick_count, updated_at
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                    ?11, ?12, NULL, 0, ?13
                 )",
                params![
                    decision.decision_id,
                    scenario.product,
                    scenario.setup_type,
                    optional_u64_to_i64(scenario.reference_price, "reference_price")?,
                    optional_u64_to_i64(scenario.confirmation_price, "confirmation_price")?,
                    optional_u64_to_i64(scenario.invalidation_price, "invalidation_price")?,
                    u64_to_i64(scenario.trigger_price, "trigger_price")?,
                    scenario.target_return_pct,
                    scenario.rationale_ko,
                    scenario.status,
                    scenario.reference_observed_at,
                    scenario.terminal_reason,
                    decision.created_at,
                ],
            )?;
        }
        tx.commit()?;
        Ok(row_id)
    }

    /// 여러 시나리오와 부모 판단 상태를 한 트랜잭션으로 갱신한다.
    ///
    /// OCO 상태 전이·판단 폐기처럼 부모/자식이 반드시 함께 움직여야 하는 경로에서
    /// 부분 반영과 성공 전 이력 이벤트 발행을 막는다.
    pub fn update_decision_and_scenarios(
        &self,
        decision_id: &str,
        decision_status: Option<LedgerDecisionStatus>,
        decision_error: Option<&str>,
        scenarios: &[ScenarioStatusUpdate<'_>],
    ) -> LedgerResult<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for scenario in scenarios {
            let changed = tx.execute(
                "UPDATE decision_scenarios
                 SET status = ?3,
                     confirmation_started_at = ?4,
                     confirmation_tick_count = ?5,
                     updated_at = ?6,
                     terminal_reason = COALESCE(?7, terminal_reason),
                     reference_observed_at = ?8
                 WHERE decision_id = ?1 AND product = ?2",
                params![
                    decision_id,
                    scenario.product,
                    scenario.status,
                    scenario.confirmation_started_at,
                    i64::from(scenario.confirmation_tick_count),
                    scenario.updated_at,
                    scenario.terminal_reason,
                    scenario.reference_observed_at,
                ],
            )?;
            ensure_changed(
                changed,
                "scenario",
                &format!("{decision_id}/{}", scenario.product.as_str()),
            )?;
        }
        if let Some(status) = decision_status {
            let changed = tx.execute(
                "UPDATE llm_decisions
                 SET status = ?2, error = COALESCE(?3, error)
                 WHERE decision_id = ?1",
                params![decision_id, status, decision_error],
            )?;
            ensure_changed(changed, "decision", decision_id)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// 프로세스 재시작으로 복원할 수 없는 메모리 OCO의 활성 장부를 종결한다.
    ///
    /// 슬롯이 이미 끝났으면 부모·자식을 `expired`로, 아직 슬롯 안이면 실행되지
    /// 않은 응답을 부모 `discarded`/자식 `replaced`로 한 트랜잭션에서 바꾼다.
    pub fn close_unrestorable_oco_decisions(&self, restarted_at: i64) -> LedgerResult<usize> {
        let mut conn = self.lock()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "UPDATE decision_scenarios
             SET status = CASE
                    WHEN (
                        SELECT expires_at
                        FROM llm_decisions
                        WHERE llm_decisions.decision_id = decision_scenarios.decision_id
                    ) <= ?1 THEN ?2
                    ELSE ?3
                 END,
                 confirmation_started_at = NULL,
                 confirmation_tick_count = 0,
                 updated_at = ?1,
                 terminal_reason = CASE
                    WHEN (
                        SELECT expires_at
                        FROM llm_decisions
                        WHERE llm_decisions.decision_id = decision_scenarios.decision_id
                    ) <= ?1 THEN '앱 재시작 중 판단 슬롯 만료'
                    ELSE '앱 재시작으로 메모리 OCO를 복원하지 않음'
                 END
             WHERE status IN (?4, ?5)
               AND decision_id IN (
                    SELECT decision_id
                    FROM llm_decisions
                    WHERE status = ?6
               )",
            params![
                restarted_at,
                LedgerScenarioStatus::Expired,
                LedgerScenarioStatus::Replaced,
                LedgerScenarioStatus::Armed,
                LedgerScenarioStatus::Confirming,
                LedgerDecisionStatus::Armed,
            ],
        )?;
        let changed = tx.execute(
            "UPDATE llm_decisions
             SET status = CASE
                    WHEN expires_at <= ?1 THEN ?2
                    ELSE ?3
                 END,
                 error = COALESCE(
                    error,
                    CASE
                        WHEN expires_at <= ?1 THEN '앱 재시작 중 판단 슬롯 만료'
                        ELSE '앱 재시작으로 메모리 OCO를 복원하지 않음'
                    END
                 )
             WHERE status = ?4",
            params![
                restarted_at,
                LedgerDecisionStatus::Expired,
                LedgerDecisionStatus::Discarded,
                LedgerDecisionStatus::Armed,
            ],
        )?;
        tx.commit()?;
        Ok(changed)
    }

    pub fn record_order_intent(&self, intent: &NewOrderIntent) -> LedgerResult<i64> {
        validate_id("intent_id", &intent.intent_id)?;
        let qty = u64_to_i64(intent.qty, "qty")?;
        if qty == 0 {
            return Err(LedgerError::InvalidInput(
                "주문 수량은 1 이상이어야 합니다".into(),
            ));
        }
        let price = optional_u64_to_i64(intent.price, "price")?;
        let trading_date = trading_date_from_fake_epoch(intent.requested_at)?;
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO orders(
                intent_id, session_id, trade_id, decision_id, scenario_product,
                broker_order_id, broker_org_no, original_order_id, trading_date,
                execution_kind, origin, code, side, order_type, qty, price,
                status, message, requested_at, acknowledged_at, updated_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, NULL, NULL, NULL, ?6,
                ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, NULL, ?15, NULL, ?15
             )",
            params![
                intent.intent_id,
                intent.session_id,
                intent.trade_id,
                intent.decision_id,
                intent.scenario_product,
                trading_date,
                intent.execution_kind,
                intent.origin,
                intent.code,
                intent.side,
                intent.order_type,
                qty,
                price,
                LedgerOrderStatus::Intent,
                intent.requested_at,
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn record_order_ack(
        &self,
        intent_id: &str,
        ack: &OrderAcknowledgement,
    ) -> LedgerResult<()> {
        let conn = self.lock()?;
        let changed = conn.execute(
            "UPDATE orders SET
                broker_order_id = COALESCE(?2, broker_order_id),
                broker_org_no = COALESCE(?3, broker_org_no),
                original_order_id = COALESCE(?4, original_order_id),
                status = ?5,
                message = COALESCE(?6, message),
                acknowledged_at = ?7,
                updated_at = ?7
             WHERE intent_id = ?1",
            params![
                intent_id,
                ack.broker_order_id,
                ack.broker_org_no,
                ack.original_order_id,
                ack.status,
                ack.message,
                ack.acknowledged_at,
            ],
        )?;
        ensure_changed(changed, "order", intent_id)
    }

    /// 주문 POST 직전 `intent → dispatching`을 원자적으로 한 번만 전환한다.
    /// false면 다른 실행 경로가 이미 제출을 시작했으므로 호출자는 POST하면 안 된다.
    pub fn begin_order_dispatch(&self, intent_id: &str, at: i64) -> LedgerResult<bool> {
        validate_id("intent_id", intent_id)?;
        let conn = self.lock()?;
        let changed = conn.execute(
            "UPDATE orders
             SET status = ?2, updated_at = ?3
             WHERE intent_id = ?1 AND status = ?4",
            params![
                intent_id,
                LedgerOrderStatus::Dispatching,
                at,
                LedgerOrderStatus::Intent,
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn update_order_status(
        &self,
        intent_id: &str,
        status: LedgerOrderStatus,
        message: Option<&str>,
        updated_at: i64,
    ) -> LedgerResult<()> {
        let conn = self.lock()?;
        let changed = conn.execute(
            "UPDATE orders SET
                status = ?2,
                message = COALESCE(?3, message),
                updated_at = ?4
             WHERE intent_id = ?1",
            params![intent_id, status, message, updated_at],
        )?;
        ensure_changed(changed, "order", intent_id)
    }

    pub fn get_order(&self, intent_id: &str) -> LedgerResult<Option<OrderRecord>> {
        let conn = self.lock()?;
        conn.query_row(
            "SELECT
                id, intent_id, session_id, trade_id, decision_id, scenario_product,
                broker_order_id, broker_org_no, original_order_id, trading_date, execution_kind,
                origin, code, side, order_type, qty, price, status, message,
                requested_at, acknowledged_at, updated_at
             FROM orders WHERE intent_id = ?1",
            [intent_id],
            map_order,
        )
        .optional()
        .map_err(LedgerError::from)
    }

    /// 거래일·조직번호·주문번호가 모두 같은 Auto 주문을 반환한다.
    ///
    /// 주문번호는 거래일마다 재사용될 수 있고 WebSocket에는 조직번호가 없을 수
    /// 있으므로, 취소 종결을 확정하는 경로에서는 세 식별자를 모두 알고 있을 때만
    /// 이 메서드를 사용한다.
    pub fn get_auto_order_by_broker_key(
        &self,
        key: &BrokerOrderKey,
    ) -> LedgerResult<Option<OrderRecord>> {
        key.validate()?;
        let conn = self.lock()?;
        conn.query_row(
            "SELECT
                id, intent_id, session_id, trade_id, decision_id, scenario_product,
                broker_order_id, broker_org_no, original_order_id, trading_date, execution_kind,
                origin, code, side, order_type, qty, price, status, message,
                requested_at, acknowledged_at, updated_at
             FROM orders
             WHERE trading_date = ?1
               AND COALESCE(broker_org_no, '') = ?2
               AND broker_order_id = ?3
               AND origin = 'auto'",
            params![key.trading_date, key.org_no, key.order_no],
            map_order,
        )
        .optional()
        .map_err(LedgerError::from)
    }

    /// 지정 거래일의 실제 주문 중 아직 경제 종결을 장부에서 확인하지 못한 목록.
    ///
    /// 과거 영업일의 주문은 더 이상 체결될 수 없으므로 현재 거래일만 조회한다.
    /// 수동 주문은 정상 체결 뒤에도 `submitted`로 남을 수 있어, 호출자는 정확한
    /// 일별 주문상태를 대조해 종결한 뒤 브로커 재시작 여부를 판단해야 한다.
    pub fn list_unresolved_real_orders(
        &self,
        trading_date: &str,
    ) -> LedgerResult<Vec<OrderRecord>> {
        NaiveDate::parse_from_str(trading_date, "%Y%m%d").map_err(|_| {
            LedgerError::InvalidInput(format!(
                "거래일은 YYYYMMDD 형식이어야 합니다: {trading_date}"
            ))
        })?;
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT
                id, intent_id, session_id, trade_id, decision_id, scenario_product,
                broker_order_id, broker_org_no, original_order_id, trading_date, execution_kind,
                origin, code, side, order_type, qty, price, status, message,
                requested_at, acknowledged_at, updated_at
             FROM orders
             WHERE trading_date = ?1
               AND execution_kind = ?2
               AND status IN (?3, ?4, ?5, ?6, ?7)
             ORDER BY requested_at, id",
        )?;
        let rows = stmt.query_map(
            params![
                trading_date,
                LedgerExecutionKind::Real,
                LedgerOrderStatus::Dispatching,
                LedgerOrderStatus::Submitted,
                LedgerOrderStatus::PartiallyFilled,
                LedgerOrderStatus::CancelPending,
                LedgerOrderStatus::Unknown,
            ],
            map_order,
        )?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(LedgerError::from)
    }

    /// 거래일·조직번호·주문번호가 모두 같은 Auto 주문인지 확인한다.
    pub fn is_auto_broker_order_key(&self, key: &BrokerOrderKey) -> LedgerResult<bool> {
        key.validate()?;
        let conn = self.lock()?;
        let exists: i64 = conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM orders
                WHERE trading_date = ?1
                  AND COALESCE(broker_org_no, '') = ?2
                  AND broker_order_id = ?3
                  AND origin = 'auto'
             )",
            params![key.trading_date, key.org_no, key.order_no],
            |row| row.get(0),
        )?;
        Ok(exists != 0)
    }

    /// 새 체결이면 true, 같은 주문 범위에서 이미 기록된 체결이면 false를 반환한다.
    /// 체결 ID가 없는 알림은 `fill_key`에 체결시각·수량·가격을 포함해야 한다.
    pub fn record_fill_for_broker_order(
        &self,
        key: &BrokerOrderKey,
        fill: &NewFill,
    ) -> LedgerResult<bool> {
        key.validate()?;
        validate_id("fill_key", &fill.fill_key)?;
        let filled_date = trading_date_from_fake_epoch(fill.filled_at)?;
        if filled_date != key.trading_date {
            return Err(LedgerError::InvalidInput(format!(
                "체결 거래일({filled_date})과 장부 주문 거래일({})이 다릅니다",
                key.trading_date
            )));
        }
        if let Some(order_no) = fill.broker_order_id.as_deref() {
            if order_no != key.order_no {
                return Err(LedgerError::InvalidInput(format!(
                    "체결 주문번호({order_no})와 장부 주문 식별자({})가 다릅니다",
                    key.order_no
                )));
            }
        }
        let qty = u64_to_i64(fill.qty, "fill.qty")?;
        if qty == 0 {
            return Err(LedgerError::InvalidInput(
                "체결 수량은 1 이상이어야 합니다".into(),
            ));
        }
        let conn = self.lock()?;
        // 실시간 체결통보에는 조직번호가 없을 수 있다. 같은 거래일의 ACK에서 복원해
        // REST 조정 체결과 동일한 주문 범위에 기록한다.
        let org_no = if key.org_no.is_empty() {
            infer_fill_org_no(
                &conn,
                &key.trading_date,
                &key.order_no,
                fill.intent_id.as_deref(),
            )?
        } else {
            key.org_no.clone()
        };
        let changed = conn.execute(
            "INSERT INTO fills(
                fill_key, trading_date, intent_id, broker_order_id, broker_org_no, broker_fill_id,
                execution_kind, origin, code, side, qty, price, filled_at, raw_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
             ON CONFLICT DO NOTHING",
            params![
                fill.fill_key,
                key.trading_date,
                fill.intent_id,
                key.order_no,
                org_no,
                fill.broker_fill_id,
                fill.execution_kind,
                fill.origin,
                fill.code,
                fill.side,
                qty,
                fill.price,
                fill.filled_at,
                fill.raw_json,
            ],
        )?;
        Ok(changed == 1)
    }

    /// WebSocket 체결통보 한 건을 감사 장부에 기록한다.
    ///
    /// 경제 체결의 중복 방지는 `fills`의 정확한 주문 복합키와 REST 누적 delta가
    /// 담당한다. 이 테이블은 동일한 원문이 연속으로 와도 수신 건을 합치지 않는다.
    pub fn record_fill_notice(&self, notice: &NewFillNotice) -> LedgerResult<()> {
        validate_id("notice_id", &notice.notice_id)?;
        validate_id("broker_order_id", &notice.broker_order_id)?;
        validate_id("code", &notice.code)?;
        let trading_date = trading_date_from_fake_epoch(notice.notified_at)?;
        let qty = u64_to_i64(notice.qty, "fill_notice.qty")?;
        if qty == 0 {
            return Err(LedgerError::InvalidInput(
                "체결통보 수량은 1 이상이어야 합니다".into(),
            ));
        }
        if !notice.price.is_finite() || notice.price <= 0.0 {
            return Err(LedgerError::InvalidInput(
                "체결통보 가격은 유효한 양수여야 합니다".into(),
            ));
        }
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO fill_notices(
                notice_id, source_notice_id, trading_date, broker_order_id, broker_org_no,
                code, side, qty, price, notified_at, received_at, raw_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                notice.notice_id,
                notice.source_notice_id,
                trading_date,
                notice.broker_order_id,
                notice.broker_org_no.as_deref().unwrap_or_default(),
                notice.code,
                notice.side,
                qty,
                notice.price,
                notice.notified_at,
                notice.received_at,
                notice.raw_json,
            ],
        )?;
        Ok(())
    }

    /// 한 거래일의 주문번호가 이미 결합된 조직번호 후보를 돌려준다.
    /// 주문 ACK와 정확 REST 체결 모두를 보되 빈 조직번호는 후보에서 제외한다.
    pub fn broker_order_org_candidates(
        &self,
        trading_date: &str,
        broker_order_id: &str,
        code: &str,
        side: LedgerSide,
    ) -> LedgerResult<Vec<String>> {
        BrokerOrderKey::new(trading_date, "", broker_order_id)?.validate()?;
        validate_id("code", code)?;
        let conn = self.lock()?;
        let mut statement = conn.prepare(
            "SELECT broker_org_no FROM orders
              WHERE trading_date = ?1 AND broker_order_id = ?2 AND code = ?3 AND side = ?4
                AND broker_org_no IS NOT NULL AND TRIM(broker_org_no) <> ''
             UNION
             SELECT broker_org_no FROM fills
              WHERE trading_date = ?1 AND broker_order_id = ?2 AND code = ?3 AND side = ?4
                AND TRIM(broker_org_no) <> ''
             ORDER BY broker_org_no",
        )?;
        let rows = statement
            .query_map(params![trading_date, broker_order_id, code, side], |row| {
                row.get::<_, String>(0)
            })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// 호환 호출부에서도 거래일은 반드시 범위에 넣고, 조직번호는 같은 날의 주문 ACK에서
    /// 복원한다. 신규 호출부는 [`Ledger::record_fill_for_broker_order`]를 사용한다.
    pub fn record_fill(&self, fill: &NewFill) -> LedgerResult<bool> {
        validate_id("fill_key", &fill.fill_key)?;
        let trading_date = trading_date_from_fake_epoch(fill.filled_at)?;
        let order_no = fill
            .broker_order_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&fill.fill_key)
            .to_owned();
        let org_no = {
            let conn = self.lock()?;
            infer_fill_org_no(&conn, &trading_date, &order_no, fill.intent_id.as_deref())?
        };
        let key = BrokerOrderKey::new(trading_date, org_no, order_no)?;
        self.record_fill_for_broker_order(&key, fill)
    }

    /// REST 주문별 누적체결을 delta로 바꿀 때 이미 반영한 수량과 체결대금을 센다.
    /// KIS가 주는 누적 평균가에서 새 구간의 정확한 평균가를 역산하는 데 사용한다.
    pub fn fill_totals_for_broker_order(&self, key: &BrokerOrderKey) -> LedgerResult<(u64, f64)> {
        key.validate()?;
        let conn = self.lock()?;
        let (qty, value): (i64, f64) = conn.query_row(
            "SELECT COALESCE(SUM(qty), 0), COALESCE(SUM(qty * price), 0.0) FROM fills
             WHERE trading_date = ?1 AND broker_org_no = ?2 AND broker_order_id = ?3",
            params![key.trading_date, key.org_no, key.order_no],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok((i64_to_u64(qty, 0)?, value))
    }

    /// 이미 반영한 수량만 필요한 이전 호출부를 위한 편의 API.
    pub fn filled_qty_for_broker_order(&self, key: &BrokerOrderKey) -> LedgerResult<u64> {
        self.fill_totals_for_broker_order(key).map(|(qty, _)| qty)
    }

    /// 동일 trade_id가 있으면 최신 집계 값으로 갱신한다.
    pub fn upsert_trade(&self, trade: &NewTrade) -> LedgerResult<i64> {
        validate_id("trade_id", &trade.trade_id)?;
        let entry_qty = u64_to_i64(trade.entry_qty, "entry_qty")?;
        let exit_qty = u64_to_i64(trade.exit_qty, "exit_qty")?;
        if exit_qty > entry_qty {
            return Err(LedgerError::InvalidInput(
                "청산 수량은 진입 수량보다 클 수 없습니다".into(),
            ));
        }
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO trades(
                trade_id, session_id, execution_kind, origin, code,
                entry_qty, exit_qty, entry_avg_price, exit_avg_price, pnl_rate,
                entered_at, exited_at, status, exit_reason, updated_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                ?11, ?12, ?13, ?14, ?15
             )
             ON CONFLICT(trade_id) DO UPDATE SET
                session_id = excluded.session_id,
                execution_kind = excluded.execution_kind,
                origin = excluded.origin,
                code = excluded.code,
                entry_qty = excluded.entry_qty,
                exit_qty = excluded.exit_qty,
                entry_avg_price = excluded.entry_avg_price,
                exit_avg_price = excluded.exit_avg_price,
                pnl_rate = excluded.pnl_rate,
                entered_at = excluded.entered_at,
                exited_at = excluded.exited_at,
                status = excluded.status,
                exit_reason = excluded.exit_reason,
                updated_at = excluded.updated_at",
            params![
                trade.trade_id,
                trade.session_id,
                trade.execution_kind,
                trade.origin,
                trade.code,
                entry_qty,
                exit_qty,
                trade.entry_avg_price,
                trade.exit_avg_price,
                trade.pnl_rate,
                trade.entered_at,
                trade.exited_at,
                trade.status,
                trade.exit_reason,
                trade.updated_at,
            ],
        )?;
        Ok(conn.query_row(
            "SELECT id FROM trades WHERE trade_id = ?1",
            [&trade.trade_id],
            |row| row.get(0),
        )?)
    }

    /// 최신순 cursor pagination. cursor가 있으면 `id < cursor`인 항목을 조회한다.
    pub fn list_trades(
        &self,
        query: &TradeQuery,
        cursor: Option<i64>,
        limit: usize,
    ) -> LedgerResult<CursorPage<TradeRecord>> {
        let limit = normalize_limit(limit);
        let fetch_limit = i64::try_from(limit + 1).expect("페이지 제한은 i64 범위 안");
        let cursor = cursor.unwrap_or(i64::MAX);
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT
                id, trade_id, session_id, execution_kind, origin, code,
                entry_qty, exit_qty, entry_avg_price, exit_avg_price, pnl_rate,
                entered_at, exited_at, status, exit_reason, updated_at
             FROM trades
             WHERE id < ?1
               AND (?2 IS NULL OR execution_kind = ?2)
               AND (?3 IS NULL OR origin = ?3)
               AND (?4 IS NULL OR status = ?4)
             ORDER BY id DESC
             LIMIT ?5",
        )?;
        let execution_kind = query.execution_kind.map(|v| v.as_str());
        let origin = query.origin.map(|v| v.as_str());
        let status = query.status.map(|v| v.as_str());
        let rows = stmt.query_map(
            params![cursor, execution_kind, origin, status, fetch_limit],
            map_trade,
        )?;
        let mut items = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        let has_more = items.len() > limit;
        if has_more {
            items.truncate(limit);
        }
        let next_cursor = has_more.then(|| items.last().expect("limit은 1 이상").id);
        Ok(CursorPage { items, next_cursor })
    }

    /// 최신순 결정 목록과 각 결정의 시나리오를 함께 반환한다.
    pub fn list_decisions(
        &self,
        query: &DecisionQuery,
        cursor: Option<i64>,
        limit: usize,
    ) -> LedgerResult<CursorPage<DecisionRecord>> {
        let limit = normalize_limit(limit);
        let fetch_limit = i64::try_from(limit + 1).expect("페이지 제한은 i64 범위 안");
        let cursor = cursor.unwrap_or(i64::MAX);
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT
                id, decision_id, session_id, control_mode, revision, as_of_ts,
                expires_at, underlying_price, status, model, prompt_version,
                input_tokens, cached_input_tokens, cache_write_tokens, output_tokens, reasoning_tokens,
                latency_ms, input_hash, chart_hash, error, created_at,
                market_regime, decision_summary_ko
             FROM llm_decisions
             WHERE id < ?1
               AND (?2 IS NULL OR control_mode = ?2)
               AND (?3 IS NULL OR status = ?3)
             ORDER BY id DESC
             LIMIT ?4",
        )?;
        let control_mode = query.control_mode.map(|v| v.as_str());
        let status = query.status.map(|v| v.as_str());
        let rows = stmt.query_map(
            params![cursor, control_mode, status, fetch_limit],
            map_decision_without_scenarios,
        )?;
        let mut items = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        let has_more = items.len() > limit;
        if has_more {
            items.truncate(limit);
        }
        for decision in &mut items {
            decision.scenarios = load_scenarios(&conn, &decision.decision_id)?;
        }
        let next_cursor = has_more.then(|| items.last().expect("limit은 1 이상").id);
        Ok(CursorPage { items, next_cursor })
    }

    fn lock(&self) -> LedgerResult<MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(|_| LedgerError::LockPoisoned)
    }
}

fn configure_connection(conn: &mut Connection) -> LedgerResult<()> {
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.execute_batch(
        "PRAGMA foreign_keys = ON;
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;",
    )?;
    Ok(())
}

fn migrate(conn: &mut Connection) -> LedgerResult<()> {
    let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version > SCHEMA_VERSION {
        return Err(LedgerError::UnsupportedSchema(version));
    }
    if version == SCHEMA_VERSION {
        return Ok(());
    }

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if version == 0 {
        tx.execute_batch(
            "CREATE TABLE sessions (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id      TEXT NOT NULL UNIQUE,
                execution_kind  TEXT NOT NULL,
                control_mode    TEXT NOT NULL,
                starting_cash   INTEGER,
                status          TEXT NOT NULL,
                started_at      INTEGER NOT NULL,
                ended_at        INTEGER
             );

             CREATE TABLE llm_decisions (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                decision_id          TEXT NOT NULL UNIQUE,
                session_id           TEXT REFERENCES sessions(session_id) ON DELETE SET NULL,
                control_mode         TEXT NOT NULL,
                revision             INTEGER NOT NULL,
                as_of_ts             INTEGER NOT NULL,
                expires_at           INTEGER NOT NULL,
                underlying_price     REAL NOT NULL,
                status               TEXT NOT NULL,
                model                TEXT NOT NULL,
                prompt_version       TEXT NOT NULL,
                input_tokens         INTEGER NOT NULL DEFAULT 0 CHECK(input_tokens >= 0),
                cached_input_tokens  INTEGER NOT NULL DEFAULT 0 CHECK(cached_input_tokens >= 0),
                cache_write_tokens   INTEGER NOT NULL DEFAULT 0 CHECK(cache_write_tokens >= 0),
                output_tokens        INTEGER NOT NULL DEFAULT 0 CHECK(output_tokens >= 0),
                reasoning_tokens     INTEGER NOT NULL DEFAULT 0 CHECK(reasoning_tokens >= 0),
                latency_ms           INTEGER NOT NULL DEFAULT 0 CHECK(latency_ms >= 0),
                input_hash           TEXT,
                chart_hash           TEXT,
                market_regime        TEXT,
                decision_summary_ko  TEXT,
                error                TEXT,
                created_at           INTEGER NOT NULL
             );

             CREATE TABLE decision_scenarios (
                id                         INTEGER PRIMARY KEY AUTOINCREMENT,
                decision_id                TEXT NOT NULL REFERENCES llm_decisions(decision_id) ON DELETE CASCADE,
                product                    TEXT NOT NULL,
                setup_type                 TEXT,
                reference_price            INTEGER CHECK(reference_price IS NULL OR reference_price > 0),
                confirmation_price         INTEGER CHECK(confirmation_price IS NULL OR confirmation_price > 0),
                invalidation_price         INTEGER CHECK(invalidation_price IS NULL OR invalidation_price > 0),
                trigger_price              INTEGER NOT NULL CHECK(trigger_price > 0),
                target_return_pct          REAL NOT NULL,
                rationale_ko               TEXT,
                status                     TEXT NOT NULL,
                reference_observed_at      INTEGER,
                terminal_reason            TEXT,
                confirmation_started_at    INTEGER,
                confirmation_tick_count    INTEGER NOT NULL DEFAULT 0 CHECK(confirmation_tick_count >= 0),
                updated_at                 INTEGER NOT NULL,
                UNIQUE(decision_id, product)
             );

             CREATE TABLE orders (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                intent_id           TEXT NOT NULL UNIQUE,
                session_id          TEXT REFERENCES sessions(session_id) ON DELETE SET NULL,
                trade_id            TEXT,
                decision_id         TEXT REFERENCES llm_decisions(decision_id) ON DELETE SET NULL,
                scenario_product    TEXT,
                broker_order_id     TEXT,
                broker_org_no       TEXT,
                original_order_id   TEXT,
                trading_date        TEXT NOT NULL,
                execution_kind      TEXT NOT NULL,
                origin              TEXT NOT NULL,
                code                TEXT NOT NULL,
                side                TEXT NOT NULL,
                order_type          TEXT NOT NULL,
                qty                 INTEGER NOT NULL CHECK(qty > 0),
                price               INTEGER,
                status              TEXT NOT NULL,
                message             TEXT,
                requested_at        INTEGER NOT NULL,
                acknowledged_at     INTEGER,
                updated_at          INTEGER NOT NULL
             );

             CREATE TABLE fills (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                fill_key            TEXT NOT NULL,
                trading_date        TEXT NOT NULL,
                intent_id           TEXT REFERENCES orders(intent_id) ON DELETE SET NULL,
                broker_order_id     TEXT NOT NULL,
                broker_org_no       TEXT NOT NULL DEFAULT '',
                broker_fill_id      TEXT,
                execution_kind      TEXT NOT NULL,
                origin              TEXT NOT NULL,
                code                TEXT NOT NULL,
                side                TEXT NOT NULL,
                qty                 INTEGER NOT NULL CHECK(qty > 0),
                price               REAL NOT NULL,
                filled_at           INTEGER NOT NULL,
                raw_json            TEXT,
                UNIQUE(trading_date, broker_org_no, broker_order_id, fill_key),
                UNIQUE(trading_date, broker_org_no, broker_order_id, broker_fill_id)
             );

             CREATE TABLE fill_notices (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                notice_id           TEXT NOT NULL UNIQUE,
                source_notice_id    TEXT,
                trading_date        TEXT NOT NULL,
                broker_order_id     TEXT NOT NULL,
                broker_org_no       TEXT NOT NULL DEFAULT '',
                code                TEXT NOT NULL,
                side                TEXT NOT NULL,
                qty                 INTEGER NOT NULL CHECK(qty > 0),
                price               REAL NOT NULL CHECK(price > 0),
                notified_at         INTEGER NOT NULL,
                received_at         INTEGER NOT NULL,
                raw_json            TEXT
             );

             CREATE TABLE trades (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id            TEXT NOT NULL UNIQUE,
                session_id          TEXT REFERENCES sessions(session_id) ON DELETE SET NULL,
                execution_kind      TEXT NOT NULL,
                origin              TEXT NOT NULL,
                code                TEXT NOT NULL,
                entry_qty           INTEGER NOT NULL CHECK(entry_qty >= 0),
                exit_qty            INTEGER NOT NULL CHECK(exit_qty >= 0 AND exit_qty <= entry_qty),
                entry_avg_price     REAL NOT NULL,
                exit_avg_price      REAL,
                pnl_rate            REAL,
                entered_at          INTEGER NOT NULL,
                exited_at           INTEGER,
                status              TEXT NOT NULL,
                exit_reason         TEXT,
                updated_at          INTEGER NOT NULL
             );

             CREATE TABLE runtime_state (
                key          TEXT PRIMARY KEY,
                value_json   TEXT NOT NULL,
                updated_at   INTEGER NOT NULL
             );

             CREATE INDEX idx_sessions_status
                ON sessions(status, started_at DESC);
             CREATE INDEX idx_decisions_created
                ON llm_decisions(created_at DESC);
             CREATE INDEX idx_scenarios_status
                ON decision_scenarios(status, updated_at DESC);
             CREATE INDEX idx_orders_broker_order_identity
                ON orders(trading_date, broker_org_no, broker_order_id);
             CREATE INDEX idx_orders_status
                ON orders(status, requested_at DESC);
             CREATE INDEX idx_fills_broker_order_identity
                ON fills(trading_date, broker_org_no, broker_order_id, filled_at);
             CREATE INDEX idx_fill_notices_broker_order_identity
                ON fill_notices(
                    trading_date, broker_org_no, broker_order_id, notified_at, received_at
                );
             CREATE INDEX idx_trades_updated
                ON trades(updated_at DESC);

             PRAGMA user_version = 5;",
        )?;
    } else if version == 1 {
        tx.execute_batch(
            "ALTER TABLE orders ADD COLUMN trading_date TEXT NOT NULL DEFAULT '';
             UPDATE orders
                SET trading_date = COALESCE(strftime('%Y%m%d', requested_at, 'unixepoch'), '19700101')
              WHERE trading_date = '';

             DROP INDEX IF EXISTS idx_orders_broker_order;
             CREATE INDEX idx_orders_broker_order_identity
                ON orders(trading_date, broker_org_no, broker_order_id);

             DROP INDEX IF EXISTS idx_fills_broker_order;
             CREATE TABLE fills_v2 (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                fill_key            TEXT NOT NULL,
                trading_date        TEXT NOT NULL,
                intent_id           TEXT REFERENCES orders(intent_id) ON DELETE SET NULL,
                broker_order_id     TEXT NOT NULL,
                broker_org_no       TEXT NOT NULL DEFAULT '',
                broker_fill_id      TEXT,
                execution_kind      TEXT NOT NULL,
                origin              TEXT NOT NULL,
                code                TEXT NOT NULL,
                side                TEXT NOT NULL,
                qty                 INTEGER NOT NULL CHECK(qty > 0),
                price               REAL NOT NULL,
                filled_at           INTEGER NOT NULL,
                raw_json            TEXT,
                UNIQUE(trading_date, broker_org_no, broker_order_id, fill_key),
                UNIQUE(trading_date, broker_org_no, broker_order_id, broker_fill_id)
             );
             INSERT INTO fills_v2(
                id, fill_key, trading_date, intent_id, broker_order_id, broker_org_no,
                broker_fill_id, execution_kind, origin, code, side, qty, price, filled_at, raw_json
             )
             SELECT
                f.id,
                f.fill_key,
                COALESCE(strftime('%Y%m%d', f.filled_at, 'unixepoch'), '19700101'),
                f.intent_id,
                COALESCE(f.broker_order_id, f.fill_key),
                COALESCE(
                    o.broker_org_no,
                    (
                        SELECT CASE
                            WHEN COUNT(DISTINCT COALESCE(o2.broker_org_no, '')) = 1
                            THEN MAX(COALESCE(o2.broker_org_no, ''))
                            ELSE ''
                        END
                        FROM orders o2
                        WHERE o2.broker_order_id = f.broker_order_id
                          AND o2.trading_date = COALESCE(
                              strftime('%Y%m%d', f.filled_at, 'unixepoch'),
                              '19700101'
                          )
                    ),
                    ''
                ),
                f.broker_fill_id,
                f.execution_kind,
                f.origin,
                f.code,
                f.side,
                f.qty,
                f.price,
                f.filled_at,
                f.raw_json
             FROM fills f
             LEFT JOIN orders o ON o.intent_id = f.intent_id;
             DROP TABLE fills;
             ALTER TABLE fills_v2 RENAME TO fills;
             CREATE INDEX idx_fills_broker_order_identity
                ON fills(trading_date, broker_org_no, broker_order_id, filled_at);

             CREATE TABLE fill_notices (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                notice_id           TEXT NOT NULL UNIQUE,
                source_notice_id    TEXT,
                trading_date        TEXT NOT NULL,
                broker_order_id     TEXT NOT NULL,
                broker_org_no       TEXT NOT NULL DEFAULT '',
                code                TEXT NOT NULL,
                side                TEXT NOT NULL,
                qty                 INTEGER NOT NULL CHECK(qty > 0),
                price               REAL NOT NULL CHECK(price > 0),
                notified_at         INTEGER NOT NULL,
                received_at         INTEGER NOT NULL,
                raw_json            TEXT
             );
             CREATE INDEX idx_fill_notices_broker_order_identity
                ON fill_notices(
                    trading_date, broker_org_no, broker_order_id, notified_at, received_at
                );

             PRAGMA user_version = 3;",
        )?;
    } else if version == 2 {
        tx.execute_batch(
            "CREATE TABLE fill_notices (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                notice_id           TEXT NOT NULL UNIQUE,
                source_notice_id    TEXT,
                trading_date        TEXT NOT NULL,
                broker_order_id     TEXT NOT NULL,
                broker_org_no       TEXT NOT NULL DEFAULT '',
                code                TEXT NOT NULL,
                side                TEXT NOT NULL,
                qty                 INTEGER NOT NULL CHECK(qty > 0),
                price               REAL NOT NULL CHECK(price > 0),
                notified_at         INTEGER NOT NULL,
                received_at         INTEGER NOT NULL,
                raw_json            TEXT
             );
             CREATE INDEX idx_fill_notices_broker_order_identity
                ON fill_notices(
                    trading_date, broker_org_no, broker_order_id, notified_at, received_at
                );
             PRAGMA user_version = 3;",
        )?;
    }

    // 구 장부의 GPT-5.6 캐시 쓰기 토큰과 v5 판단 메타데이터를 보강한다.
    // 일부 초기 마이그레이션 테스트 DB에는 주문·체결 표만 있으므로 표 존재를
    // 확인한 뒤 열을 추가한다. 기존 판단·시나리오의 신규 메타데이터는 NULL이다.
    let has_llm_decisions: i64 = tx.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'llm_decisions'",
        [],
        |row| row.get(0),
    )?;
    if has_llm_decisions > 0 {
        let has_cache_write_tokens: i64 = tx.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('llm_decisions') WHERE name = 'cache_write_tokens'",
            [],
            |row| row.get(0),
        )?;
        if has_cache_write_tokens == 0 {
            tx.execute_batch(
                "ALTER TABLE llm_decisions
                 ADD COLUMN cache_write_tokens INTEGER NOT NULL DEFAULT 0
                 CHECK(cache_write_tokens >= 0);",
            )?;
        }

        let has_market_regime: i64 = tx.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('llm_decisions') WHERE name = 'market_regime'",
            [],
            |row| row.get(0),
        )?;
        if has_market_regime == 0 {
            tx.execute_batch(
                "ALTER TABLE llm_decisions ADD COLUMN market_regime TEXT;
                 ALTER TABLE llm_decisions ADD COLUMN decision_summary_ko TEXT;",
            )?;
        }
    }

    let has_decision_scenarios: i64 = tx.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'decision_scenarios'",
        [],
        |row| row.get(0),
    )?;
    if has_decision_scenarios > 0 {
        let has_setup_type: i64 = tx.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('decision_scenarios') WHERE name = 'setup_type'",
            [],
            |row| row.get(0),
        )?;
        if has_setup_type == 0 {
            tx.execute_batch(
                "ALTER TABLE decision_scenarios ADD COLUMN setup_type TEXT;
                 ALTER TABLE decision_scenarios ADD COLUMN reference_price INTEGER
                    CHECK(reference_price IS NULL OR reference_price > 0);
                 ALTER TABLE decision_scenarios ADD COLUMN confirmation_price INTEGER
                    CHECK(confirmation_price IS NULL OR confirmation_price > 0);
                 ALTER TABLE decision_scenarios ADD COLUMN invalidation_price INTEGER
                    CHECK(invalidation_price IS NULL OR invalidation_price > 0);
                 ALTER TABLE decision_scenarios ADD COLUMN rationale_ko TEXT;
                 ALTER TABLE decision_scenarios ADD COLUMN reference_observed_at INTEGER;
                 ALTER TABLE decision_scenarios ADD COLUMN terminal_reason TEXT;",
            )?;
        }
    }
    tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    tx.commit()?;
    Ok(())
}

fn validate_id(field: &'static str, value: &str) -> LedgerResult<()> {
    if value.trim().is_empty() {
        return Err(LedgerError::InvalidInput(format!(
            "{field}는 비어 있을 수 없습니다"
        )));
    }
    Ok(())
}

fn trading_date_from_fake_epoch(timestamp: i64) -> LedgerResult<String> {
    let datetime = DateTime::<Utc>::from_timestamp(timestamp, 0).ok_or_else(|| {
        LedgerError::InvalidInput(format!(
            "거래 시각을 날짜로 변환할 수 없습니다: {timestamp}"
        ))
    })?;
    Ok(datetime.format("%Y%m%d").to_string())
}

fn infer_fill_org_no(
    conn: &Connection,
    trading_date: &str,
    order_no: &str,
    intent_id: Option<&str>,
) -> LedgerResult<String> {
    if let Some(intent_id) = intent_id {
        if let Some(org_no) = conn
            .query_row(
                "SELECT COALESCE(broker_org_no, '') FROM orders WHERE intent_id = ?1",
                [intent_id],
                |row| row.get(0),
            )
            .optional()?
        {
            return Ok(org_no);
        }
    }
    let mut stmt = conn.prepare(
        "SELECT DISTINCT COALESCE(broker_org_no, '') FROM orders
         WHERE trading_date = ?1 AND broker_order_id = ?2",
    )?;
    let candidates = stmt
        .query_map(params![trading_date, order_no], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    match candidates.as_slice() {
        [] => Ok(String::new()),
        [org_no] => Ok(org_no.clone()),
        _ => Err(LedgerError::InvalidInput(format!(
            "조직번호가 없는 체결의 주문 범위가 모호합니다: {trading_date}/{order_no}"
        ))),
    }
}

fn validate_scenarios(scenarios: &[NewDecisionScenario]) -> LedgerResult<()> {
    if scenarios.len() > 2 {
        return Err(LedgerError::InvalidInput(
            "결정에는 시나리오를 최대 2개만 기록할 수 있습니다".into(),
        ));
    }
    let mut products = HashSet::with_capacity(scenarios.len());
    for scenario in scenarios {
        if scenario.trigger_price == 0 {
            return Err(LedgerError::InvalidInput(
                "시나리오 실제 확인가는 1 이상이어야 합니다".into(),
            ));
        }
        for (field, price) in [
            ("reference_price", scenario.reference_price),
            ("confirmation_price", scenario.confirmation_price),
            ("invalidation_price", scenario.invalidation_price),
        ] {
            if price == Some(0) {
                return Err(LedgerError::InvalidInput(format!(
                    "시나리오 {field}는 1 이상이어야 합니다"
                )));
            }
        }
        if !products.insert(scenario.product.as_str()) {
            return Err(LedgerError::InvalidInput(
                "한 결정에 같은 상품 시나리오를 중복 기록할 수 없습니다".into(),
            ));
        }
    }
    Ok(())
}

fn u64_to_i64(value: u64, field: &'static str) -> LedgerResult<i64> {
    i64::try_from(value).map_err(|_| LedgerError::OutOfRange(field))
}

fn optional_u64_to_i64(value: Option<u64>, field: &'static str) -> LedgerResult<Option<i64>> {
    value.map(|value| u64_to_i64(value, field)).transpose()
}

fn i64_to_u64(value: i64, column: usize) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

fn optional_i64_to_u64(value: Option<i64>, column: usize) -> rusqlite::Result<Option<u64>> {
    value.map(|value| i64_to_u64(value, column)).transpose()
}

fn ensure_changed(changed: usize, kind: &str, id: &str) -> LedgerResult<()> {
    if changed == 0 {
        Err(LedgerError::NotFound(format!("{kind}:{id}")))
    } else {
        Ok(())
    }
}

fn normalize_limit(limit: usize) -> usize {
    if limit == 0 {
        DEFAULT_PAGE_SIZE
    } else {
        limit.min(MAX_PAGE_SIZE)
    }
}

fn map_order(row: &rusqlite::Row<'_>) -> rusqlite::Result<OrderRecord> {
    let qty: i64 = row.get(15)?;
    let price: Option<i64> = row.get(16)?;
    Ok(OrderRecord {
        id: row.get(0)?,
        intent_id: row.get(1)?,
        session_id: row.get(2)?,
        trade_id: row.get(3)?,
        decision_id: row.get(4)?,
        scenario_product: row.get(5)?,
        broker_order_id: row.get(6)?,
        broker_org_no: row.get(7)?,
        original_order_id: row.get(8)?,
        trading_date: row.get(9)?,
        execution_kind: row.get(10)?,
        origin: row.get(11)?,
        code: row.get(12)?,
        side: row.get(13)?,
        order_type: row.get(14)?,
        qty: i64_to_u64(qty, 15)?,
        price: optional_i64_to_u64(price, 16)?,
        status: row.get(17)?,
        message: row.get(18)?,
        requested_at: row.get(19)?,
        acknowledged_at: row.get(20)?,
        updated_at: row.get(21)?,
    })
}

fn map_trade(row: &rusqlite::Row<'_>) -> rusqlite::Result<TradeRecord> {
    let entry_qty: i64 = row.get(6)?;
    let exit_qty: i64 = row.get(7)?;
    Ok(TradeRecord {
        id: row.get(0)?,
        trade_id: row.get(1)?,
        session_id: row.get(2)?,
        execution_kind: row.get(3)?,
        origin: row.get(4)?,
        code: row.get(5)?,
        entry_qty: i64_to_u64(entry_qty, 6)?,
        exit_qty: i64_to_u64(exit_qty, 7)?,
        entry_avg_price: row.get(8)?,
        exit_avg_price: row.get(9)?,
        pnl_rate: row.get(10)?,
        entered_at: row.get(11)?,
        exited_at: row.get(12)?,
        status: row.get(13)?,
        exit_reason: row.get(14)?,
        updated_at: row.get(15)?,
    })
}

fn map_decision_without_scenarios(row: &rusqlite::Row<'_>) -> rusqlite::Result<DecisionRecord> {
    let input_tokens: i64 = row.get(11)?;
    let cached_input_tokens: i64 = row.get(12)?;
    let cache_write_tokens: i64 = row.get(13)?;
    let output_tokens: i64 = row.get(14)?;
    let reasoning_tokens: i64 = row.get(15)?;
    let latency_ms: i64 = row.get(16)?;
    Ok(DecisionRecord {
        id: row.get(0)?,
        decision_id: row.get(1)?,
        session_id: row.get(2)?,
        control_mode: row.get(3)?,
        revision: row.get(4)?,
        as_of_ts: row.get(5)?,
        expires_at: row.get(6)?,
        underlying_price: row.get(7)?,
        status: row.get(8)?,
        model: row.get(9)?,
        prompt_version: row.get(10)?,
        input_tokens: i64_to_u64(input_tokens, 11)?,
        cached_input_tokens: i64_to_u64(cached_input_tokens, 12)?,
        cache_write_tokens: i64_to_u64(cache_write_tokens, 13)?,
        output_tokens: i64_to_u64(output_tokens, 14)?,
        reasoning_tokens: i64_to_u64(reasoning_tokens, 15)?,
        latency_ms: i64_to_u64(latency_ms, 16)?,
        input_hash: row.get(17)?,
        chart_hash: row.get(18)?,
        error: row.get(19)?,
        created_at: row.get(20)?,
        market_regime: row.get(21)?,
        decision_summary_ko: row.get(22)?,
        scenarios: Vec::new(),
    })
}

fn load_scenarios(conn: &Connection, decision_id: &str) -> LedgerResult<Vec<ScenarioRecord>> {
    let mut stmt = conn.prepare(
        "SELECT
            id, decision_id, product, trigger_price, target_return_pct, status,
            confirmation_started_at, confirmation_tick_count, updated_at,
            setup_type, reference_price, confirmation_price, invalidation_price,
            rationale_ko, reference_observed_at, terminal_reason
         FROM decision_scenarios
         WHERE decision_id = ?1
         ORDER BY id ASC",
    )?;
    let rows = stmt.query_map([decision_id], |row| {
        let trigger_price: i64 = row.get(3)?;
        let reference_price: Option<i64> = row.get(10)?;
        let confirmation_price: Option<i64> = row.get(11)?;
        let invalidation_price: Option<i64> = row.get(12)?;
        let tick_count: i64 = row.get(7)?;
        let confirmation_tick_count = u32::try_from(tick_count).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Integer,
                Box::new(error),
            )
        })?;
        Ok(ScenarioRecord {
            id: row.get(0)?,
            decision_id: row.get(1)?,
            product: row.get(2)?,
            setup_type: row.get(9)?,
            reference_price: optional_i64_to_u64(reference_price, 10)?,
            confirmation_price: optional_i64_to_u64(confirmation_price, 11)?,
            invalidation_price: optional_i64_to_u64(invalidation_price, 12)?,
            trigger_price: i64_to_u64(trigger_price, 3)?,
            target_return_pct: row.get(4)?,
            rationale_ko: row.get(13)?,
            status: row.get(5)?,
            reference_observed_at: row.get(14)?,
            terminal_reason: row.get(15)?,
            confirmation_started_at: row.get(6)?,
            confirmation_tick_count,
            updated_at: row.get(8)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_epoch(date: &str, time: &str) -> i64 {
        let value = format!("{date}{time}");
        chrono::NaiveDateTime::parse_from_str(&value, "%Y%m%d%H%M%S")
            .unwrap()
            .and_utc()
            .timestamp()
    }

    fn session() -> NewSession {
        NewSession {
            session_id: "session-1".into(),
            execution_kind: LedgerExecutionKind::Real,
            control_mode: LedgerControlMode::Auto,
            starting_cash: Some(10_000_000),
            started_at: 1_000,
        }
    }

    fn decision(id: &str, revision: i64) -> NewDecision {
        NewDecision {
            decision_id: id.into(),
            session_id: Some("session-1".into()),
            control_mode: LedgerControlMode::Auto,
            revision,
            as_of_ts: 1_100 + revision,
            expires_at: 1_400 + revision,
            underlying_price: 185_000.0,
            status: LedgerDecisionStatus::Armed,
            model: "gpt-5.6-sol".into(),
            prompt_version: "oco-v1".into(),
            input_tokens: 100,
            cached_input_tokens: 0,
            cache_write_tokens: 12,
            output_tokens: 20,
            reasoning_tokens: 30,
            latency_ms: 800,
            input_hash: Some("input-hash".into()),
            chart_hash: Some("chart-hash".into()),
            market_regime: Some(LedgerMarketRegime::Range),
            decision_summary_ko: Some("저항 반락과 지지 반등 후보를 함께 감시".into()),
            error: None,
            created_at: 1_100 + revision,
        }
    }

    fn scenarios() -> Vec<NewDecisionScenario> {
        vec![
            NewDecisionScenario {
                product: LedgerProductKind::Leverage,
                setup_type: Some(LedgerSetupType::Continuation),
                reference_price: Some(185_800),
                confirmation_price: Some(186_000),
                invalidation_price: Some(185_400),
                trigger_price: 186_000,
                target_return_pct: 0.3,
                rationale_ko: Some("거래량을 동반한 상단 압축 돌파".into()),
                status: LedgerScenarioStatus::Armed,
                reference_observed_at: None,
                terminal_reason: None,
            },
            NewDecisionScenario {
                product: LedgerProductKind::Inverse,
                setup_type: Some(LedgerSetupType::Reversal),
                reference_price: Some(185_500),
                confirmation_price: Some(185_200),
                invalidation_price: Some(185_800),
                trigger_price: 183_500,
                target_return_pct: 0.2,
                rationale_ko: Some("저항 반복 시험 뒤 거래량 소진".into()),
                status: LedgerScenarioStatus::Armed,
                reference_observed_at: Some(1_090),
                terminal_reason: None,
            },
        ]
    }

    #[test]
    fn 메모리_장부에_스키마와_제어_모드를_저장한다() {
        let ledger = Ledger::open_in_memory().unwrap();
        assert_eq!(ledger.schema_version().unwrap(), SCHEMA_VERSION);
        assert_eq!(ledger.control_mode().unwrap(), None);

        ledger
            .set_control_mode(LedgerControlMode::Shadow, 100)
            .unwrap();
        assert_eq!(
            ledger.control_mode().unwrap(),
            Some(LedgerControlMode::Shadow)
        );
    }

    #[test]
    fn 런타임_초기화는_기록을_보존하고_활성_세션을_중단한다() {
        let ledger = Ledger::open_in_memory().unwrap();
        ledger.start_session(&session()).unwrap();
        ledger
            .upsert_trade(&NewTrade {
                trade_id: "reset-trade".into(),
                session_id: Some("session-1".into()),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                entry_qty: 10,
                exit_qty: 0,
                entry_avg_price: 10_000.0,
                exit_avg_price: None,
                pnl_rate: None,
                entered_at: 1_100,
                exited_at: None,
                status: LedgerTradeStatus::Open,
                exit_reason: None,
                updated_at: 1_100,
            })
            .unwrap();
        ledger
            .set_runtime_state(
                "automation_runtime_v1",
                &serde_json::json!({ "stale": true }),
                1_200,
            )
            .unwrap();
        ledger
            .set_control_mode(LedgerControlMode::Auto, 1_200)
            .unwrap();

        ledger.reset_runtime_state_to_manual(1_300).unwrap();

        assert_eq!(
            ledger.control_mode().unwrap(),
            Some(LedgerControlMode::Manual)
        );
        assert!(ledger
            .get_runtime_state::<serde_json::Value>("automation_runtime_v1")
            .unwrap()
            .is_none());
        let trades = ledger
            .list_trades(&TradeQuery::default(), None, 10)
            .unwrap();
        assert_eq!(trades.items.len(), 1);
        assert_eq!(trades.items[0].status, LedgerTradeStatus::Interrupted);
        assert_eq!(
            trades.items[0].exit_reason.as_deref(),
            Some("runtime_reset")
        );
    }

    #[test]
    fn 동일초_동일수량_동일가격_ws_통보도_각_수신건을_보존한다() {
        let ledger = Ledger::open_in_memory().unwrap();
        let notified_at = crate::util::now_kst_fake_epoch();
        let base = NewFillNotice {
            notice_id: "notice-1".into(),
            source_notice_id: Some("same-broker-hint".into()),
            broker_order_id: "order-1".into(),
            broker_org_no: None,
            code: "0193T0".into(),
            side: LedgerSide::Buy,
            qty: 3,
            price: 10_005.0,
            notified_at,
            received_at: notified_at,
            raw_json: None,
        };
        ledger.record_fill_notice(&base).unwrap();
        ledger
            .record_fill_notice(&NewFillNotice {
                notice_id: "notice-2".into(),
                ..base
            })
            .unwrap();

        let conn = ledger.lock().unwrap();
        let (count, distinct_source): (i64, i64) = conn
            .query_row(
                "SELECT COUNT(*), COUNT(DISTINCT source_notice_id) FROM fill_notices",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(distinct_source, 1);
    }

    #[test]
    fn 버전2_장부에_체결통보_감사테이블을_마이그레이션한다() {
        let mut conn = Connection::open_in_memory().unwrap();
        configure_connection(&mut conn).unwrap();
        conn.execute_batch("PRAGMA user_version = 2;").unwrap();

        migrate(&mut conn).unwrap();

        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'fill_notices'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            1
        );
    }

    #[test]
    fn 버전3_장부에_캐시쓰기_토큰열을_마이그레이션한다() {
        let mut conn = Connection::open_in_memory().unwrap();
        configure_connection(&mut conn).unwrap();
        conn.execute_batch(
            "CREATE TABLE llm_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                decision_id TEXT NOT NULL UNIQUE,
                cached_input_tokens INTEGER NOT NULL DEFAULT 0
             );
             INSERT INTO llm_decisions(decision_id, cached_input_tokens)
             VALUES ('legacy-decision', 42);
             PRAGMA user_version = 3;",
        )
        .unwrap();

        migrate(&mut conn).unwrap();

        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        assert_eq!(
            conn.query_row(
                "SELECT cache_write_tokens FROM llm_decisions WHERE decision_id = 'legacy-decision'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn 버전4_판단과_시나리오를_nullable_v5_메타데이터로_마이그레이션한다() {
        let mut conn = Connection::open_in_memory().unwrap();
        configure_connection(&mut conn).unwrap();
        conn.execute_batch(
            "CREATE TABLE llm_decisions (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                decision_id          TEXT NOT NULL UNIQUE,
                session_id           TEXT,
                control_mode         TEXT NOT NULL,
                revision             INTEGER NOT NULL,
                as_of_ts             INTEGER NOT NULL,
                expires_at           INTEGER NOT NULL,
                underlying_price     REAL NOT NULL,
                status               TEXT NOT NULL,
                model                TEXT NOT NULL,
                prompt_version       TEXT NOT NULL,
                input_tokens         INTEGER NOT NULL DEFAULT 0,
                cached_input_tokens  INTEGER NOT NULL DEFAULT 0,
                cache_write_tokens   INTEGER NOT NULL DEFAULT 0,
                output_tokens        INTEGER NOT NULL DEFAULT 0,
                reasoning_tokens     INTEGER NOT NULL DEFAULT 0,
                latency_ms           INTEGER NOT NULL DEFAULT 0,
                input_hash           TEXT,
                chart_hash           TEXT,
                error                TEXT,
                created_at           INTEGER NOT NULL
             );
             CREATE TABLE decision_scenarios (
                id                         INTEGER PRIMARY KEY AUTOINCREMENT,
                decision_id                TEXT NOT NULL,
                product                    TEXT NOT NULL,
                trigger_price              INTEGER NOT NULL,
                target_return_pct          REAL NOT NULL,
                status                     TEXT NOT NULL,
                confirmation_started_at    INTEGER,
                confirmation_tick_count    INTEGER NOT NULL DEFAULT 0,
                updated_at                 INTEGER NOT NULL,
                UNIQUE(decision_id, product)
             );
             INSERT INTO llm_decisions(
                decision_id, control_mode, revision, as_of_ts, expires_at,
                underlying_price, status, model, prompt_version, created_at
             ) VALUES (
                'legacy-v4', 'auto', 1, 1100, 1400,
                185000, 'armed', 'gpt-5.6-sol', 'oco-v3', 1101
             );
             INSERT INTO decision_scenarios(
                decision_id, product, trigger_price, target_return_pct, status, updated_at
             ) VALUES ('legacy-v4', 'inverse', 184900, 0.3, 'armed', 1101);
             PRAGMA user_version = 4;",
        )
        .unwrap();

        migrate(&mut conn).unwrap();

        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        let ledger = Ledger {
            conn: Mutex::new(conn),
        };
        let page = ledger
            .list_decisions(&DecisionQuery::default(), None, 10)
            .unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].market_regime, None);
        assert_eq!(page.items[0].decision_summary_ko, None);
        assert_eq!(page.items[0].scenarios.len(), 1);
        let scenario = &page.items[0].scenarios[0];
        assert_eq!(scenario.setup_type, None);
        assert_eq!(scenario.reference_price, None);
        assert_eq!(scenario.confirmation_price, None);
        assert_eq!(scenario.invalidation_price, None);
        assert_eq!(scenario.rationale_ko, None);
        assert_eq!(scenario.reference_observed_at, None);
        assert_eq!(scenario.terminal_reason, None);
        assert_eq!(scenario.trigger_price, 184_900);
    }

    #[test]
    fn 결정과_양방향_시나리오를_원자적으로_기록하고_갱신한다() {
        let ledger = Ledger::open_in_memory().unwrap();
        ledger.start_session(&session()).unwrap();
        ledger
            .record_decision(&decision("decision-1", 1), &scenarios())
            .unwrap();

        ledger
            .update_decision_and_scenarios(
                "decision-1",
                Some(LedgerDecisionStatus::Triggered),
                None,
                &[
                    ScenarioStatusUpdate {
                        product: LedgerProductKind::Leverage,
                        status: LedgerScenarioStatus::Confirming,
                        confirmation_started_at: Some(1_200),
                        confirmation_tick_count: 2,
                        updated_at: 1_202,
                        terminal_reason: None,
                        reference_observed_at: Some(1_198),
                    },
                    ScenarioStatusUpdate {
                        product: LedgerProductKind::Inverse,
                        status: LedgerScenarioStatus::Invalidated,
                        confirmation_started_at: None,
                        confirmation_tick_count: 0,
                        updated_at: 1_203,
                        terminal_reason: Some("무효화가 선행 침범"),
                        reference_observed_at: Some(1_090),
                    },
                ],
            )
            .unwrap();

        let page = ledger
            .list_decisions(&DecisionQuery::default(), None, 10)
            .unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].status, LedgerDecisionStatus::Triggered);
        assert_eq!(page.items[0].cache_write_tokens, 12);
        assert_eq!(page.items[0].market_regime, Some(LedgerMarketRegime::Range));
        assert_eq!(
            page.items[0].decision_summary_ko.as_deref(),
            Some("저항 반락과 지지 반등 후보를 함께 감시")
        );
        assert_eq!(
            serde_json::to_value(&page.items[0]).unwrap()["cacheWriteTokens"],
            12
        );
        assert_eq!(page.items[0].scenarios.len(), 2);
        assert_eq!(
            page.items[0].scenarios[0].status,
            LedgerScenarioStatus::Confirming
        );
        assert_eq!(page.items[0].scenarios[0].confirmation_tick_count, 2);
        assert_eq!(
            page.items[0].scenarios[0].setup_type,
            Some(LedgerSetupType::Continuation)
        );
        assert_eq!(page.items[0].scenarios[0].reference_price, Some(185_800));
        assert_eq!(page.items[0].scenarios[0].confirmation_price, Some(186_000));
        assert_eq!(page.items[0].scenarios[0].invalidation_price, Some(185_400));
        assert_eq!(
            page.items[0].scenarios[0].rationale_ko.as_deref(),
            Some("거래량을 동반한 상단 압축 돌파")
        );
        assert_eq!(
            page.items[0].scenarios[0].reference_observed_at,
            Some(1_198)
        );
        assert_eq!(page.items[0].scenarios[0].terminal_reason, None);
        assert_eq!(
            page.items[0].scenarios[1].status,
            LedgerScenarioStatus::Invalidated
        );
        assert_eq!(
            page.items[0].scenarios[1].terminal_reason.as_deref(),
            Some("무효화가 선행 침범")
        );
    }

    #[test]
    fn 부모자식_상태갱신은_한_시나리오라도_없으면_전부_롤백한다() {
        let ledger = Ledger::open_in_memory().unwrap();
        ledger.start_session(&session()).unwrap();
        let one = vec![scenarios().remove(0)];
        ledger
            .record_decision(&decision("decision-atomic", 1), &one)
            .unwrap();

        let result = ledger.update_decision_and_scenarios(
            "decision-atomic",
            Some(LedgerDecisionStatus::Triggered),
            None,
            &[
                ScenarioStatusUpdate {
                    product: LedgerProductKind::Leverage,
                    status: LedgerScenarioStatus::Confirming,
                    confirmation_started_at: Some(1_200),
                    confirmation_tick_count: 2,
                    updated_at: 1_202,
                    terminal_reason: None,
                    reference_observed_at: None,
                },
                ScenarioStatusUpdate {
                    product: LedgerProductKind::Inverse,
                    status: LedgerScenarioStatus::Invalidated,
                    confirmation_started_at: None,
                    confirmation_tick_count: 0,
                    updated_at: 1_203,
                    terminal_reason: Some("존재하지 않는 행"),
                    reference_observed_at: None,
                },
            ],
        );
        assert!(matches!(result, Err(LedgerError::NotFound(_))));

        let page = ledger
            .list_decisions(&DecisionQuery::default(), None, 10)
            .unwrap();
        assert_eq!(page.items[0].status, LedgerDecisionStatus::Armed);
        assert_eq!(
            page.items[0].scenarios[0].status,
            LedgerScenarioStatus::Armed
        );
        assert_eq!(page.items[0].scenarios[0].confirmation_tick_count, 0);
    }

    #[test]
    fn 재시작은_복원불가_oco를_슬롯여부에_따라_원자종결한다() {
        let ledger = Ledger::open_in_memory().unwrap();
        ledger.start_session(&session()).unwrap();

        let mut within_slot = decision("decision-restart-discarded", 1);
        within_slot.expires_at = 2_000;
        ledger.record_decision(&within_slot, &scenarios()).unwrap();

        let mut expired = decision("decision-restart-expired", 2);
        expired.expires_at = 1_200;
        ledger.record_decision(&expired, &scenarios()).unwrap();

        let mut triggered = decision("decision-restart-triggered", 3);
        triggered.status = LedgerDecisionStatus::Triggered;
        ledger.record_decision(&triggered, &scenarios()).unwrap();

        assert_eq!(ledger.close_unrestorable_oco_decisions(1_500).unwrap(), 2);

        let page = ledger
            .list_decisions(&DecisionQuery::default(), None, 10)
            .unwrap();
        let discarded = page
            .items
            .iter()
            .find(|item| item.decision_id == within_slot.decision_id)
            .unwrap();
        assert_eq!(discarded.status, LedgerDecisionStatus::Discarded);
        assert!(discarded
            .scenarios
            .iter()
            .all(|scenario| scenario.status == LedgerScenarioStatus::Replaced));
        assert!(discarded.scenarios.iter().all(|scenario| {
            scenario.terminal_reason.as_deref() == Some("앱 재시작으로 메모리 OCO를 복원하지 않음")
        }));

        let expired = page
            .items
            .iter()
            .find(|item| item.decision_id == expired.decision_id)
            .unwrap();
        assert_eq!(expired.status, LedgerDecisionStatus::Expired);
        assert!(expired
            .scenarios
            .iter()
            .all(|scenario| scenario.status == LedgerScenarioStatus::Expired));

        let triggered = page
            .items
            .iter()
            .find(|item| item.decision_id == triggered.decision_id)
            .unwrap();
        assert_eq!(triggered.status, LedgerDecisionStatus::Triggered);
        assert!(triggered
            .scenarios
            .iter()
            .all(|scenario| scenario.status == LedgerScenarioStatus::Armed));
    }

    #[test]
    fn 추격금지와_무효화_종결상태를_장부에서_왕복한다() {
        let ledger = Ledger::open_in_memory().unwrap();
        ledger.start_session(&session()).unwrap();
        let mut decision = decision("decision-terminal", 1);
        decision.status = LedgerDecisionStatus::Missed;
        let mut scenarios = scenarios();
        scenarios[0].status = LedgerScenarioStatus::Missed;
        scenarios[0].terminal_reason = Some("응답 적용 전에 확인가 통과".into());
        scenarios[1].status = LedgerScenarioStatus::Invalidated;
        scenarios[1].terminal_reason = Some("응답 적용 전에 무효화가 침범".into());

        ledger.record_decision(&decision, &scenarios).unwrap();

        let page = ledger
            .list_decisions(&DecisionQuery::default(), None, 10)
            .unwrap();
        assert_eq!(page.items[0].status, LedgerDecisionStatus::Missed);
        assert_eq!(
            page.items[0].scenarios[0].status,
            LedgerScenarioStatus::Missed
        );
        assert_eq!(
            page.items[0].scenarios[1].status,
            LedgerScenarioStatus::Invalidated
        );
    }

    #[test]
    fn 중복_상품_시나리오는_결정_전체를_기록하지_않는다() {
        let ledger = Ledger::open_in_memory().unwrap();
        ledger.start_session(&session()).unwrap();
        let mut duplicated = scenarios();
        duplicated[1].product = LedgerProductKind::Leverage;

        assert!(ledger
            .record_decision(&decision("decision-invalid", 1), &duplicated)
            .is_err());
        assert!(ledger
            .list_decisions(&DecisionQuery::default(), None, 10)
            .unwrap()
            .items
            .is_empty());
    }

    #[test]
    fn 주문_의도와_ack를_기록하고_체결을_중복_제거한다() {
        let ledger = Ledger::open_in_memory().unwrap();
        ledger.start_session(&session()).unwrap();
        ledger
            .record_decision(&decision("decision-1", 1), &scenarios())
            .unwrap();
        ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: "intent-1".into(),
                session_id: Some("session-1".into()),
                trade_id: Some("trade-1".into()),
                decision_id: Some("decision-1".into()),
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                side: LedgerSide::Buy,
                order_type: LedgerOrderType::IocLimit,
                qty: 100,
                price: Some(10_000),
                requested_at: 1_300,
            })
            .unwrap();
        ledger
            .record_order_ack(
                "intent-1",
                &OrderAcknowledgement {
                    broker_order_id: Some("kis-order-1".into()),
                    broker_org_no: Some("org-1".into()),
                    original_order_id: None,
                    status: LedgerOrderStatus::Submitted,
                    message: None,
                    acknowledged_at: 1_301,
                },
            )
            .unwrap();

        let fill = NewFill {
            fill_key: "kis-order-1/fill-1".into(),
            intent_id: Some("intent-1".into()),
            broker_order_id: Some("kis-order-1".into()),
            broker_fill_id: Some("fill-1".into()),
            execution_kind: LedgerExecutionKind::Real,
            origin: LedgerOrigin::Auto,
            code: "0193T0".into(),
            side: LedgerSide::Buy,
            qty: 40,
            price: 9_980.0,
            filled_at: 1_302,
            raw_json: None,
        };
        assert!(ledger.record_fill(&fill).unwrap());
        assert!(!ledger.record_fill(&fill).unwrap());

        let order = ledger.get_order("intent-1").unwrap().unwrap();
        assert_eq!(order.broker_order_id.as_deref(), Some("kis-order-1"));
        assert_eq!(order.status, LedgerOrderStatus::Submitted);
    }

    #[test]
    fn 주문_dispatch는_intent에서_정확히_한_번만_성공한다() {
        let ledger = Ledger::open_in_memory().unwrap();
        ledger
            .record_order_intent(&NewOrderIntent {
                intent_id: "dispatch-once".into(),
                session_id: None,
                trade_id: Some("dispatch-trade".into()),
                decision_id: None,
                scenario_product: Some(LedgerProductKind::Leverage),
                execution_kind: LedgerExecutionKind::Real,
                origin: LedgerOrigin::Auto,
                code: "0193T0".into(),
                side: LedgerSide::Buy,
                order_type: LedgerOrderType::IocLimit,
                qty: 10,
                price: Some(10_000),
                requested_at: 1_300,
            })
            .unwrap();

        assert!(ledger.begin_order_dispatch("dispatch-once", 1_301).unwrap());
        assert!(!ledger.begin_order_dispatch("dispatch-once", 1_302).unwrap());
        assert_eq!(
            ledger.get_order("dispatch-once").unwrap().unwrap().status,
            LedgerOrderStatus::Dispatching
        );
    }

    #[test]
    fn 같은_주문번호도_거래일과_조직번호별로_분리한다() {
        let ledger = Ledger::open_in_memory().unwrap();
        let day1 = fake_epoch("20260721", "100000");
        let day2 = fake_epoch("20260722", "100000");

        for (intent_id, requested_at, origin, org_no) in [
            ("day1-auto", day1, LedgerOrigin::Auto, "org-a"),
            ("day2-manual", day2, LedgerOrigin::Manual, "org-a"),
            ("day2-auto", day2 + 1, LedgerOrigin::Auto, "org-b"),
        ] {
            ledger
                .record_order_intent(&NewOrderIntent {
                    intent_id: intent_id.into(),
                    session_id: None,
                    trade_id: None,
                    decision_id: None,
                    scenario_product: None,
                    execution_kind: LedgerExecutionKind::Real,
                    origin,
                    code: "0193T0".into(),
                    side: LedgerSide::Buy,
                    order_type: LedgerOrderType::IocLimit,
                    qty: 10,
                    price: Some(10_000),
                    requested_at,
                })
                .unwrap();
            ledger
                .record_order_ack(
                    intent_id,
                    &OrderAcknowledgement {
                        broker_order_id: Some("reused-order".into()),
                        broker_org_no: Some(org_no.into()),
                        original_order_id: None,
                        status: LedgerOrderStatus::Submitted,
                        message: None,
                        acknowledged_at: requested_at + 1,
                    },
                )
                .unwrap();
        }

        let cases = [
            ("20260721", "org-a", "day1-auto", day1, 3_u64),
            ("20260722", "org-a", "day2-manual", day2, 5_u64),
            ("20260722", "org-b", "day2-auto", day2 + 1, 7_u64),
        ];
        for (date, org_no, intent_id, filled_at, qty) in cases {
            let key = BrokerOrderKey::new(date, org_no, "reused-order").unwrap();
            let fill = NewFill {
                // KIS가 같은 체결번호를 다음 날 또는 다른 조직에서 재사용해도 충돌하지 않는다.
                fill_key: "fill-1".into(),
                intent_id: Some(intent_id.into()),
                broker_order_id: Some("reused-order".into()),
                broker_fill_id: Some("fill-1".into()),
                execution_kind: LedgerExecutionKind::Real,
                origin: if intent_id.contains("manual") {
                    LedgerOrigin::Manual
                } else {
                    LedgerOrigin::Auto
                },
                code: "0193T0".into(),
                side: LedgerSide::Buy,
                qty,
                price: 10_000.0,
                filled_at,
                raw_json: None,
            };
            assert!(ledger.record_fill_for_broker_order(&key, &fill).unwrap());
            assert!(!ledger.record_fill_for_broker_order(&key, &fill).unwrap());
            assert_eq!(ledger.filled_qty_for_broker_order(&key).unwrap(), qty);
        }

        let old_auto = BrokerOrderKey::new("20260721", "org-a", "reused-order").unwrap();
        let today_manual = BrokerOrderKey::new("20260722", "org-a", "reused-order").unwrap();
        let today_auto = BrokerOrderKey::new("20260722", "org-b", "reused-order").unwrap();
        assert!(ledger.is_auto_broker_order_key(&old_auto).unwrap());
        assert!(!ledger.is_auto_broker_order_key(&today_manual).unwrap());
        assert!(ledger.is_auto_broker_order_key(&today_auto).unwrap());

        let ambiguous_ws_fill = NewFill {
            fill_key: "ambiguous-fill".into(),
            intent_id: None,
            broker_order_id: Some("reused-order".into()),
            broker_fill_id: Some("ambiguous-fill".into()),
            execution_kind: LedgerExecutionKind::Real,
            origin: LedgerOrigin::External,
            code: "0193T0".into(),
            side: LedgerSide::Buy,
            qty: 1,
            price: 10_000.0,
            filled_at: day2 + 2,
            raw_json: None,
        };
        assert!(matches!(
            ledger.record_fill(&ambiguous_ws_fill),
            Err(LedgerError::InvalidInput(_))
        ));

        assert_eq!(
            ledger.get_order("day2-auto").unwrap().unwrap().trading_date,
            "20260722"
        );
    }

    #[test]
    fn 버전1_장부를_주문_복합키_스키마로_마이그레이션한다() {
        let mut conn = Connection::open_in_memory().unwrap();
        configure_connection(&mut conn).unwrap();
        conn.execute_batch(
            "CREATE TABLE orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                intent_id TEXT NOT NULL UNIQUE,
                broker_order_id TEXT,
                broker_org_no TEXT,
                origin TEXT NOT NULL,
                requested_at INTEGER NOT NULL
             );
             CREATE TABLE fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fill_key TEXT NOT NULL UNIQUE,
                intent_id TEXT,
                broker_order_id TEXT,
                broker_fill_id TEXT,
                execution_kind TEXT NOT NULL,
                origin TEXT NOT NULL,
                code TEXT NOT NULL,
                side TEXT NOT NULL,
                qty INTEGER NOT NULL,
                price REAL NOT NULL,
                filled_at INTEGER NOT NULL,
                raw_json TEXT,
                UNIQUE(broker_order_id, broker_fill_id)
             );
             CREATE INDEX idx_orders_broker_order ON orders(broker_order_id);
             CREATE INDEX idx_fills_broker_order ON fills(broker_order_id, filled_at);
             INSERT INTO orders(intent_id, broker_order_id, broker_org_no, origin, requested_at)
             VALUES ('legacy-intent', 'order-1', 'org-1', 'auto', 1784601000);
             INSERT INTO fills(
                fill_key, intent_id, broker_order_id, broker_fill_id, execution_kind,
                origin, code, side, qty, price, filled_at, raw_json
             ) VALUES (
                'legacy-fill', 'legacy-intent', 'order-1', 'fill-1', 'real',
                'auto', '0193T0', 'buy', 4, 10000, 1784601001, NULL
             );
             INSERT INTO fills(
                fill_key, intent_id, broker_order_id, broker_fill_id, execution_kind,
                origin, code, side, qty, price, filled_at, raw_json
             ) VALUES (
                'legacy-ws-fill', NULL, 'order-1', 'fill-2', 'real',
                'auto', '0193T0', 'buy', 2, 10000, 1784601002, NULL
             );
             PRAGMA user_version = 1;",
        )
        .unwrap();

        migrate(&mut conn).unwrap();
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        let (order_date, fill_date, org_no): (String, String, String) = conn
            .query_row(
                "SELECT o.trading_date, f.trading_date, f.broker_org_no
                 FROM orders o JOIN fills f ON f.intent_id = o.intent_id",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(order_date, fill_date);
        assert_eq!(org_no, "org-1");
        assert_eq!(
            conn.query_row(
                "SELECT broker_org_no FROM fills WHERE fill_key = 'legacy-ws-fill'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
            "org-1"
        );

        let ledger = Ledger {
            conn: Mutex::new(conn),
        };
        let key = BrokerOrderKey::new(fill_date, "org-1", "order-1").unwrap();
        assert_eq!(ledger.filled_qty_for_broker_order(&key).unwrap(), 6);
        assert!(ledger.is_auto_broker_order_key(&key).unwrap());
    }

    #[test]
    fn 거래와_결정_목록을_cursor로_나눈다() {
        let ledger = Ledger::open_in_memory().unwrap();
        ledger.start_session(&session()).unwrap();
        for index in 0..3 {
            ledger
                .record_decision(&decision(&format!("decision-{index}"), index), &scenarios())
                .unwrap();
            ledger
                .upsert_trade(&NewTrade {
                    trade_id: format!("trade-{index}"),
                    session_id: Some("session-1".into()),
                    execution_kind: LedgerExecutionKind::Real,
                    origin: LedgerOrigin::Auto,
                    code: "0193T0".into(),
                    entry_qty: 100,
                    exit_qty: 100,
                    entry_avg_price: 10_000.0,
                    exit_avg_price: Some(10_030.0),
                    pnl_rate: Some(0.3),
                    entered_at: 2_000 + index,
                    exited_at: Some(2_100 + index),
                    status: LedgerTradeStatus::Closed,
                    exit_reason: Some("target".into()),
                    updated_at: 2_100 + index,
                })
                .unwrap();
        }

        let first = ledger.list_trades(&TradeQuery::default(), None, 2).unwrap();
        assert_eq!(first.items.len(), 2);
        assert_eq!(first.items[0].trade_id, "trade-2");
        assert!(first.next_cursor.is_some());

        let second = ledger
            .list_trades(&TradeQuery::default(), first.next_cursor, 2)
            .unwrap();
        assert_eq!(second.items.len(), 1);
        assert_eq!(second.items[0].trade_id, "trade-0");
        assert_eq!(second.next_cursor, None);

        let decision_page = ledger
            .list_decisions(&DecisionQuery::default(), None, 2)
            .unwrap();
        assert_eq!(decision_page.items.len(), 2);
        assert_eq!(decision_page.items[0].decision_id, "decision-2");
        assert!(decision_page.next_cursor.is_some());
    }
}
