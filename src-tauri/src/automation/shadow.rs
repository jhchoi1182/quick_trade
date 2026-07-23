//! 실계좌와 완전히 분리된 섀도 체결 시뮬레이터.
//!
//! 네트워크·브로커·KIS 객체를 받지 않고 호출자가 주입한 ETF 체결 틱과 1호가만
//! 소비한다. 따라서 이 모듈의 어떤 상태 전이도 외부 주문을 발생시킬 수 없다.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::broker::ShadowCashSource;
use crate::util::AUTO_PROFIT_GUARD_PCT;

/// 진입에 사용하는 가상현금 비율(95%).
pub const CASH_USE_BPS: u64 = 9_500;
pub const BPS_DENOMINATOR: u64 = 10_000;
/// 자동·섀도 포지션의 단순 수익률 손절선.
pub const STOP_LOSS_PCT: f64 = -0.3;
/// 첫 체결부터 허용하는 최대 보유시간.
pub const MAX_HOLD_SECONDS: i64 = 600;
/// 호가가 유효하다고 보는 최대 나이.
pub const BOOK_FRESH_SECONDS: i64 = 10;
/// KST 가짜 epoch의 하루 중 15:15:00.
pub const MARKET_CLOSE_SECOND: i64 = 15 * 60 * 60 + 15 * 60;
const ETF_TICK_SIZE: u64 = 5;
const TARGET_MIN_PCT: f64 = 0.2;
const TARGET_MAX_PCT: f64 = 2.0;
const FLOAT_EPSILON: f64 = 1e-8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowProduct {
    Leverage,
    Inverse,
}

/// 특정 ETF의 1호가 스냅샷. `sequence`는 상품별로 단조 증가해야 한다.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShadowBook {
    pub product: ShadowProduct,
    pub sequence: u64,
    pub ask_price: u64,
    pub ask_qty: u64,
    pub bid_price: u64,
    pub bid_qty: u64,
    /// KST 가짜 epoch 초.
    pub observed_at: i64,
}

impl ShadowBook {
    pub fn is_fresh_at(&self, now: i64) -> bool {
        now >= self.observed_at && now - self.observed_at <= BOOK_FRESH_SECONDS
    }
}

/// 실제 ETF 체결 틱. 목표 지정가의 가상 체결량은 이 거래량을 넘지 않는다.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShadowTradeTick {
    pub product: ShadowProduct,
    pub sequence: u64,
    pub price: u64,
    pub volume: u64,
    /// KST 가짜 epoch 초.
    pub at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitReason {
    TargetReached,
    ModeExit,
    MaxHolding,
    ProfitGuard,
    StopLoss,
    MarketClose,
}

impl ExitReason {
    fn priority(self) -> u8 {
        match self {
            Self::TargetReached => 0,
            Self::ModeExit => 1,
            Self::MaxHolding => 2,
            Self::ProfitGuard => 3,
            Self::StopLoss => 4,
            Self::MarketClose => 5,
        }
    }

    fn is_forced(self) -> bool {
        self != Self::TargetReached
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowOrderKind {
    EntryIoc,
    Target,
    ForcedExit(ExitReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowOrderStatus {
    Pending,
    PartiallyFilled,
    Filled,
    Cancelled,
    PartiallyFilledCancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShadowOrder {
    pub id: u64,
    pub product: ShadowProduct,
    pub code: String,
    pub side: ShadowSide,
    pub kind: ShadowOrderKind,
    pub requested_qty: u64,
    pub filled_qty: u64,
    pub avg_fill_price: Option<f64>,
    pub limit_price: Option<u64>,
    pub status: ShadowOrderStatus,
    pub created_at: i64,
    pub updated_at: i64,
}

impl ShadowOrder {
    fn new(
        id: u64,
        product: ShadowProduct,
        code: String,
        side: ShadowSide,
        kind: ShadowOrderKind,
        requested_qty: u64,
        limit_price: Option<u64>,
        at: i64,
    ) -> Self {
        Self {
            id,
            product,
            code,
            side,
            kind,
            requested_qty,
            filled_qty: 0,
            avg_fill_price: None,
            limit_price,
            status: ShadowOrderStatus::Pending,
            created_at: at,
            updated_at: at,
        }
    }

    fn record_fill(&mut self, qty: u64, price: u64, at: i64) {
        if qty == 0 {
            return;
        }
        let previous_value = self.avg_fill_price.unwrap_or_default() * self.filled_qty as f64;
        self.filled_qty += qty;
        self.avg_fill_price =
            Some((previous_value + price as f64 * qty as f64) / self.filled_qty as f64);
        self.status = if self.filled_qty >= self.requested_qty {
            ShadowOrderStatus::Filled
        } else {
            ShadowOrderStatus::PartiallyFilled
        };
        self.updated_at = at;
    }

    fn cancel_remainder(&mut self, at: i64) {
        self.status = if self.filled_qty == 0 {
            ShadowOrderStatus::Cancelled
        } else if self.filled_qty < self.requested_qty {
            ShadowOrderStatus::PartiallyFilledCancelled
        } else {
            ShadowOrderStatus::Filled
        };
        self.updated_at = at;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShadowPosition {
    pub product: ShadowProduct,
    pub code: String,
    pub entry_qty: u64,
    pub qty: u64,
    pub entry_vwap: f64,
    pub target_return_pct: f64,
    pub target_price: u64,
    pub first_fill_time: i64,
    pub last_price: u64,
    #[serde(default)]
    pub profit_guard_armed: bool,
}

impl ShadowPosition {
    /// 현재 화면과 같은 단순 수익률 `(현재가 / 체결평단 - 1) * 100`.
    pub fn simple_return_pct(&self) -> f64 {
        simple_return_pct(self.entry_vwap, self.last_price)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShadowError {
    PositionAlreadyOpen,
    ProductMismatch,
    InvalidBook,
    StaleBook,
    InvalidTargetReturn,
    NoAffordableQuantity,
    ArithmeticOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryOutcome {
    pub order_id: u64,
    pub requested_qty: u64,
    pub filled_qty: u64,
    pub fill_price: Option<u64>,
    pub target_order_id: Option<u64>,
    pub target_price: Option<u64>,
}

/// 주문 메서드가 없는 읽기 전용 capability만 소유하는 섀도 진입 실행기.
/// 실계좌 현금 복사와 순수 가상 체결 계산 외의 외부 효과를 만들 수 없다.
#[derive(Clone)]
pub struct ShadowEntryExecutor {
    cash_source: Arc<dyn ShadowCashSource>,
}

pub struct ShadowEntryRequest {
    pub existing_session: Option<ShadowSession>,
    pub copy_real_cash: bool,
    pub fallback_cash: u64,
    pub product: ShadowProduct,
    pub code: String,
    pub target_return_pct: f64,
    pub now: i64,
    pub book: ShadowBook,
}

pub struct PreparedShadowEntry {
    pub session: ShadowSession,
    pub outcome: EntryOutcome,
    /// 이번 진입에서 실계좌로부터 새로 복사한 주문가능현금.
    pub copied_cash: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum ShadowExecutionError {
    #[error("주문가능현금 조회 실패: {0}")]
    Cash(String),
    #[error("가상 체결 실패: {0:?}")]
    Simulation(ShadowError),
}

impl ShadowEntryExecutor {
    pub fn new(cash_source: Arc<dyn ShadowCashSource>) -> Self {
        Self { cash_source }
    }

    pub async fn prepare(
        &self,
        request: ShadowEntryRequest,
    ) -> Result<PreparedShadowEntry, ShadowExecutionError> {
        let copied_cash = if request.copy_real_cash {
            Some(
                self.cash_source
                    .orderable_cash(&request.code)
                    .await
                    .map_err(|error| ShadowExecutionError::Cash(error.to_string()))?,
            )
        } else {
            None
        };
        let mut session = if let Some(cash) = copied_cash {
            ShadowSession::start(cash)
        } else {
            request
                .existing_session
                .unwrap_or_else(|| ShadowSession::start(request.fallback_cash))
        };
        let outcome = session
            .try_enter(
                request.product,
                request.code,
                request.target_return_pct,
                request.now,
                request.book,
            )
            .map_err(ShadowExecutionError::Simulation)?;
        Ok(PreparedShadowEntry {
            session,
            outcome,
            copied_cash,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowFill {
    pub order_id: u64,
    pub product: ShadowProduct,
    pub side: ShadowSide,
    pub qty: u64,
    pub price: u64,
    pub at: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShadowUpdate {
    /// 중복 순번이 아닌 새로운 호가/체결을 소비했는지 여부.
    pub accepted: bool,
    pub fill: Option<ShadowFill>,
    pub exit_reason: Option<ExitReason>,
    pub position_closed: bool,
}

/// 한 섀도 세션의 가상현금·포지션·주문을 소유하는 순수 상태 객체.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShadowSession {
    starting_cash: u64,
    cash: u64,
    position: Option<ShadowPosition>,
    orders: Vec<ShadowOrder>,
    next_order_id: u64,
    target_order_id: Option<u64>,
    forced_order_id: Option<u64>,
    active_exit_reason: Option<ExitReason>,
    last_book: Option<ShadowBook>,
    last_exit_book_sequence: Option<u64>,
    last_trade_sequence: Option<u64>,
}

impl ShadowSession {
    /// 실계좌의 주문가능현금을 값으로 한 번 복사한다. 이후 실계좌 변화와 무관하다.
    pub fn start(real_orderable_cash: u64) -> Self {
        Self {
            starting_cash: real_orderable_cash,
            cash: real_orderable_cash,
            position: None,
            orders: Vec::new(),
            next_order_id: 1,
            target_order_id: None,
            forced_order_id: None,
            active_exit_reason: None,
            last_book: None,
            last_exit_book_sequence: None,
            last_trade_sequence: None,
        }
    }

    /// 앱 재시작 때 영속화된 섀도 포지션과 가상현금을 같은 세션으로 복원한다.
    pub fn restore(starting_cash: u64, cash: u64, position: ShadowPosition) -> Self {
        let mut target = ShadowOrder::new(
            1,
            position.product,
            position.code.clone(),
            ShadowSide::Sell,
            ShadowOrderKind::Target,
            position.qty,
            Some(position.target_price),
            position.first_fill_time,
        );
        target.status = ShadowOrderStatus::Pending;
        Self {
            starting_cash,
            cash,
            position: Some(position),
            orders: vec![target],
            next_order_id: 2,
            target_order_id: Some(1),
            forced_order_id: None,
            active_exit_reason: None,
            last_book: None,
            last_exit_book_sequence: None,
            last_trade_sequence: None,
        }
    }

    #[cfg(test)]
    pub fn starting_cash(&self) -> u64 {
        self.starting_cash
    }

    pub fn cash(&self) -> u64 {
        self.cash
    }

    pub fn position(&self) -> Option<&ShadowPosition> {
        self.position.as_ref()
    }

    pub fn orders(&self) -> &[ShadowOrder] {
        &self.orders
    }

    #[cfg(test)]
    pub fn first_fill_time(&self) -> Option<i64> {
        self.position
            .as_ref()
            .map(|position| position.first_fill_time)
    }

    #[cfg(test)]
    pub fn simple_return_pct(&self) -> Option<f64> {
        self.position
            .as_ref()
            .map(ShadowPosition::simple_return_pct)
    }

    pub fn pending_exit_reason(&self) -> Option<ExitReason> {
        self.active_exit_reason
    }

    /// 이미 체결된 모든 가상 매도의 수량과 금액을 반환한다.
    pub fn exit_summary(&self) -> (u64, f64) {
        self.orders
            .iter()
            .filter(|order| order.side == ShadowSide::Sell && order.filled_qty > 0)
            .fold((0_u64, 0.0_f64), |(qty, value), order| {
                (
                    qty.saturating_add(order.filled_qty),
                    value + order.avg_fill_price.unwrap_or_default() * order.filled_qty as f64,
                )
            })
    }

    /// 재시작 뒤 새 WebSocket 순번 공간과 실제 새 호가만 소비하도록 피드 임시값을 비운다.
    /// 포지션·현금·주문·부분체결·강제청산 사유는 그대로 보존한다.
    pub fn prepare_for_restart(&mut self) {
        self.last_book = None;
        self.last_exit_book_sequence = None;
        self.last_trade_sequence = None;
    }

    /// 선택 ETF의 현재 매도1호가 한 단계에서만 IOC 진입을 모사한다.
    /// 주문가능 가상현금의 95% 수량보다 호가수량이 적으면 그만큼만 체결하고 끝낸다.
    pub fn try_enter(
        &mut self,
        product: ShadowProduct,
        code: impl Into<String>,
        target_return_pct: f64,
        now: i64,
        book: ShadowBook,
    ) -> Result<EntryOutcome, ShadowError> {
        if self.position.is_some() {
            return Err(ShadowError::PositionAlreadyOpen);
        }
        if book.product != product {
            return Err(ShadowError::ProductMismatch);
        }
        if book.ask_price == 0 {
            return Err(ShadowError::InvalidBook);
        }
        if !book.is_fresh_at(now) {
            return Err(ShadowError::StaleBook);
        }
        if !is_valid_target_return(target_return_pct) {
            return Err(ShadowError::InvalidTargetReturn);
        }

        let budget: u64 = ((self.cash as u128) * CASH_USE_BPS as u128 / BPS_DENOMINATOR as u128)
            .try_into()
            .map_err(|_| ShadowError::ArithmeticOverflow)?;
        let requested_qty = budget / book.ask_price;
        if requested_qty == 0 {
            return Err(ShadowError::NoAffordableQuantity);
        }

        let code = code.into();
        let filled_qty = requested_qty.min(book.ask_qty);
        let entry_order_id = self.allocate_order_id()?;
        let mut entry_order = ShadowOrder::new(
            entry_order_id,
            product,
            code.clone(),
            ShadowSide::Buy,
            ShadowOrderKind::EntryIoc,
            requested_qty,
            Some(book.ask_price),
            now,
        );
        entry_order.record_fill(filled_qty, book.ask_price, now);
        entry_order.cancel_remainder(now);
        self.orders.push(entry_order);
        self.last_book = Some(book);

        if filled_qty == 0 {
            return Ok(EntryOutcome {
                order_id: entry_order_id,
                requested_qty,
                filled_qty: 0,
                fill_price: None,
                target_order_id: None,
                target_price: None,
            });
        }

        let cost = checked_value(book.ask_price, filled_qty)?;
        self.cash = self
            .cash
            .checked_sub(cost)
            .ok_or(ShadowError::ArithmeticOverflow)?;
        let target_price = target_price_for(book.ask_price as f64, target_return_pct)?;
        let target_order_id = self.allocate_order_id()?;
        self.orders.push(ShadowOrder::new(
            target_order_id,
            product,
            code.clone(),
            ShadowSide::Sell,
            ShadowOrderKind::Target,
            filled_qty,
            Some(target_price),
            now,
        ));
        self.position = Some(ShadowPosition {
            product,
            code,
            entry_qty: filled_qty,
            qty: filled_qty,
            entry_vwap: book.ask_price as f64,
            target_return_pct,
            target_price,
            first_fill_time: now,
            last_price: book.ask_price,
            profit_guard_armed: false,
        });
        self.target_order_id = Some(target_order_id);
        self.forced_order_id = None;
        self.active_exit_reason = None;
        self.last_exit_book_sequence = None;
        self.last_trade_sequence = None;

        Ok(EntryOutcome {
            order_id: entry_order_id,
            requested_qty,
            filled_qty,
            fill_price: Some(book.ask_price),
            target_order_id: Some(target_order_id),
            target_price: Some(target_price),
        })
    }

    /// 실제 ETF 체결 틱으로 현재 수익률과 목표 주문을 갱신한다.
    /// 동일 순번 틱의 거래량은 두 번 사용할 수 없다.
    pub fn on_trade_tick(&mut self, tick: ShadowTradeTick) -> Result<ShadowUpdate, ShadowError> {
        let Some(position) = self.position.as_ref() else {
            return Ok(ShadowUpdate::default());
        };
        if position.product != tick.product || tick.price == 0 || tick.volume == 0 {
            return Ok(ShadowUpdate::default());
        }
        if self
            .last_trade_sequence
            .is_some_and(|previous| tick.sequence <= previous)
        {
            return Ok(ShadowUpdate::default());
        }
        self.last_trade_sequence = Some(tick.sequence);
        self.position
            .as_mut()
            .expect("위에서 확인한 포지션")
            .last_price = tick.price;
        if let Some(position) = self.position.as_mut() {
            if position.target_return_pct > AUTO_PROFIT_GUARD_PCT
                && position.simple_return_pct() > AUTO_PROFIT_GUARD_PCT
            {
                position.profit_guard_armed = true;
            }
        }

        if let Some(reason) = self.select_forced_reason(tick.at, false) {
            self.begin_forced_exit(reason, tick.at)?;
            let mut update = self.drive_forced_exit(tick.at)?;
            update.accepted = true;
            return Ok(update);
        }

        let target_price = self.position.as_ref().expect("포지션 유지").target_price;
        if tick.price < target_price {
            return Ok(ShadowUpdate {
                accepted: true,
                ..ShadowUpdate::default()
            });
        }

        self.fill_target(tick)
    }

    /// 새 1호가를 저장하고, 이미 강제청산 중이거나 시간 조건이 도달했다면
    /// 이 호가의 매수1호가 수량까지만 매도한다.
    pub fn on_book(&mut self, book: ShadowBook) -> Result<ShadowUpdate, ShadowError> {
        let Some(position) = self.position.as_ref() else {
            self.last_book = Some(book);
            return Ok(ShadowUpdate {
                accepted: true,
                ..ShadowUpdate::default()
            });
        };
        if position.product != book.product {
            return Ok(ShadowUpdate::default());
        }
        if self.last_book.is_some_and(|previous| {
            previous.product == book.product && book.sequence <= previous.sequence
        }) {
            return Ok(ShadowUpdate::default());
        }
        self.last_book = Some(book);
        let mut update = self.evaluate_forced_exit(book.observed_at, false)?;
        update.accepted = true;
        Ok(update)
    }

    /// 타이머에서 호출해 -0.3%, 600초, 15:15 조건을 확인한다.
    pub fn poll(&mut self, now: i64) -> Result<ShadowUpdate, ShadowError> {
        self.evaluate_forced_exit(now, false)
    }

    /// Shadow 이탈 시 호출한다. 더 높은 우선순위 조건이 동시에 충족되면 그 사유를 보존한다.
    pub fn request_mode_exit(&mut self, now: i64) -> Result<ShadowUpdate, ShadowError> {
        self.evaluate_forced_exit(now, true)
    }

    fn evaluate_forced_exit(
        &mut self,
        now: i64,
        mode_exit: bool,
    ) -> Result<ShadowUpdate, ShadowError> {
        if self.position.is_none() {
            return Ok(ShadowUpdate::default());
        }
        if let Some(reason) = self.select_forced_reason(now, mode_exit) {
            self.begin_forced_exit(reason, now)?;
        }
        self.drive_forced_exit(now)
    }

    fn select_forced_reason(&self, now: i64, mode_exit: bool) -> Option<ExitReason> {
        let position = self.position.as_ref()?;
        let candidate = if has_reached_market_close(position.first_fill_time, now) {
            Some(ExitReason::MarketClose)
        } else if position.simple_return_pct() <= STOP_LOSS_PCT + FLOAT_EPSILON {
            Some(ExitReason::StopLoss)
        } else if position.profit_guard_armed
            && position.simple_return_pct() <= AUTO_PROFIT_GUARD_PCT + FLOAT_EPSILON
        {
            Some(ExitReason::ProfitGuard)
        } else if now >= position.first_fill_time
            && now - position.first_fill_time >= MAX_HOLD_SECONDS
        {
            Some(ExitReason::MaxHolding)
        } else if mode_exit {
            Some(ExitReason::ModeExit)
        } else {
            None
        };

        match (self.active_exit_reason, candidate) {
            (Some(active), Some(next)) if active.priority() >= next.priority() => Some(active),
            (_, next) => next.or(self.active_exit_reason),
        }
    }

    fn begin_forced_exit(&mut self, reason: ExitReason, now: i64) -> Result<(), ShadowError> {
        debug_assert!(reason.is_forced());
        let Some(position) = self.position.as_ref() else {
            return Ok(());
        };
        if self
            .active_exit_reason
            .is_some_and(|active| active.priority() >= reason.priority())
        {
            return Ok(());
        }

        let product = position.product;
        let code = position.code.clone();
        let qty = position.qty;
        self.active_exit_reason = Some(reason);
        if let Some(target_id) = self.target_order_id.take() {
            if let Some(order) = self.order_mut(target_id) {
                order.cancel_remainder(now);
            }
        }

        if let Some(order_id) = self.forced_order_id {
            if let Some(order) = self.order_mut(order_id) {
                order.kind = ShadowOrderKind::ForcedExit(reason);
                order.updated_at = now;
            }
            return Ok(());
        }

        let order_id = self.allocate_order_id()?;
        self.orders.push(ShadowOrder::new(
            order_id,
            product,
            code,
            ShadowSide::Sell,
            ShadowOrderKind::ForcedExit(reason),
            qty,
            None,
            now,
        ));
        self.forced_order_id = Some(order_id);
        Ok(())
    }

    fn drive_forced_exit(&mut self, now: i64) -> Result<ShadowUpdate, ShadowError> {
        let Some(reason) = self.active_exit_reason else {
            return Ok(ShadowUpdate::default());
        };
        let mut update = ShadowUpdate {
            exit_reason: Some(reason),
            ..ShadowUpdate::default()
        };
        let Some(book) = self.last_book else {
            return Ok(update);
        };
        if !book.is_fresh_at(now)
            || book.bid_price == 0
            || book.bid_qty == 0
            || self.last_exit_book_sequence == Some(book.sequence)
        {
            return Ok(update);
        }
        self.last_exit_book_sequence = Some(book.sequence);
        let remaining = self
            .position
            .as_ref()
            .map(|position| position.qty)
            .unwrap_or(0);
        let fill_qty = remaining.min(book.bid_qty);
        if fill_qty == 0 {
            return Ok(update);
        }
        let order_id = self.forced_order_id.expect("강제청산 주문이 먼저 생성된다");
        self.apply_exit_fill(order_id, fill_qty, book.bid_price, now)?;
        update.fill = Some(ShadowFill {
            order_id,
            product: book.product,
            side: ShadowSide::Sell,
            qty: fill_qty,
            price: book.bid_price,
            at: now,
        });
        if self
            .position
            .as_ref()
            .is_some_and(|position| position.qty == 0)
        {
            self.position = None;
            self.forced_order_id = None;
            self.active_exit_reason = None;
            update.position_closed = true;
        }
        Ok(update)
    }

    fn fill_target(&mut self, tick: ShadowTradeTick) -> Result<ShadowUpdate, ShadowError> {
        let position = self.position.as_ref().expect("목표 체결 전 포지션 존재");
        let fill_qty = position.qty.min(tick.volume);
        let target_price = position.target_price;
        let order_id = self
            .target_order_id
            .expect("포지션과 목표 주문은 함께 생성된다");
        self.apply_exit_fill(order_id, fill_qty, target_price, tick.at)?;
        let closed = self
            .position
            .as_ref()
            .is_some_and(|position| position.qty == 0);
        if closed {
            self.position = None;
            self.target_order_id = None;
        }
        Ok(ShadowUpdate {
            accepted: true,
            fill: Some(ShadowFill {
                order_id,
                product: tick.product,
                side: ShadowSide::Sell,
                qty: fill_qty,
                price: target_price,
                at: tick.at,
            }),
            exit_reason: Some(ExitReason::TargetReached),
            position_closed: closed,
        })
    }

    fn apply_exit_fill(
        &mut self,
        order_id: u64,
        qty: u64,
        price: u64,
        at: i64,
    ) -> Result<(), ShadowError> {
        let credit = checked_value(price, qty)?;
        self.cash = self
            .cash
            .checked_add(credit)
            .ok_or(ShadowError::ArithmeticOverflow)?;
        if let Some(position) = self.position.as_mut() {
            position.qty = position
                .qty
                .checked_sub(qty)
                .ok_or(ShadowError::ArithmeticOverflow)?;
            position.last_price = price;
        }
        self.order_mut(order_id)
            .expect("내부 주문 ID는 항상 존재한다")
            .record_fill(qty, price, at);
        Ok(())
    }

    fn allocate_order_id(&mut self) -> Result<u64, ShadowError> {
        let id = self.next_order_id;
        self.next_order_id = self
            .next_order_id
            .checked_add(1)
            .ok_or(ShadowError::ArithmeticOverflow)?;
        Ok(id)
    }

    fn order_mut(&mut self, id: u64) -> Option<&mut ShadowOrder> {
        self.orders.iter_mut().find(|order| order.id == id)
    }
}

/// ETF 체결평단에 목표율을 한 번만 직접 적용하고 5원 호가로 올린다.
pub fn target_price_for(entry_vwap: f64, target_return_pct: f64) -> Result<u64, ShadowError> {
    if !entry_vwap.is_finite() || entry_vwap <= 0.0 {
        return Err(ShadowError::InvalidBook);
    }
    if !is_valid_target_return(target_return_pct) {
        return Err(ShadowError::InvalidTargetReturn);
    }
    let raw = entry_vwap * (1.0 + target_return_pct / 100.0);
    let rounded = (raw / ETF_TICK_SIZE as f64).ceil() * ETF_TICK_SIZE as f64;
    if !rounded.is_finite() || rounded > u64::MAX as f64 {
        return Err(ShadowError::ArithmeticOverflow);
    }
    Ok(rounded as u64)
}

pub fn simple_return_pct(entry_vwap: f64, current_price: u64) -> f64 {
    (current_price as f64 / entry_vwap - 1.0) * 100.0
}

fn is_valid_target_return(value: f64) -> bool {
    value.is_finite()
        && (TARGET_MIN_PCT..=TARGET_MAX_PCT).contains(&value)
        && ((value * 10.0) - (value * 10.0).round()).abs() <= FLOAT_EPSILON
}

fn checked_value(price: u64, qty: u64) -> Result<u64, ShadowError> {
    price
        .checked_mul(qty)
        .ok_or(ShadowError::ArithmeticOverflow)
}

fn has_reached_market_close(first_fill_time: i64, now: i64) -> bool {
    let entry_close = first_fill_time.div_euclid(24 * 60 * 60) * 24 * 60 * 60 + MARKET_CLOSE_SECOND;
    now >= entry_close
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 20_000 * 24 * 60 * 60;

    fn at(hour: i64, minute: i64, second: i64) -> i64 {
        DAY + hour * 60 * 60 + minute * 60 + second
    }

    fn book(
        sequence: u64,
        ask_price: u64,
        ask_qty: u64,
        bid_price: u64,
        bid_qty: u64,
        observed_at: i64,
    ) -> ShadowBook {
        ShadowBook {
            product: ShadowProduct::Leverage,
            sequence,
            ask_price,
            ask_qty,
            bid_price,
            bid_qty,
            observed_at,
        }
    }

    fn enter(
        session: &mut ShadowSession,
        now: i64,
        ask_price: u64,
        ask_qty: u64,
        bid_price: u64,
        bid_qty: u64,
        target: f64,
    ) -> EntryOutcome {
        session
            .try_enter(
                ShadowProduct::Leverage,
                "0193T0",
                target,
                now,
                book(1, ask_price, ask_qty, bid_price, bid_qty, now),
            )
            .unwrap()
    }

    struct FixedShadowCash;

    #[async_trait::async_trait]
    impl ShadowCashSource for FixedShadowCash {
        async fn orderable_cash(&self, _code: &str) -> crate::error::AppResult<u64> {
            Ok(1_000_000)
        }
    }

    #[tokio::test]
    async fn 읽기전용_executor가_현금을_복사해_가상진입만_수행한다() {
        let now = at(10, 0, 0);
        let executor = ShadowEntryExecutor::new(Arc::new(FixedShadowCash));
        let prepared = executor
            .prepare(ShadowEntryRequest {
                existing_session: None,
                copy_real_cash: true,
                fallback_cash: 0,
                product: ShadowProduct::Leverage,
                code: "0193T0".into(),
                target_return_pct: 0.3,
                now,
                book: book(1, 10_000, 10, 9_995, 10, now),
            })
            .await
            .unwrap();

        assert_eq!(prepared.copied_cash, Some(1_000_000));
        assert_eq!(prepared.outcome.filled_qty, 10);
        assert_eq!(prepared.session.position().unwrap().qty, 10);
    }

    #[test]
    fn 실계좌_현금은_복사되고_매도일호가_수량만큼만_부분진입한다() {
        let now = at(10, 0, 0);
        let mut session = ShadowSession::start(1_000_000);
        let outcome = enter(&mut session, now, 10_000, 30, 9_995, 50, 0.3);

        assert_eq!(session.starting_cash(), 1_000_000);
        assert_eq!(outcome.requested_qty, 95);
        assert_eq!(outcome.filled_qty, 30);
        assert_eq!(session.cash(), 700_000);
        assert_eq!(session.first_fill_time(), Some(now));
        let position = session.position().unwrap();
        assert_eq!(position.qty, 30);
        assert_eq!(position.entry_vwap, 10_000.0);
        assert_eq!(position.target_price, 10_030);
        assert_eq!(session.orders().len(), 2);
        assert_eq!(
            session.orders()[0].status,
            ShadowOrderStatus::PartiallyFilledCancelled
        );
        assert_eq!(session.orders()[1].kind, ShadowOrderKind::Target);
    }

    #[test]
    fn 목표율은_etf_vwap에_직접_한번만_적용하고_실제틱_거래량으로_나눠_체결한다() {
        let now = at(10, 0, 0);
        let mut session = ShadowSession::start(1_000_000);
        let entry = enter(&mut session, now, 10_005, 10, 10_000, 20, 0.3);
        assert_eq!(entry.target_price, Some(10_040));

        let first = session
            .on_trade_tick(ShadowTradeTick {
                product: ShadowProduct::Leverage,
                sequence: 1,
                price: 10_040,
                volume: 3,
                at: now + 1,
            })
            .unwrap();
        assert_eq!(first.fill.unwrap().qty, 3);
        assert_eq!(session.position().unwrap().qty, 7);
        assert!(session.simple_return_pct().unwrap() > 0.3);

        // 같은 체결 틱의 거래량은 다시 사용할 수 없다.
        let duplicate = session
            .on_trade_tick(ShadowTradeTick {
                product: ShadowProduct::Leverage,
                sequence: 1,
                price: 10_050,
                volume: 100,
                at: now + 2,
            })
            .unwrap();
        assert!(!duplicate.accepted);
        assert_eq!(session.position().unwrap().qty, 7);

        let final_fill = session
            .on_trade_tick(ShadowTradeTick {
                product: ShadowProduct::Leverage,
                sequence: 2,
                price: 10_045,
                volume: 100,
                at: now + 3,
            })
            .unwrap();
        assert!(final_fill.position_closed);
        assert_eq!(final_fill.exit_reason, Some(ExitReason::TargetReached));
        assert!(session.position().is_none());
        assert_eq!(session.orders()[1].status, ShadowOrderStatus::Filled);
    }

    #[test]
    fn 강제청산은_신선한_매수일호가_수량만큼_체결하고_새호가에서_계속한다() {
        let now = at(10, 0, 0);
        let mut session = ShadowSession::start(1_000_000);
        enter(&mut session, now, 10_000, 10, 9_995, 2, 0.3);

        let first = session.request_mode_exit(now + 1).unwrap();
        assert_eq!(first.exit_reason, Some(ExitReason::ModeExit));
        assert_eq!(first.fill.unwrap().qty, 2);
        assert_eq!(session.position().unwrap().qty, 8);
        assert_eq!(session.orders()[1].status, ShadowOrderStatus::Cancelled);

        // 같은 호가수량을 poll로 다시 소비하지 않는다.
        assert!(session.poll(now + 2).unwrap().fill.is_none());
        assert_eq!(session.position().unwrap().qty, 8);

        let second = session
            .on_book(book(2, 10_000, 10, 9_990, 3, now + 3))
            .unwrap();
        assert_eq!(second.fill.unwrap().qty, 3);
        assert_eq!(session.position().unwrap().qty, 5);

        let last = session
            .on_book(book(3, 10_000, 10, 9_985, 20, now + 4))
            .unwrap();
        assert!(last.position_closed);
        assert!(session.position().is_none());
        let forced = session.orders().last().unwrap();
        assert_eq!(
            forced.kind,
            ShadowOrderKind::ForcedExit(ExitReason::ModeExit)
        );
        assert_eq!(forced.status, ShadowOrderStatus::Filled);
    }

    #[test]
    fn 손절과_보유시간은_목표보다_우선하고_장마감은_보유시간보다_우선한다() {
        let now = at(15, 4, 58);
        let mut session = ShadowSession::start(1_000_000);
        enter(&mut session, now, 10_000, 10, 9_995, 10, 0.3);

        // 600초 경계에 목표가 틱이 와도 오래된 호가로는 청산하지 않고 목표 주문부터 취소한다.
        let max_hold = session
            .on_trade_tick(ShadowTradeTick {
                product: ShadowProduct::Leverage,
                sequence: 1,
                price: 10_030,
                volume: 10,
                at: now + MAX_HOLD_SECONDS,
            })
            .unwrap();
        assert_eq!(max_hold.exit_reason, Some(ExitReason::MaxHolding));
        assert!(max_hold.fill.is_none());
        assert_eq!(session.position().unwrap().qty, 10);

        // 15:15 새 호가가 오면 기존 최대보유 사유를 장마감으로 올려 전량 정리한다.
        let close = session
            .on_book(book(2, 10_030, 10, 10_020, 10, at(15, 15, 0)))
            .unwrap();
        assert_eq!(close.exit_reason, Some(ExitReason::MarketClose));
        assert!(close.position_closed);
        assert_eq!(
            session.orders().last().unwrap().kind,
            ShadowOrderKind::ForcedExit(ExitReason::MarketClose)
        );

        // 같은 시각에 손절과 최대보유가 겹치면 손절이 우선한다.
        let morning = at(10, 0, 0);
        let mut stop_session = ShadowSession::start(1_000_000);
        enter(&mut stop_session, morning, 10_000, 10, 9_995, 10, 0.3);
        let stopped = stop_session
            .on_trade_tick(ShadowTradeTick {
                product: ShadowProduct::Leverage,
                sequence: 1,
                price: 9_970,
                volume: 10,
                at: morning + MAX_HOLD_SECONDS,
            })
            .unwrap();
        assert_eq!(stopped.exit_reason, Some(ExitReason::StopLoss));
    }

    #[test]
    fn 목표가_03_초과인_섀도포지션은_초과후_되밀림에서_수익을_보호한다() {
        let now = at(10, 0, 0);
        let mut session = ShadowSession::start(1_000_000);
        enter(&mut session, now, 10_000, 10, 9_995, 10, 0.4);

        let armed = session
            .on_trade_tick(ShadowTradeTick {
                product: ShadowProduct::Leverage,
                sequence: 1,
                price: 10_035,
                volume: 10,
                at: now + 1,
            })
            .unwrap();
        assert!(armed.fill.is_none());
        assert!(session.position().unwrap().profit_guard_armed);

        session
            .on_book(book(2, 10_035, 10, 10_030, 10, now + 1))
            .unwrap();
        let protected = session
            .on_trade_tick(ShadowTradeTick {
                product: ShadowProduct::Leverage,
                sequence: 2,
                price: 10_030,
                volume: 10,
                at: now + 2,
            })
            .unwrap();
        assert_eq!(protected.exit_reason, Some(ExitReason::ProfitGuard));
        assert!(protected.position_closed);
        assert_eq!(
            session.orders().last().unwrap().kind,
            ShadowOrderKind::ForcedExit(ExitReason::ProfitGuard)
        );

        let mut point_three = ShadowSession::start(1_000_000);
        enter(&mut point_three, now, 10_000, 10, 9_995, 10, 0.3);
        point_three
            .on_trade_tick(ShadowTradeTick {
                product: ShadowProduct::Leverage,
                sequence: 1,
                price: 10_025,
                volume: 10,
                at: now + 1,
            })
            .unwrap();
        assert!(!point_three.position().unwrap().profit_guard_armed);
    }

    #[test]
    fn 다음날_재시작해도_전일_장마감이_최대보유보다_우선한다() {
        let now = at(14, 59, 0);
        let mut session = ShadowSession::start(1_000_000);
        enter(&mut session, now, 10_000, 10, 9_995, 10, 0.3);

        let next_day = now + 24 * 60 * 60;
        let update = session
            .on_book(book(2, 10_000, 10, 9_990, 10, next_day))
            .unwrap();

        assert_eq!(update.exit_reason, Some(ExitReason::MarketClose));
        assert!(update.position_closed);
    }

    #[test]
    fn 부분청산_세션은_직렬화_복원_후_새호가에서_정확히_이어진다() {
        let now = at(10, 0, 0);
        let mut session = ShadowSession::start(1_000_000);
        enter(&mut session, now, 10_000, 10, 9_995, 2, 0.3);
        let first = session.request_mode_exit(now + 1).unwrap();
        assert_eq!(first.fill.unwrap().qty, 2);
        assert_eq!(session.exit_summary(), (2, 19_990.0));

        let json = serde_json::to_string(&session).unwrap();
        let mut restored: ShadowSession = serde_json::from_str(&json).unwrap();
        restored.prepare_for_restart();

        assert_eq!(restored.cash(), 919_990);
        assert_eq!(restored.position().unwrap().qty, 8);
        assert_eq!(restored.exit_summary(), (2, 19_990.0));
        assert_eq!(restored.pending_exit_reason(), Some(ExitReason::ModeExit));
        assert_eq!(restored.orders(), session.orders());
        // 재시작 전 호가는 다시 쓰지 않고 새 호가가 와야 청산을 잇는다.
        assert!(restored.poll(now + 2).unwrap().fill.is_none());

        let completed = restored
            .on_book(book(1, 10_000, 10, 9_990, 20, now + 2))
            .unwrap();
        assert!(completed.position_closed);
        assert_eq!(restored.exit_summary(), (10, 99_910.0));
        assert_eq!(restored.cash(), 999_910);
    }

    #[test]
    fn 섀도_executor는_읽기전용_capability만_보유한다() {
        let source = include_str!("shadow.rs");
        assert!(source.contains("cash_source: Arc<dyn ShadowCashSource>"));
        let forbidden_order_paths = [
            ["place", "_buy("].concat(),
            ["place", "_sell"].concat(),
            ["cancel", "_order("].concat(),
            ["crate", "::", "kis"].concat(),
        ];
        for path in forbidden_order_paths {
            assert!(!source.contains(&path), "금지된 외부 주문 경로: {path}");
        }
    }
}
