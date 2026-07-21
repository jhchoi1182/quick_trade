use chrono::{Duration, NaiveDate, NaiveDateTime, Utc};

/// KST 벽시계 시각 (한국은 서머타임이 없으므로 고정 +9h)
pub fn now_kst() -> NaiveDateTime {
    (Utc::now() + Duration::hours(9)).naive_utc()
}

/// KST 벽시계를 UTC epoch처럼 취급한 "가짜 epoch" 초 (Candle.time / Quote.ts 규약)
pub fn now_kst_fake_epoch() -> i64 {
    now_kst().and_utc().timestamp()
}

pub fn naive_to_fake_epoch(dt: NaiveDateTime) -> i64 {
    dt.and_utc().timestamp()
}

/// "YYYYMMDD" + "HHMMSS" → 가짜 epoch 초
pub fn kst_str_to_fake_epoch(date: &str, time: &str) -> Option<i64> {
    let d = NaiveDate::parse_from_str(date, "%Y%m%d").ok()?;
    let t = chrono::NaiveTime::parse_from_str(time, "%H%M%S").ok()?;
    Some(naive_to_fake_epoch(d.and_time(t)))
}

/// KRX 호가단위. ETF/ETN은 가격과 무관하게 5원.
pub fn tick_size(price: u64, etf: bool) -> u64 {
    if etf {
        return 5;
    }
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

/// 매수 지정가 프리미엄(%). 급등 중에도 IOC가 즉시 체결되도록 기준 가격보다 높게 잡는다.
pub const BUY_PREMIUM_PCT: u64 = 3;

/// 즉시 매수 시 기준 가격으로 투입할 주문가능현금 비율.
/// 매수가능수량을 다시 조회하지 않고 시드의 약 95%를 즉시 투입한다.
pub const CASH_USE_RATIO: f64 = 0.95;

/// 매수 지정가: 기준 가격 +3%, 호가단위에 맞춰 내림 정렬
pub fn buy_limit_price(base: u64, etf: bool) -> u64 {
    let raw = base + base * BUY_PREMIUM_PCT / 100;
    let tick = tick_size(raw, etf);
    raw - (raw % tick)
}

/// 주문가능현금의 95%로 기준 가격에 살 수 있는 최대 수량
pub fn max_buy_qty(cash: u64, reference_price: u64) -> u64 {
    if reference_price == 0 {
        return 0;
    }
    ((cash as f64) * CASH_USE_RATIO / (reference_price as f64)).floor() as u64
}

/// 예약 매도 목표가: 평단 × (1 + pct/100) "이상"인 첫 호가(올림 정렬).
/// buy_limit_price가 내림인 것과 달리, 입력 수익률 이상을 보장하려고 올림한다.
/// (예: 0.3% 지정 시 평단보다 0.3% 이상인 가장 낮은 호가에 매도 주문을 건다)
pub fn sell_target_price(avg_price: f64, pct: f64, etf: bool) -> u64 {
    if avg_price <= 0.0 {
        return 0;
    }
    let raw = avg_price * (1.0 + pct / 100.0);
    let tick = tick_size(raw as u64, etf);
    let ticks = (raw / tick as f64).ceil() as u64;
    ticks * tick
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_size_etf_is_always_5() {
        assert_eq!(tick_size(1_000, true), 5);
        assert_eq!(tick_size(123_456, true), 5);
    }

    #[test]
    fn tick_size_stock_bands() {
        assert_eq!(tick_size(1_999, false), 1);
        assert_eq!(tick_size(2_000, false), 5);
        assert_eq!(tick_size(19_999, false), 10);
        assert_eq!(tick_size(50_000, false), 100);
        assert_eq!(tick_size(200_000, false), 500);
        assert_eq!(tick_size(700_000, false), 1_000);
    }

    #[test]
    fn buy_limit_adds_premium() {
        // ETF 12,000원 × 1.03 = 12,360 (5원 배수라 그대로)
        assert_eq!(buy_limit_price(12_000, true), 12_360);
        // 10,001 × 1.03 = 10,301 → 5원 단위 내림 = 10,300
        assert_eq!(buy_limit_price(10_001, true), 10_300);
    }

    #[test]
    fn buy_limit_aligns_to_tick() {
        // 비ETF: 프리미엄 결과가 해당 가격대 호가단위로 내림 정렬된다
        // 21,000 × 1.03 = 21,630 → 50원 단위 내림 = 21,600
        assert_eq!(buy_limit_price(21_000, false), 21_600);
    }

    #[test]
    fn max_qty_uses_95_percent() {
        // 1,000,000원 × 0.95 / 12,010원 = 79.10 → 79주
        assert_eq!(max_buy_qty(1_000_000, 12_010), 79);
        // 실계좌 재현값: 95% 투입 수량은 KIS가 허용한 4,179주보다 작다.
        assert_eq!(max_buy_qty(63_956_290, 14_710), 4_130);
        assert_eq!(max_buy_qty(0, 12_010), 0);
        assert_eq!(max_buy_qty(1_000_000, 0), 0);
    }

    #[test]
    fn sell_target_ceils_to_first_tick_at_or_above() {
        // ETF 평단 10,000원 +0.3% = 10,030 (5원 배수라 그대로)
        assert_eq!(sell_target_price(10_000.0, 0.3, true), 10_030);
        // 평단 10,001원 +0.3% = 10,031.003 → 5원 올림 = 10,035
        assert_eq!(sell_target_price(10_001.0, 0.3, true), 10_035);
        // 딱 맞는 경우: 12,000원 +0.5% = 12,060 (5원 배수) 그대로
        assert_eq!(sell_target_price(12_000.0, 0.5, true), 12_060);
        // pct 0 → 평단 이상 첫 호가 = 평단(호가배수면 그대로)
        assert_eq!(sell_target_price(10_000.0, 0.0, true), 10_000);
        // 비ETF 호가단위(50원) 적용: 20,000원 +0.2% = 20,040 → 50원 올림 = 20,050
        assert_eq!(sell_target_price(20_000.0, 0.2, false), 20_050);
        // 평단 0 방어
        assert_eq!(sell_target_price(0.0, 0.3, true), 0);
    }

    #[test]
    fn kst_str_parses() {
        let e = kst_str_to_fake_epoch("20260716", "093000").unwrap();
        // 2026-07-16 09:30:00 (벽시계) — 요일/시간 재변환 검증
        let dt = chrono::DateTime::from_timestamp(e, 0).unwrap().naive_utc();
        assert_eq!(dt.format("%Y%m%d %H%M%S").to_string(), "20260716 093000");
    }
}
