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

/// 매수 지정가: 매도1호가 + 버퍼틱, 호가단위에 맞춰 내림 정렬
pub fn buy_limit_price(ask1: u64, buffer_ticks: u32, etf: bool) -> u64 {
    let mut price = ask1;
    for _ in 0..buffer_ticks {
        price += tick_size(price, etf);
    }
    let tick = tick_size(price, etf);
    price - (price % tick)
}

/// 예수금으로 살 수 있는 최대 수량 (수수료 여유분 0.1% 차감)
pub fn max_buy_qty(cash: u64, limit_price: u64) -> u64 {
    if limit_price == 0 {
        return 0;
    }
    ((cash as f64) * 0.999 / (limit_price as f64)).floor() as u64
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
    fn buy_limit_adds_buffer_ticks() {
        // ETF 12,000원 + 2틱 = 12,010
        assert_eq!(buy_limit_price(12_000, 2, true), 12_010);
        // 버퍼 0이면 그대로
        assert_eq!(buy_limit_price(12_000, 0, true), 12_000);
    }

    #[test]
    fn buy_limit_aligns_to_tick() {
        // 호가단위에 어긋난 입력도 내림 정렬된다
        assert_eq!(buy_limit_price(12_003, 0, true), 12_000);
    }

    #[test]
    fn max_qty_reserves_fee_margin() {
        // 1,000,000원 / 12,010원 = 83.26 → 83주보다 작거나 같아야 함
        let qty = max_buy_qty(1_000_000, 12_010);
        assert!(qty <= 83);
        assert!(qty >= 82);
        assert_eq!(max_buy_qty(0, 12_010), 0);
        assert_eq!(max_buy_qty(1_000_000, 0), 0);
    }

    #[test]
    fn kst_str_parses() {
        let e = kst_str_to_fake_epoch("20260716", "093000").unwrap();
        // 2026-07-16 09:30:00 (벽시계) — 요일/시간 재변환 검증
        let dt = chrono::DateTime::from_timestamp(e, 0).unwrap().naive_utc();
        assert_eq!(dt.format("%Y%m%d %H%M%S").to_string(), "20260716 093000");
    }
}
