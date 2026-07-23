use chrono::{Datelike, Duration as ChronoDuration, NaiveTime, Weekday};
use serde_json::Value;

use crate::error::AppResult;
use crate::kis::rest::{num_f64, num_u64, KisRest};
use crate::types::{Candle, Quote};
use crate::util::{kst_str_to_fake_epoch, now_kst, now_kst_fake_epoch};

/// 실전 차트는 KRX+NXT 통합을 우선하고, 호출부에서 KRX로 폴백한다.
fn market_div(_rest: &KisRest) -> &'static str {
    "UN"
}

const PATH_PRICE: &str = "/uapi/domestic-stock/v1/quotations/inquire-price";
const PATH_ASKING: &str = "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn";
const PATH_TODAY_MIN: &str = "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice";
const PATH_DAILY_MIN: &str = "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice";

/// MA120(15분봉) 계산에 필요한 최소 1분봉 수 + 여유
const MIN_BARS: usize = 15 * 120 + 120;
/// 과거로 최대 며칠까지 스캔할지 (휴장일 포함 달력일)
const MAX_SCAN_DAYS: usize = 15;
/// 백필 페이지 사이 추가 지연. 리미터(120ms)와 합쳐 페이지당 ~160ms —
/// 주문·계좌 호출 여유는 지키면서 첫 로드가 수십 초씩 걸리지 않게 한다
const BACKFILL_PAGE_DELAY: std::time::Duration = std::time::Duration::from_millis(40);

/// 세션 시간 범위: 통합(UN)은 NXT 프리(08:00)~애프터(20:00), KRX(J)는 09:00~15:30
fn session_bounds(div: &str) -> (NaiveTime, NaiveTime) {
    if div == "UN" {
        (
            NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
            NaiveTime::from_hms_opt(20, 0, 0).unwrap(),
        )
    } else {
        (
            NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            NaiveTime::from_hms_opt(15, 30, 0).unwrap(),
        )
    }
}

/// REST 폴백 스냅샷 — 주문 직전 신선한 시세가 필요할 때만 호출된다.
/// 주문에 필요한 건 1호가라 호가 API를 먼저 부르고, 호가가 비어있을 때(동시호가 등)만
/// 현재가 API로 폴백해 리미터 슬롯 1건을 아낀다 (주문 대기 지연 절감).
pub async fn snapshot(rest: &KisRest, code: &str) -> AppResult<Quote> {
    let params = vec![
        ("FID_COND_MRKT_DIV_CODE", market_div(rest).to_string()),
        ("FID_INPUT_ISCD", code.to_string()),
    ];
    let v = rest.get(PATH_ASKING, "FHKST01010200", &params).await?;
    KisRest::check_rt(&v)?;
    let o1 = &v["output1"];
    let ask1 = num_f64(&o1["askp1"]);
    let bid1 = num_f64(&o1["bidp1"]);
    let ask1_qty = num_u64(&o1["askp_rsqn1"]);
    let bid1_qty = num_u64(&o1["bidp_rsqn1"]);

    // price는 주문 경로에서 ask1이 없을 때의 폴백으로만 쓰인다.
    // UI 시세는 웹소켓이 소스이므로 여기서 ask1로 근사해도 표시에 영향 없음.
    let (price, change_rate) = if ask1 > 0.0 {
        (ask1, 0.0)
    } else {
        let v2 = rest.get(PATH_PRICE, "FHKST01010100", &params).await?;
        KisRest::check_rt(&v2)?;
        let o = &v2["output"];
        (num_f64(&o["stck_prpr"]), num_f64(&o["prdy_ctrt"]))
    };

    let now = now_kst_fake_epoch();
    Ok(Quote {
        code: code.to_string(),
        price,
        change_rate,
        ask1,
        bid1,
        ask1_qty,
        bid1_qty,
        volume: 0.0,
        trade_sequence: 0,
        received_at_micros: crate::util::monotonic_now()
            .as_micros()
            .try_into()
            .unwrap_or(u64::MAX),
        trade_ts: now,
        book_ts: now,
    })
}

fn row_to_candle(row: &Value) -> Option<Candle> {
    let date = row["stck_bsop_date"].as_str()?;
    let hour = row["stck_cntg_hour"].as_str()?;
    Some(Candle {
        time: kst_str_to_fake_epoch(date, hour)?,
        open: num_f64(&row["stck_oprc"]),
        high: num_f64(&row["stck_hgpr"]),
        low: num_f64(&row["stck_lwpr"]),
        close: num_f64(&row["stck_prpr"]),
        volume: num_f64(&row["cntg_vol"]),
    })
}

fn minus_one_minute(hhmmss: &str, session_open: NaiveTime) -> Option<String> {
    let t = NaiveTime::parse_from_str(hhmmss, "%H%M%S").ok()?;
    if t <= session_open {
        return None;
    }
    Some(
        (t - ChronoDuration::minutes(1))
            .format("%H%M%S")
            .to_string(),
    )
}

/// 당일 1분봉 페이징 (커서 시각에서 과거 방향으로 30건씩)
async fn today_minutes(
    rest: &KisRest,
    code: &str,
    div: &str,
    out: &mut Vec<Candle>,
) -> AppResult<()> {
    let now = now_kst();
    let (open, close) = session_bounds(div);
    if now.time() < open || matches!(now.weekday(), Weekday::Sat | Weekday::Sun) {
        return Ok(());
    }
    let mut cursor = now.time().min(close).format("%H%M%S").to_string();
    loop {
        let params = vec![
            ("FID_ETC_CLS_CODE", String::new()),
            ("FID_COND_MRKT_DIV_CODE", div.to_string()),
            ("FID_INPUT_ISCD", code.to_string()),
            ("FID_INPUT_HOUR_1", cursor.clone()),
            ("FID_PW_DATA_INCU_YN", "Y".to_string()),
        ];
        let v = rest.get(PATH_TODAY_MIN, "FHKST03010200", &params).await?;
        KisRest::check_rt(&v)?;
        let rows = match v["output2"].as_array() {
            Some(r) if !r.is_empty() => r.clone(),
            _ => break,
        };
        let mut oldest_hour: Option<String> = None;
        for row in &rows {
            if let Some(c) = row_to_candle(row) {
                out.push(c);
            }
            if let Some(h) = row["stck_cntg_hour"].as_str() {
                let is_older = oldest_hour.as_deref().map(|cur| h < cur).unwrap_or(true);
                if is_older {
                    oldest_hour = Some(h.to_string());
                }
            }
        }
        match oldest_hour
            .as_deref()
            .and_then(|h| minus_one_minute(h, open))
        {
            Some(next) => cursor = next,
            None => break,
        }
        if rows.len() < 30 {
            break; // 마지막 페이지
        }
        tokio::time::sleep(BACKFILL_PAGE_DELAY).await;
    }
    Ok(())
}

/// 과거 일자 1분봉 페이징 (하루 단위)
async fn past_day_minutes(
    rest: &KisRest,
    code: &str,
    date: &str,
    div: &str,
    out: &mut Vec<Candle>,
) -> AppResult<usize> {
    let (open, close) = session_bounds(div);
    let mut fetched = 0usize;
    let mut cursor = close.format("%H%M%S").to_string();
    loop {
        let params = vec![
            ("FID_COND_MRKT_DIV_CODE", div.to_string()),
            ("FID_INPUT_ISCD", code.to_string()),
            ("FID_INPUT_DATE_1", date.to_string()),
            ("FID_INPUT_HOUR_1", cursor.clone()),
            ("FID_PW_DATA_INCU_YN", "Y".to_string()),
            ("FID_FAKE_TICK_INCU_YN", String::new()),
        ];
        let v = rest.get(PATH_DAILY_MIN, "FHKST03010230", &params).await?;
        KisRest::check_rt(&v)?;
        let rows = match v["output2"].as_array() {
            Some(r) if !r.is_empty() => r.clone(),
            _ => break,
        };
        let page_len = rows.len();
        let mut oldest_hour: Option<String> = None;
        for row in &rows {
            if let Some(c) = row_to_candle(row) {
                out.push(c);
                fetched += 1;
            }
            if let Some(h) = row["stck_cntg_hour"].as_str() {
                let is_older = oldest_hour.as_deref().map(|cur| h < cur).unwrap_or(true);
                if is_older {
                    oldest_hour = Some(h.to_string());
                }
            }
        }
        match oldest_hour
            .as_deref()
            .and_then(|h| minus_one_minute(h, open))
        {
            Some(next) => cursor = next,
            None => break,
        }
        // 페이지 크기가 작으면(마지막 페이지) 종료 — 페이지 단위는 문서상 가변이라 보수적으로 5건 미만일 때만
        if page_len < 5 {
            break;
        }
        tokio::time::sleep(BACKFILL_PAGE_DELAY).await;
    }
    Ok(fetched)
}

/// 차트용 1분봉: 당일 + MA120이 가능해질 때까지 과거 영업일 백필
pub async fn candles_1m(rest: &KisRest, code: &str) -> AppResult<Vec<Candle>> {
    let mut all: Vec<Candle> = Vec::new();
    // 당일 분봉도 통합(UN) 지원 여부가 불확실하므로 실패 시 KRX(J)로 폴백
    let mut div = market_div(rest);
    if let Err(e) = today_minutes(rest, code, div, &mut all).await {
        if div != "UN" {
            return Err(e);
        }
        tracing::warn!("당일 분봉 통합(UN) 조회 실패({e}) — KRX(J)로 폴백");
        div = "J";
        all.clear();
        today_minutes(rest, code, div, &mut all).await?;
    }

    let mut date = now_kst().date();
    let mut scanned = 0usize;
    // 과거 일별 분봉: 당일 조회가 이미 J로 폴백했으면 그대로 J를 쓴다
    let mut past_div = div;
    while all.len() < MIN_BARS && scanned < MAX_SCAN_DAYS {
        date -= ChronoDuration::days(1);
        scanned += 1;
        if matches!(date.weekday(), Weekday::Sat | Weekday::Sun) {
            continue;
        }
        let date_str = date.format("%Y%m%d").to_string();
        match past_day_minutes(rest, code, &date_str, past_div, &mut all).await {
            Ok(_) => {}
            Err(e) if past_div == "UN" => {
                tracing::warn!("과거 분봉 통합(UN) 조회 실패({e}) — KRX(J)로 폴백");
                past_div = "J";
                past_day_minutes(rest, code, &date_str, past_div, &mut all).await?;
            }
            Err(e) => return Err(e),
        }
    }

    all.sort_by_key(|c| c.time);
    all.dedup_by_key(|c| c.time);
    Ok(all)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_bounds_by_market() {
        let (open, close) = session_bounds("UN");
        assert_eq!(open.format("%H%M%S").to_string(), "080000");
        assert_eq!(close.format("%H%M%S").to_string(), "200000");
        let (open, close) = session_bounds("J");
        assert_eq!(open.format("%H%M%S").to_string(), "090000");
        assert_eq!(close.format("%H%M%S").to_string(), "153000");
    }

    #[test]
    fn minus_one_minute_respects_session_open() {
        let krx_open = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
        assert_eq!(minus_one_minute("093000", krx_open).unwrap(), "092900");
        assert!(minus_one_minute("090000", krx_open).is_none());
        // 통합(UN) 세션에서는 프리마켓(08시대)도 유효
        let un_open = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        assert_eq!(minus_one_minute("083000", un_open).unwrap(), "082900");
        assert!(minus_one_minute("080000", un_open).is_none());
    }
}
