use crate::broker::{BrokerFill, BrokerOpenOrder, BrokerOrderStatus};
use crate::error::{AppError, AppResult};
use crate::kis::rest::{num_f64, num_u64, KisRest};
use crate::types::Side;
use crate::util::{kst_str_to_fake_epoch, now_kst};

const PATH_DAILY: &str = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld";
const PATH_OPEN: &str = "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl";

fn exact_side(value: &serde_json::Value) -> AppResult<Side> {
    match value.as_str().unwrap_or_default() {
        "01" => Ok(Side::Sell),
        "02" => Ok(Side::Buy),
        value => Err(AppError::Parse(format!(
            "일별 주문체결의 매도매수 구분값이 올바르지 않습니다: {value}"
        ))),
    }
}

/// 일별 주문체결 응답의 체결 반영시각을 KST 가짜 epoch로 변환한다.
/// `infm_tmd`는 주문시각인 `ord_tmd`보다 실제 체결통보에 가깝다. 다만 일부
/// 응답에서 비어 있거나 잘못된 값이 올 수 있으므로 파싱까지 성공했을 때만 쓰고,
/// 그렇지 않으면 주문시각으로 폴백한다.
fn fill_timestamp(row: &serde_json::Value, trading_date: &str) -> i64 {
    ["infm_tmd", "ord_tmd"]
        .into_iter()
        .find_map(|key| {
            row[key]
                .as_str()
                .and_then(|time| kst_str_to_fake_epoch(trading_date, time))
        })
        .unwrap_or_default()
}

fn parse_exact_order_status(
    row: &serde_json::Value,
    trading_date: &str,
    expected_order_no: &str,
) -> AppResult<Option<BrokerOrderStatus>> {
    let order_no = row["odno"].as_str().unwrap_or_default().trim();
    if order_no != expected_order_no {
        return Ok(None);
    }
    let row_date = row["ord_dt"].as_str().unwrap_or_default().trim();
    if !row_date.is_empty() && row_date != trading_date {
        return Ok(None);
    }
    let order_time = row["ord_tmd"].as_str().unwrap_or_default();
    let ordered_at = kst_str_to_fake_epoch(trading_date, order_time).ok_or_else(|| {
        AppError::Parse(format!(
            "일별 주문체결의 주문시각이 올바르지 않습니다: {trading_date} {order_time}"
        ))
    })?;

    Ok(Some(BrokerOrderStatus {
        trading_date: trading_date.to_string(),
        order_no: order_no.to_string(),
        original_order_no: row["orgn_odno"]
            .as_str()
            .unwrap_or_default()
            .trim()
            .to_string(),
        org_no: row["ord_gno_brno"]
            .as_str()
            .unwrap_or_default()
            .trim()
            .to_string(),
        code: row["pdno"].as_str().unwrap_or_default().trim().to_string(),
        side: exact_side(&row["sll_buy_dvsn_cd"])?,
        ordered_qty: num_u64(&row["ord_qty"]),
        filled_qty: num_u64(&row["tot_ccld_qty"]),
        cancelled_qty: row
            .get("cnc_cfrm_qty")
            .map(num_u64)
            // 구 응답/테스트 fixture의 오탈자 키도 읽되 공식 키를 우선한다.
            .unwrap_or_else(|| num_u64(&row["cncl_cfrm_qty"])),
        remaining_qty: num_u64(&row["rmn_qty"]),
        rejected_qty: num_u64(&row["rjct_qty"]),
        avg_fill_price: num_f64(&row["avg_prvs"]),
        ordered_at,
    }))
}

/// KIS 주문일자와 주문번호를 함께 지정해 주문 하나의 누적 종결 상태를 조회한다.
///
/// 체결만 조회하는 `CCLD_DVSN=01`과 달리 전체(`00`)를 사용해야 전량 거부,
/// 전량 취소, 부분체결 후 잔량취소를 체결 지연과 구분할 수 있다. 서버 필터를
/// 신뢰하는 데 그치지 않고 응답에서도 주문번호와 주문일자를 다시 대조한다.
pub async fn order_status(
    rest: &KisRest,
    cano: &str,
    prdt: &str,
    trading_date: &str,
    order_no: &str,
) -> AppResult<Option<BrokerOrderStatus>> {
    if trading_date.len() != 8 || !trading_date.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(AppError::Parse(format!(
            "주문일자는 YYYYMMDD 형식이어야 합니다: {trading_date}"
        )));
    }
    let order_no = order_no.trim();
    if order_no.is_empty() {
        return Err(AppError::Parse("조회할 주문번호가 비어 있습니다".into()));
    }

    let mut fk = String::new();
    let mut nk = String::new();
    let mut tr_cont: Option<String> = None;
    let mut result: Option<BrokerOrderStatus> = None;

    for _ in 0..20 {
        let params = vec![
            ("CANO", cano.to_string()),
            ("ACNT_PRDT_CD", prdt.to_string()),
            ("INQR_STRT_DT", trading_date.to_string()),
            ("INQR_END_DT", trading_date.to_string()),
            ("SLL_BUY_DVSN_CD", "00".to_string()),
            ("INQR_DVSN", "00".to_string()),
            ("PDNO", String::new()),
            ("CCLD_DVSN", "00".to_string()),
            ("ORD_GNO_BRNO", String::new()),
            ("ODNO", order_no.to_string()),
            ("INQR_DVSN_3", "00".to_string()),
            ("INQR_DVSN_1", String::new()),
            ("EXCG_ID_DVSN_CD", "ALL".to_string()),
            ("CTX_AREA_FK100", fk.clone()),
            ("CTX_AREA_NK100", nk.clone()),
        ];
        let page = rest
            .get_page(PATH_DAILY, "TTTC0081R", &params, tr_cont.as_deref())
            .await?;
        KisRest::check_rt(&page.body)?;
        for row in page.body["output1"].as_array().cloned().unwrap_or_default() {
            let Some(status) = parse_exact_order_status(&row, trading_date, order_no)? else {
                continue;
            };
            if result.as_ref().is_some_and(|existing| existing != &status) {
                return Err(AppError::Parse(format!(
                    "같은 주문번호의 일별 주문상태가 서로 다릅니다: {trading_date}/{order_no}"
                )));
            }
            result = Some(status);
        }
        fk = page.body["ctx_area_fk100"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        nk = page.body["ctx_area_nk100"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if page.tr_cont.is_empty() || page.tr_cont == "D" || (fk.is_empty() && nk.is_empty()) {
            break;
        }
        tr_cont = Some("N".to_string());
    }

    Ok(result)
}

/// 당일 체결 조회. KIS 연속조회 헤더와 CTX 키를 함께 넘겨 모든 페이지를 합친다.
pub async fn today_fills(rest: &KisRest, cano: &str, prdt: &str) -> AppResult<Vec<BrokerFill>> {
    let date = now_kst().format("%Y%m%d").to_string();
    let mut fk = String::new();
    let mut nk = String::new();
    let mut tr_cont: Option<String> = None;
    let mut result = Vec::new();

    for _ in 0..20 {
        let params = vec![
            ("CANO", cano.to_string()),
            ("ACNT_PRDT_CD", prdt.to_string()),
            ("INQR_STRT_DT", date.clone()),
            ("INQR_END_DT", date.clone()),
            ("SLL_BUY_DVSN_CD", "00".to_string()),
            ("INQR_DVSN", "00".to_string()),
            ("PDNO", String::new()),
            ("CCLD_DVSN", "01".to_string()),
            ("ORD_GNO_BRNO", String::new()),
            ("ODNO", String::new()),
            ("INQR_DVSN_3", "00".to_string()),
            ("INQR_DVSN_1", String::new()),
            ("EXCG_ID_DVSN_CD", "ALL".to_string()),
            ("CTX_AREA_FK100", fk.clone()),
            ("CTX_AREA_NK100", nk.clone()),
        ];
        let page = rest
            .get_page(PATH_DAILY, "TTTC0081R", &params, tr_cont.as_deref())
            .await?;
        KisRest::check_rt(&page.body)?;
        for row in page.body["output1"].as_array().cloned().unwrap_or_default() {
            let qty = num_u64(&row["tot_ccld_qty"]);
            if qty == 0 {
                continue;
            }
            let order_no = row["odno"].as_str().unwrap_or_default().to_string();
            let filled_at = fill_timestamp(&row, &date);
            if filled_at <= 0 {
                return Err(AppError::Parse(format!(
                    "일별 주문체결의 체결시각이 올바르지 않습니다: {date}/{order_no}"
                )));
            }
            result.push(BrokerFill {
                fill_id: format!("daily:{order_no}:{qty}:{}", num_f64(&row["avg_prvs"])),
                order_no,
                original_order_no: row["orgn_odno"].as_str().unwrap_or_default().to_string(),
                org_no: row["ord_gno_brno"].as_str().unwrap_or_default().to_string(),
                code: row["pdno"].as_str().unwrap_or_default().to_string(),
                side: exact_side(&row["sll_buy_dvsn_cd"])?,
                qty,
                price: num_f64(&row["avg_prvs"]),
                filled_at,
            });
        }
        fk = page.body["ctx_area_fk100"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        nk = page.body["ctx_area_nk100"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if page.tr_cont.is_empty() || page.tr_cont == "D" || (fk.is_empty() && nk.is_empty()) {
            break;
        }
        tr_cont = Some("N".to_string());
    }
    Ok(result)
}

/// 취소 가능한 주문 조회. 실제 취소 전 잔량을 다시 확인하는 소스 오브 트루스다.
pub async fn open_orders(
    rest: &KisRest,
    cano: &str,
    prdt: &str,
) -> AppResult<Vec<BrokerOpenOrder>> {
    let mut fk = String::new();
    let mut nk = String::new();
    let mut tr_cont: Option<String> = None;
    let mut result = Vec::new();

    for _ in 0..20 {
        let params = vec![
            ("CANO", cano.to_string()),
            ("ACNT_PRDT_CD", prdt.to_string()),
            ("CTX_AREA_FK100", fk.clone()),
            ("CTX_AREA_NK100", nk.clone()),
            ("INQR_DVSN_1", "0".to_string()),
            ("INQR_DVSN_2", "0".to_string()),
        ];
        let page = rest
            .get_page(PATH_OPEN, "TTTC0084R", &params, tr_cont.as_deref())
            .await?;
        KisRest::check_rt(&page.body)?;
        let date = now_kst().format("%Y%m%d").to_string();
        for row in page.body["output"].as_array().cloned().unwrap_or_default() {
            let cancelable_qty = num_u64(&row["psbl_qty"]);
            if cancelable_qty == 0 {
                continue;
            }
            result.push(BrokerOpenOrder {
                order_no: row["odno"].as_str().unwrap_or_default().to_string(),
                original_order_no: row["orgn_odno"].as_str().unwrap_or_default().to_string(),
                org_no: row["ord_gno_brno"].as_str().unwrap_or_default().to_string(),
                code: row["pdno"].as_str().unwrap_or_default().to_string(),
                side: exact_side(&row["sll_buy_dvsn_cd"])?,
                ordered_qty: num_u64(&row["ord_qty"]),
                filled_qty: num_u64(&row["tot_ccld_qty"]),
                cancelable_qty,
                price: num_f64(&row["ord_unpr"]),
                ordered_at: kst_str_to_fake_epoch(
                    &date,
                    row["ord_tmd"].as_str().unwrap_or("000000"),
                )
                .ok_or_else(|| {
                    AppError::Parse(format!(
                        "정정취소 가능 주문의 주문시각이 올바르지 않습니다: {date}/{}",
                        row["odno"].as_str().unwrap_or_default()
                    ))
                })?,
            });
        }
        fk = page.body["ctx_area_fk100"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        nk = page.body["ctx_area_nk100"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if page.tr_cont.is_empty() || page.tr_cont == "D" || (fk.is_empty() && nk.is_empty()) {
            break;
        }
        tr_cont = Some("N".to_string());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;

    fn order_row(
        ordered_qty: u64,
        filled_qty: u64,
        cancelled_qty: u64,
        remaining_qty: u64,
        rejected_qty: u64,
        avg_fill_price: f64,
    ) -> Value {
        json!({
            "ord_dt": "20260722",
            "ord_gno_brno": "00950",
            "odno": "0000012345",
            "orgn_odno": "",
            "sll_buy_dvsn_cd": "01",
            "pdno": "0193T0",
            "ord_qty": ordered_qty.to_string(),
            "ord_tmd": "101500",
            "tot_ccld_qty": filled_qty.to_string(),
            "avg_prvs": avg_fill_price.to_string(),
            "cnc_cfrm_qty": cancelled_qty.to_string(),
            "rmn_qty": remaining_qty.to_string(),
            "rjct_qty": rejected_qty.to_string()
        })
    }

    #[test]
    fn 정확한_주문번호의_누적상태를_파싱한다() {
        let status = parse_exact_order_status(
            &order_row(10, 10, 0, 0, 0, 10_005.0),
            "20260722",
            "0000012345",
        )
        .unwrap()
        .unwrap();

        assert_eq!(status.trading_date, "20260722");
        assert_eq!(status.order_no, "0000012345");
        assert_eq!(status.org_no, "00950");
        assert_eq!(status.code, "0193T0");
        assert_eq!(status.side, Side::Sell);
        assert_eq!(status.ordered_qty, 10);
        assert_eq!(status.filled_qty, 10);
        assert_eq!(status.avg_fill_price, 10_005.0);
        assert!(status.is_terminal());
    }

    #[test]
    fn 체결_취소_거부_누적값으로만_종결을_확정한다() {
        for (row, expected_terminal) in [
            (order_row(10, 10, 0, 0, 0, 10_005.0), true),
            (order_row(10, 4, 6, 0, 0, 10_005.0), true),
            (order_row(10, 0, 0, 0, 10, 0.0), true),
            (order_row(10, 4, 0, 6, 0, 10_005.0), false),
            // 조회 반영 중인 모순 상태는 주문 부재나 종결로 해석하지 않는다.
            (order_row(10, 0, 0, 0, 0, 0.0), false),
            // 체결수량만 먼저 보이고 평균가가 아직 없으면 경제 체결 적용을 기다린다.
            (order_row(10, 10, 0, 0, 0, 0.0), false),
            (order_row(10, 11, 0, 0, 0, 10_005.0), false),
            // 각 항목은 주문수량 이하지만 합계가 초과한 모순 상태도 종결이 아니다.
            (order_row(10, 6, 5, 0, 0, 10_005.0), false),
        ] {
            let status = parse_exact_order_status(&row, "20260722", "0000012345")
                .unwrap()
                .unwrap();
            assert_eq!(status.is_terminal(), expected_terminal, "row={row}");
        }
    }

    #[test]
    fn 서버가_다른_주문이나_날짜를_반환하면_제외한다() {
        let mut other_order = order_row(10, 10, 0, 0, 0, 10_005.0);
        other_order["odno"] = json!("0000099999");
        assert!(
            parse_exact_order_status(&other_order, "20260722", "0000012345")
                .unwrap()
                .is_none()
        );

        let mut other_date = order_row(10, 10, 0, 0, 0, 10_005.0);
        other_date["ord_dt"] = json!("20260721");
        assert!(
            parse_exact_order_status(&other_date, "20260722", "0000012345")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn 정확한_주문의_식별필드가_깨졌으면_오류로_보류한다() {
        let mut invalid_side = order_row(10, 10, 0, 0, 0, 10_005.0);
        invalid_side["sll_buy_dvsn_cd"] = json!("00");
        assert!(parse_exact_order_status(&invalid_side, "20260722", "0000012345").is_err());

        let mut invalid_time = order_row(10, 10, 0, 0, 0, 10_005.0);
        invalid_time["ord_tmd"] = json!("잘못된시각");
        assert!(parse_exact_order_status(&invalid_time, "20260722", "0000012345").is_err());
    }

    #[test]
    fn 체결시각은_유효한_통보시각을_주문시각보다_우선한다() {
        let mut row = order_row(10, 10, 0, 0, 0, 10_005.0);
        row["infm_tmd"] = json!("101537");

        assert_eq!(
            fill_timestamp(&row, "20260722"),
            kst_str_to_fake_epoch("20260722", "101537").unwrap()
        );
    }

    #[test]
    fn 통보시각이_비었거나_잘못되면_주문시각으로_폴백한다() {
        for invalid in [json!(null), json!(""), json!("999999"), json!("10:15:37")] {
            let mut row = order_row(10, 10, 0, 0, 0, 10_005.0);
            row["infm_tmd"] = invalid;

            assert_eq!(
                fill_timestamp(&row, "20260722"),
                kst_str_to_fake_epoch("20260722", "101500").unwrap()
            );
        }
    }
}
