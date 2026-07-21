use crate::broker::OrderAck;
use crate::error::AppResult;
use crate::kis::rest::KisRest;
use crate::types::{Side, TradeMode};

const PATH_ORDER: &str = "/uapi/domestic-stock/v1/trading/order-cash";
const PATH_CANCEL: &str = "/uapi/domestic-stock/v1/trading/order-rvsecncl";

/// 주문구분: 00 지정가, 01 시장가, 11 IOC지정가(미체결 잔량 즉시 자동취소)
pub const ORD_DVSN_LIMIT: &str = "00";
pub const ORD_DVSN_MARKET: &str = "01";
pub const ORD_DVSN_IOC_LIMIT: &str = "11";

/// 정정취소구분: 01 정정, 02 취소
const RVSE_CNCL_CANCEL: &str = "02";

fn order_tr(mode: TradeMode, side: Side) -> &'static str {
    match (mode, side) {
        (TradeMode::Paper, Side::Buy) => "VTTC0012U",
        (TradeMode::Paper, Side::Sell) => "VTTC0011U",
        (_, Side::Buy) => "TTTC0012U",
        (_, Side::Sell) => "TTTC0011U",
    }
}

/// 정정취소 TR: 실전 TTTC0013U / 모의 VTTC0013U
fn cancel_tr(mode: TradeMode) -> &'static str {
    match mode {
        TradeMode::Paper => "VTTC0013U",
        _ => "TTTC0013U",
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn order_cash(
    rest: &KisRest,
    cano: &str,
    prdt: &str,
    side: Side,
    code: &str,
    qty: u64,
    ord_dvsn: &str,
    price: u64,
    exchange: &str,
) -> AppResult<OrderAck> {
    let body = serde_json::json!({
        "CANO": cano,
        "ACNT_PRDT_CD": prdt,
        "PDNO": code,
        "ORD_DVSN": ord_dvsn,
        "ORD_QTY": qty.to_string(),
        "ORD_UNPR": price.to_string(),
        // KRX 고정 또는 SOR(스마트 주문 라우팅: KRX/NXT 중 유리한 호가로)
        "EXCG_ID_DVSN_CD": exchange,
    });
    let v = rest.post(PATH_ORDER, order_tr(rest.mode, side), &body).await?;
    KisRest::check_rt(&v)?;
    Ok(OrderAck {
        order_no: v["output"]["ODNO"].as_str().unwrap_or_default().to_string(),
        org_no: v["output"]["KRX_FWDG_ORD_ORGNO"].as_str().unwrap_or_default().to_string(),
        message: v["msg1"].as_str().unwrap_or("주문 접수").trim().to_string(),
    })
}

/// 미체결 주문 취소 (잔량 전부). 이미 체결된 주문은 KIS가 거부한다.
/// KIS는 원주문 조직번호(org_no)+원주문번호(order_no)로 주문을 식별하므로 종목코드(PDNO)는 불필요하다.
pub async fn cancel_order(
    rest: &KisRest,
    cano: &str,
    prdt: &str,
    order_no: &str,
    org_no: &str,
    exchange: &str,
) -> AppResult<OrderAck> {
    let body = serde_json::json!({
        "CANO": cano,
        "ACNT_PRDT_CD": prdt,
        "KRX_FWDG_ORD_ORGNO": org_no,
        "ORGN_ODNO": order_no,
        // 취소는 원주문 정보만 있으면 되므로 주문구분/수량/단가는 형식상 값
        "ORD_DVSN": ORD_DVSN_LIMIT,
        "RVSE_CNCL_DVSN_CD": RVSE_CNCL_CANCEL,
        "ORD_QTY": "0",
        "ORD_UNPR": "0",
        "QTY_ALL_ORD_YN": "Y",
        "EXCG_ID_DVSN_CD": exchange,
    });
    let v = rest.post(PATH_CANCEL, cancel_tr(rest.mode), &body).await?;
    KisRest::check_rt(&v)?;
    Ok(OrderAck {
        order_no: v["output"]["ODNO"].as_str().unwrap_or(order_no).to_string(),
        org_no: v["output"]["KRX_FWDG_ORD_ORGNO"].as_str().unwrap_or(org_no).to_string(),
        message: v["msg1"].as_str().unwrap_or("취소 접수").trim().to_string(),
    })
}
