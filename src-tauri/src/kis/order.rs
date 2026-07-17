use crate::broker::OrderAck;
use crate::error::AppResult;
use crate::kis::rest::KisRest;
use crate::types::{Side, TradeMode};

const PATH_ORDER: &str = "/uapi/domestic-stock/v1/trading/order-cash";

/// 주문구분: 00 지정가, 01 시장가, 11 IOC지정가(미체결 잔량 즉시 자동취소)
pub const ORD_DVSN_LIMIT: &str = "00";
pub const ORD_DVSN_MARKET: &str = "01";
pub const ORD_DVSN_IOC_LIMIT: &str = "11";

fn order_tr(mode: TradeMode, side: Side) -> &'static str {
    match (mode, side) {
        (TradeMode::Paper, Side::Buy) => "VTTC0012U",
        (TradeMode::Paper, Side::Sell) => "VTTC0011U",
        (_, Side::Buy) => "TTTC0012U",
        (_, Side::Sell) => "TTTC0011U",
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
        message: v["msg1"].as_str().unwrap_or("주문 접수").trim().to_string(),
    })
}
