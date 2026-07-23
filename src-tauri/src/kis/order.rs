use crate::broker::OrderAck;
use crate::error::{AppError, AppResult, OrderRejection};
use crate::kis::rest::KisRest;
use crate::types::Side;

const PATH_ORDER: &str = "/uapi/domestic-stock/v1/trading/order-cash";
const PATH_CANCEL: &str = "/uapi/domestic-stock/v1/trading/order-rvsecncl";

/// 주문구분: 00 지정가, 01 시장가, 11 IOC지정가(미체결 잔량 즉시 자동취소)
pub const ORD_DVSN_LIMIT: &str = "00";
pub const ORD_DVSN_MARKET: &str = "01";
pub const ORD_DVSN_IOC_LIMIT: &str = "11";

/// 정정취소구분: 01 정정, 02 취소
const RVSE_CNCL_CANCEL: &str = "02";

fn order_tr(side: Side) -> &'static str {
    match side {
        Side::Buy => "TTTC0012U",
        Side::Sell => "TTTC0011U",
    }
}

const CANCEL_TR: &str = "TTTC0013U";

/// HTTP 응답 본문을 정상적으로 받은 뒤 KIS가 `rt_cd != 0`으로 거부한 경우를
/// 확정 거부로 분류한다. 게이트웨이의 명시적 EGW00201 미접수도 `KisRest::post`가
/// 확정 거부로 바꾸며, 그 외 전송·HTTP·본문 파싱 오류는 불명확 상태를 보존한다.
fn check_order_rt(v: &serde_json::Value) -> AppResult<()> {
    if v["rt_cd"].as_str() == Some("0") {
        Ok(())
    } else {
        let code = v["msg_cd"]
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        let message = v["msg1"]
            .as_str()
            .unwrap_or("알 수 없는 KIS 주문 거부")
            .trim()
            .to_string();
        Err(AppError::Order(OrderRejection::kis(code, message)))
    }
}

fn parse_order_ack(v: &serde_json::Value, default_message: &str) -> AppResult<OrderAck> {
    let order_no = v["output"]["ODNO"]
        .as_str()
        .unwrap_or_default()
        .trim()
        .to_string();
    let org_no = v["output"]["KRX_FWDG_ORD_ORGNO"]
        .as_str()
        .unwrap_or_default()
        .trim()
        .to_string();
    if order_no.is_empty() || org_no.is_empty() {
        return Err(AppError::Kis(
            "KIS 성공 응답에 주문번호 또는 주문조직번호가 없어 접수 여부가 불명확합니다".into(),
        ));
    }
    Ok(OrderAck {
        order_no,
        org_no,
        message: v["msg1"]
            .as_str()
            .unwrap_or(default_message)
            .trim()
            .to_string(),
    })
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
    let v = rest.post(PATH_ORDER, order_tr(side), &body).await?;
    check_order_rt(&v)?;
    parse_order_ack(&v, "주문 접수")
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
    let v = rest.post(PATH_CANCEL, CANCEL_TR, &body).await?;
    check_order_rt(&v)?;
    parse_order_ack(&v, "취소 접수")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn 주문_업무거부만_확정거부로_분류한다() {
        let error = check_order_rt(&json!({
            "rt_cd": "1",
            "msg_cd": "APBK1234",
            "msg1": "주문 불가"
        }))
        .expect_err("업무 거부여야 한다");
        let rejection = error
            .order_rejection()
            .expect("구조화된 주문 거부여야 한다");
        assert_eq!(rejection.code(), Some("APBK1234"));
        assert_eq!(rejection.message(), "주문 불가");
    }

    #[test]
    fn 성공_응답의_주문_식별자가_비면_불명확으로_처리한다() {
        let missing_order = json!({
            "rt_cd": "0",
            "msg1": "정상",
            "output": { "ODNO": "", "KRX_FWDG_ORD_ORGNO": "00950" }
        });
        let error = parse_order_ack(&missing_order, "주문 접수")
            .expect_err("빈 주문번호를 성공으로 처리하면 안 된다");
        assert!(matches!(error, AppError::Kis(_)));

        let missing_org = json!({
            "rt_cd": "0",
            "msg1": "정상",
            "output": { "ODNO": "0000012345", "KRX_FWDG_ORD_ORGNO": "" }
        });
        let error = parse_order_ack(&missing_org, "주문 접수")
            .expect_err("빈 조직번호를 성공으로 처리하면 안 된다");
        assert!(matches!(error, AppError::Kis(_)));
    }
}
