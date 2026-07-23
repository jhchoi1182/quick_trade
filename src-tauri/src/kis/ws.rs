use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::error::{AppError, AppResult};
use crate::kis::crypto::aes_cbc_decrypt;
use crate::types::{FeedEvent, FillEvent, Quote, Side};
use crate::util::{kst_str_to_fake_epoch, now_kst};

/// H0STCNI0에는 체결 건별 고유 ID가 없으므로 수신 프레임 자체의 로컬 순번을 붙인다.
/// 같은 초에 같은 수량·가격으로 체결된 두 건을 하나로 합치면 안 된다.
static FILL_NOTICE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub struct WsConfig {
    pub url: String,
    pub approval_key: String,
    /// 실시간 구독 목록: (tr_id, tr_key). 체결가/호가 TR을 종목별로 지정한다 —
    /// 통합(H0UN~)이 시세를 안 주는 종목(신형 코드 ETF/ETN)은 KRX 단독(H0ST~)으로 구독해야 한다.
    pub subs: Vec<(String, String)>,
    /// 체결통보 구독: (tr_id, HTS ID)
    pub notice: Option<(String, String)>,
}

pub fn spawn_ws(cfg: WsConfig, tx: mpsc::Sender<FeedEvent>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = 1u64;
        loop {
            let started = std::time::Instant::now();
            match run_once(&cfg, &tx).await {
                Ok(()) => break, // 엔진 종료
                Err(e) => tracing::warn!("웹소켓 오류: {e}"),
            }
            if tx.is_closed() {
                break;
            }
            let _ = tx.send(FeedEvent::Conn(false)).await;
            // 안정적으로 붙어있었다면 백오프 리셋
            if started.elapsed().as_secs() > 60 {
                backoff = 1;
            }
            tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(30);
        }
    })
}

fn subscribe_msg(approval_key: &str, tr_id: &str, tr_key: &str) -> String {
    serde_json::json!({
        "header": {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8",
        },
        "body": { "input": { "tr_id": tr_id, "tr_key": tr_key } }
    })
    .to_string()
}

async fn run_once(cfg: &WsConfig, tx: &mpsc::Sender<FeedEvent>) -> AppResult<()> {
    let (ws, _) = connect_async(cfg.url.as_str())
        .await
        .map_err(|e| AppError::Kis(format!("웹소켓 연결 실패: {e}")))?;
    let (mut write, mut read) = ws.split();

    for (tr_id, tr_key) in &cfg.subs {
        let msg = subscribe_msg(&cfg.approval_key, tr_id, tr_key);
        write
            .send(Message::Text(msg.into()))
            .await
            .map_err(ws_err)?;
    }
    if let Some((tr_id, hts_id)) = &cfg.notice {
        let msg = subscribe_msg(&cfg.approval_key, tr_id, hts_id);
        write
            .send(Message::Text(msg.into()))
            .await
            .map_err(ws_err)?;
    }
    let _ = tx.send(FeedEvent::Conn(true)).await;

    // 체결통보 복호화 키 (구독 응답으로 수신)
    let mut aes_key_iv: Option<(String, String)> = None;

    while let Some(msg) = read.next().await {
        if tx.is_closed() {
            return Ok(());
        }
        let msg = msg.map_err(ws_err)?;
        match msg {
            Message::Text(raw) => {
                let txt = raw.as_str();
                if txt.starts_with('{') {
                    if is_pingpong(txt) {
                        write
                            .send(Message::Text(raw.clone()))
                            .await
                            .map_err(ws_err)?;
                    } else if let Some(kv) = extract_aes_keys(txt) {
                        aes_key_iv = Some(kv);
                    } else if let Some((tr_id, msg)) = subscribe_failure(txt) {
                        // 구독 거절(승인키 만료 등)을 조용히 넘기면 "연결됨인데 시세 없음"이 된다
                        tracing::warn!("웹소켓 구독 실패({tr_id}): {msg}");
                    }
                } else {
                    for ev in parse_data_frame(txt, &aes_key_iv) {
                        if tx.send(ev).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
            Message::Ping(p) => write.send(Message::Pong(p)).await.map_err(ws_err)?,
            Message::Close(_) => return Err(AppError::Kis("서버가 웹소켓을 종료했습니다".into())),
            _ => {}
        }
    }
    Err(AppError::Kis("웹소켓 스트림 종료".into()))
}

fn ws_err(e: tokio_tungstenite::tungstenite::Error) -> AppError {
    AppError::Kis(format!("웹소켓 오류: {e}"))
}

fn is_pingpong(txt: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(txt)
        .map(|v| v["header"]["tr_id"].as_str() == Some("PINGPONG"))
        .unwrap_or(false)
}

fn extract_aes_keys(txt: &str) -> Option<(String, String)> {
    let v: serde_json::Value = serde_json::from_str(txt).ok()?;
    let out = &v["body"]["output"];
    let key = out["key"].as_str()?;
    let iv = out["iv"].as_str()?;
    Some((key.to_string(), iv.to_string()))
}

/// 구독 응답이 실패(rt_cd != "0")면 (tr_id, msg1) 반환
fn subscribe_failure(txt: &str) -> Option<(String, String)> {
    let v: serde_json::Value = serde_json::from_str(txt).ok()?;
    let rt_cd = v["body"]["rt_cd"].as_str()?;
    if rt_cd == "0" {
        return None;
    }
    let tr_id = v["header"]["tr_id"].as_str().unwrap_or("?").to_string();
    let msg = v["body"]["msg1"]
        .as_str()
        .unwrap_or("(메시지 없음)")
        .trim()
        .to_string();
    Some((tr_id, msg))
}

/// 실시간 데이터 프레임: `암호화여부|TR_ID|건수|필드1^필드2^...`
fn parse_data_frame(txt: &str, aes: &Option<(String, String)>) -> Vec<FeedEvent> {
    let mut parts = txt.splitn(4, '|');
    let (Some(flag), Some(tr_id), Some(cnt), Some(payload)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Vec::new();
    };
    let cnt: usize = cnt.parse().unwrap_or(1).max(1);
    let today = now_kst().format("%Y%m%d").to_string();

    match tr_id {
        // KRX 단독(H0ST~)과 KRX+NXT 통합(H0UN~)은 필드 배열이 동일하다
        "H0STCNT0" | "H0UNCNT0" => parse_ticks(payload, cnt, &today),
        "H0STASP0" | "H0UNASP0" => parse_books(payload, cnt, &today),
        "H0STCNI0" => {
            let plain = if flag == "1" {
                let Some((key, iv)) = aes else {
                    tracing::warn!("체결통보 수신했으나 AES 키 없음");
                    return Vec::new();
                };
                match aes_cbc_decrypt(key, iv, payload) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!("체결통보 복호화 실패: {e}");
                        return Vec::new();
                    }
                }
            } else {
                payload.to_string()
            };
            parse_notice(&plain)
                .map(FeedEvent::Fill)
                .into_iter()
                .collect()
        }
        _ => Vec::new(),
    }
}

fn f64_at(fields: &[&str], i: usize) -> f64 {
    fields
        .get(i)
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0.0)
}

/// H0STCNT0: [0]종목코드 [1]체결시간 [2]현재가 [5]등락률 [10]매도호가1 [11]매수호가1 [12]체결거래량
fn parse_ticks(payload: &str, cnt: usize, today: &str) -> Vec<FeedEvent> {
    let fields: Vec<&str> = payload.split('^').collect();
    let per = fields.len() / cnt;
    if per == 0 {
        return Vec::new();
    }
    (0..cnt)
        .filter_map(|r| {
            let f = &fields[r * per..(r + 1) * per];
            let code = f.first()?.to_string();
            if code.is_empty() {
                return None;
            }
            let ts = kst_str_to_fake_epoch(today, f.get(1)?)?;
            Some(FeedEvent::Quote(Quote {
                code,
                price: f64_at(f, 2),
                change_rate: f64_at(f, 5),
                ask1: f64_at(f, 10),
                bid1: f64_at(f, 11),
                ask1_qty: 0,
                bid1_qty: 0,
                volume: f64_at(f, 12),
                // H0STCNT0 [13] 누적거래량은 실제 새 체결마다 증가하고 동일
                // 프레임 재전송에서는 같아 distinct tick 식별자로 사용할 수 있다.
                trade_sequence: f64_at(f, 13).max(0.0) as u64,
                received_at_micros: crate::util::monotonic_now()
                    .as_micros()
                    .try_into()
                    .unwrap_or(u64::MAX),
                trade_ts: ts,
                // 체결 프레임은 호가 수신 시각을 갱신하지 않는다.
                book_ts: 0,
            }))
        })
        .collect()
}

/// H0STASP0: [0]종목코드 [1]영업시간 [3]매도호가1 [13]매수호가1
/// [23]매도호가잔량1 [33]매수호가잔량1
fn parse_books(payload: &str, cnt: usize, today: &str) -> Vec<FeedEvent> {
    let fields: Vec<&str> = payload.split('^').collect();
    let per = fields.len() / cnt;
    if per == 0 {
        return Vec::new();
    }
    (0..cnt)
        .filter_map(|r| {
            let f = &fields[r * per..(r + 1) * per];
            let code = f.first()?.to_string();
            if code.is_empty() {
                return None;
            }
            let ts = kst_str_to_fake_epoch(today, f.get(1)?)?;
            Some(FeedEvent::Book {
                code,
                ask1: f64_at(f, 3),
                bid1: f64_at(f, 13),
                ask1_qty: f64_at(f, 23).max(0.0) as u64,
                bid1_qty: f64_at(f, 33).max(0.0) as u64,
                ts,
            })
        })
        .collect()
}

/// 체결통보: [2]주문번호 [3]원주문번호 [4]매도매수구분(01매도/02매수)
/// [8]종목코드 [9]체결수량 [10]체결단가 [11]체결시각 [12]거부여부 [13]체결여부.
fn parse_notice(plain: &str) -> Option<FillEvent> {
    let f: Vec<&str> = plain.split('^').collect();
    if f.len() < 14 {
        return None;
    }
    let refused = f[12] != "0";
    let is_fill = f[13] == "2";
    if refused || !is_fill {
        return None;
    }
    let qty: u64 = f[9].trim().parse().ok()?;
    let price: f64 = f[10].trim().parse().ok()?;
    let side = match f[4] {
        "01" => Side::Sell,
        "02" => Side::Buy,
        _ => return None,
    };
    if qty == 0
        || !price.is_finite()
        || price <= 0.0
        || f[2].trim().is_empty()
        || f[8].trim().is_empty()
    {
        return None;
    }
    let filled_at = kst_str_to_fake_epoch(&now_kst().format("%Y%m%d").to_string(), f[11])?;
    let order_no = f[2].to_string();
    let receive_sequence = FILL_NOTICE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let received_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    // 주문번호·통보시각은 식별 힌트로 최대한 보존하되, 동일 원문의 별도 수신도
    // 구분하도록 수신 나노초와 프로세스 단조 순번을 함께 넣는다.
    let fill_id = format!(
        "ws-notice:{order_no}:{}:{received_nanos}:{receive_sequence}",
        f[11]
    );
    Some(FillEvent {
        fill_id,
        order_no,
        original_order_no: f[3].to_string(),
        org_no: String::new(),
        code: f[8].to_string(),
        side,
        qty,
        price,
        filled_at,
        status: "filled".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tick_frame() {
        // 필드 46개짜리 실데이터를 축약: 필요한 인덱스까지만 채우고 나머지는 0
        let mut fields = vec!["0"; 46];
        fields[0] = "0193T0";
        fields[1] = "093015";
        fields[2] = "12800";
        fields[5] = "1.19";
        fields[10] = "12805";
        fields[11] = "12795";
        fields[12] = "150";
        fields[13] = "123456";
        let payload = fields.join("^");
        let txt = format!("0|H0STCNT0|001|{payload}");

        let evs = parse_data_frame(&txt, &None);
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            FeedEvent::Quote(q) => {
                assert_eq!(q.code, "0193T0");
                assert_eq!(q.price, 12800.0);
                assert_eq!(q.ask1, 12805.0);
                assert_eq!(q.bid1, 12795.0);
                assert_eq!(q.volume, 150.0);
                assert_eq!(q.trade_sequence, 123_456);
                assert!(q.received_at_micros > 0);
                assert!((q.change_rate - 1.19).abs() < 1e-9);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parse_unified_tick_frame() {
        // 통합(H0UNCNT0)도 같은 파서로 처리된다
        let mut fields = vec!["0"; 46];
        fields[0] = "000660";
        fields[1] = "154502"; // 애프터마켓 시간대
        fields[2] = "194500";
        fields[12] = "77";
        let txt = format!("0|H0UNCNT0|001|{}", fields.join("^"));
        let evs = parse_data_frame(&txt, &None);
        assert_eq!(evs.len(), 1);
        assert!(matches!(&evs[0], FeedEvent::Quote(q) if q.code == "000660" && q.volume == 77.0));
    }

    #[test]
    fn parse_multi_record_tick_frame() {
        let mut rec: Vec<String> = vec!["0".into(); 46];
        rec[0] = "005930".into();
        rec[1] = "100001".into();
        rec[2] = "86400".into();
        rec[12] = "10".into();
        let one = rec.join("^");
        let txt = format!("0|H0STCNT0|002|{one}^{one}");
        let evs = parse_data_frame(&txt, &None);
        assert_eq!(evs.len(), 2);
    }

    #[test]
    fn parse_book_frame() {
        let mut fields = vec!["0"; 59];
        fields[0] = "0193T0";
        fields[1] = "093015";
        fields[3] = "12810";
        fields[13] = "12790";
        fields[23] = "1200";
        fields[33] = "900";
        let payload = fields.join("^");
        let txt = format!("0|H0STASP0|001|{payload}");

        let evs = parse_data_frame(&txt, &None);
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            FeedEvent::Book {
                code,
                ask1,
                bid1,
                ask1_qty,
                bid1_qty,
                ..
            } => {
                assert_eq!(code, "0193T0");
                assert_eq!(*ask1, 12810.0);
                assert_eq!(*bid1, 12790.0);
                assert_eq!(*ask1_qty, 1200);
                assert_eq!(*bid1_qty, 900);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parse_notice_fill_only() {
        let mut f = vec!["0"; 20];
        f[4] = "02"; // 매수
        f[8] = "0193T0";
        f[9] = "83";
        f[10] = "12805";
        f[11] = "093015";
        f[12] = "0"; // 정상
        f[13] = "2"; // 체결
        let raw = f.join("^");
        let fill = parse_notice(&raw).unwrap();
        let same_values_second_fill = parse_notice(&raw).unwrap();
        assert_eq!(fill.qty, 83);
        assert_eq!(fill.price, 12805.0);
        assert!(matches!(fill.side, Side::Buy));
        assert_ne!(
            fill.fill_id, same_values_second_fill.fill_id,
            "동일초·수량·가격의 별도 통보를 합치면 안 됨"
        );

        // 접수(체결 아님)는 무시
        f[13] = "1";
        assert!(parse_notice(&f.join("^")).is_none());
    }

    #[test]
    fn 체결통보의_방향과_경제값이_깨졌으면_무시한다() {
        let mut f = vec!["0"; 20];
        f[2] = "0000012345";
        f[4] = "02";
        f[8] = "0193T0";
        f[9] = "1";
        f[10] = "12805";
        f[11] = "093015";
        f[12] = "0";
        f[13] = "2";

        f[4] = "00";
        assert!(parse_notice(&f.join("^")).is_none());
        f[4] = "02";
        f[10] = "NaN";
        assert!(parse_notice(&f.join("^")).is_none());
        f[10] = "0";
        assert!(parse_notice(&f.join("^")).is_none());
    }

    #[test]
    fn pingpong_detected() {
        assert!(is_pingpong(
            r#"{"header":{"tr_id":"PINGPONG","datetime":"20260717"}}"#
        ));
        assert!(!is_pingpong(r#"{"header":{"tr_id":"H0STCNT0"}}"#));
    }

    #[test]
    fn subscribe_failure_detected() {
        let fail =
            r#"{"header":{"tr_id":"H0STCNT0"},"body":{"rt_cd":"9","msg1":"INVALID APPROVAL KEY"}}"#;
        let (tr_id, msg) = subscribe_failure(fail).unwrap();
        assert_eq!(tr_id, "H0STCNT0");
        assert_eq!(msg, "INVALID APPROVAL KEY");

        // 성공 응답과 rt_cd 없는 메시지는 실패로 판정하지 않는다
        let ok =
            r#"{"header":{"tr_id":"H0STCNT0"},"body":{"rt_cd":"0","msg1":"SUBSCRIBE SUCCESS"}}"#;
        assert!(subscribe_failure(ok).is_none());
        assert!(subscribe_failure(r#"{"header":{"tr_id":"PINGPONG"}}"#).is_none());
    }
}
