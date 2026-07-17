use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::error::{AppError, AppResult};
use crate::kis::crypto::aes_cbc_decrypt;
use crate::types::{FeedEvent, FillEvent, Quote, Side};
use crate::util::{kst_str_to_fake_epoch, now_kst};

pub struct WsConfig {
    pub url: String,
    pub approval_key: String,
    /// 체결가 TR: 실전 H0UNCNT0(KRX+NXT 통합) / 모의 H0STCNT0(KRX)
    pub tr_price: String,
    /// 호가 TR: 실전 H0UNASP0(통합) / 모의 H0STASP0(KRX)
    pub tr_book: String,
    /// 실시간 체결가 구독 종목
    pub price_codes: Vec<String>,
    /// 실시간 호가 구독 종목
    pub book_codes: Vec<String>,
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

    for code in &cfg.price_codes {
        let msg = subscribe_msg(&cfg.approval_key, &cfg.tr_price, code);
        write.send(Message::Text(msg.into())).await.map_err(ws_err)?;
    }
    for code in &cfg.book_codes {
        let msg = subscribe_msg(&cfg.approval_key, &cfg.tr_book, code);
        write.send(Message::Text(msg.into())).await.map_err(ws_err)?;
    }
    if let Some((tr_id, hts_id)) = &cfg.notice {
        let msg = subscribe_msg(&cfg.approval_key, tr_id, hts_id);
        write.send(Message::Text(msg.into())).await.map_err(ws_err)?;
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
                        write.send(Message::Text(raw.clone())).await.map_err(ws_err)?;
                    } else if let Some(kv) = extract_aes_keys(txt) {
                        aes_key_iv = Some(kv);
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
        "H0STCNI0" | "H0STCNI9" => {
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
            parse_notice(&plain).map(FeedEvent::Fill).into_iter().collect()
        }
        _ => Vec::new(),
    }
}

fn f64_at(fields: &[&str], i: usize) -> f64 {
    fields.get(i).and_then(|s| s.trim().parse().ok()).unwrap_or(0.0)
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
                volume: f64_at(f, 12),
                ts,
            }))
        })
        .collect()
}

/// H0STASP0: [0]종목코드 [1]영업시간 [3]매도호가1 [13]매수호가1
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
                ts,
            })
        })
        .collect()
}

/// 체결통보: [4]매도매수구분(01매도/02매수) [8]종목코드 [9]체결수량 [10]체결단가 [12]거부여부(0정상) [13]체결여부(2체결)
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
    if qty == 0 {
        return None;
    }
    Some(FillEvent {
        code: f[8].to_string(),
        side: if f[4] == "01" { Side::Sell } else { Side::Buy },
        qty,
        price,
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
        let payload = fields.join("^");
        let txt = format!("0|H0STASP0|001|{payload}");

        let evs = parse_data_frame(&txt, &None);
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            FeedEvent::Book { code, ask1, bid1, .. } => {
                assert_eq!(code, "0193T0");
                assert_eq!(*ask1, 12810.0);
                assert_eq!(*bid1, 12790.0);
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
        f[12] = "0"; // 정상
        f[13] = "2"; // 체결
        let fill = parse_notice(&f.join("^")).unwrap();
        assert_eq!(fill.qty, 83);
        assert_eq!(fill.price, 12805.0);
        assert!(matches!(fill.side, Side::Buy));

        // 접수(체결 아님)는 무시
        f[13] = "1";
        assert!(parse_notice(&f.join("^")).is_none());
    }

    #[test]
    fn pingpong_detected() {
        assert!(is_pingpong(r#"{"header":{"tr_id":"PINGPONG","datetime":"20260717"}}"#));
        assert!(!is_pingpong(r#"{"header":{"tr_id":"H0STCNT0"}}"#));
    }
}
