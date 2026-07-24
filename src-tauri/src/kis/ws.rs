use futures_util::{Sink, SinkExt, Stream, StreamExt};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use crate::error::{AppError, AppResult};
use crate::kis::crypto::aes_cbc_decrypt;
use crate::types::{FeedEvent, FillEvent, Quote, Side};
use crate::util::{kst_str_to_fake_epoch, now_kst};

/// H0STCNI0에는 체결 건별 고유 ID가 없으므로 수신 프레임 자체의 로컬 순번을 붙인다.
/// 같은 초에 같은 수량·가격으로 체결된 두 건을 하나로 합치면 안 된다.
static FILL_NOTICE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
const SUBSCRIBE_INTERVAL: Duration = Duration::from_millis(50);
const SUBSCRIBE_ACK_TIMEOUT: Duration = Duration::from_secs(5);

type SubscriptionId = (String, String);

#[derive(Debug)]
struct SubscribeAck {
    tr_id: String,
    tr_key: String,
    success: bool,
    message: String,
    aes_key_iv: Option<(String, String)>,
}

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

fn required_subscriptions(cfg: &WsConfig) -> Vec<SubscriptionId> {
    let mut subscriptions = cfg.subs.clone();
    if let Some(notice) = &cfg.notice {
        subscriptions.push(notice.clone());
    }
    subscriptions
}

async fn send_subscriptions<W>(
    write: &mut W,
    approval_key: &str,
    subscriptions: &[SubscriptionId],
) -> AppResult<()>
where
    W: Sink<Message, Error = WsError> + Unpin,
{
    for (index, (tr_id, tr_key)) in subscriptions.iter().enumerate() {
        if index > 0 {
            tokio::time::sleep(SUBSCRIBE_INTERVAL).await;
        }
        let msg = subscribe_msg(approval_key, tr_id, tr_key);
        write
            .send(Message::Text(msg.into()))
            .await
            .map_err(ws_err)?;
    }
    Ok(())
}

async fn forward_data_frame(
    txt: &str,
    aes_key_iv: &Option<(String, String)>,
    tx: &mpsc::Sender<FeedEvent>,
) -> bool {
    for event in parse_data_frame(txt, aes_key_iv) {
        if tx.send(event).await.is_err() {
            return false;
        }
    }
    true
}

fn apply_subscribe_ack(
    pending: &mut HashSet<SubscriptionId>,
    aes_key_iv: &mut Option<(String, String)>,
    ack: SubscribeAck,
) -> AppResult<()> {
    if !ack.success {
        return Err(AppError::Kis(format!(
            "웹소켓 구독 실패({}/{}): {}",
            ack.tr_id, ack.tr_key, ack.message
        )));
    }

    let subscription = (ack.tr_id.clone(), ack.tr_key.clone());
    if !pending.remove(&subscription) {
        tracing::warn!(
            "요청 목록에 없는 웹소켓 구독 성공 응답: {}/{}",
            ack.tr_id,
            ack.tr_key
        );
    }
    if let Some(keys) = ack.aes_key_iv {
        *aes_key_iv = Some(keys);
    }
    Ok(())
}

fn pending_subscriptions(pending: &HashSet<SubscriptionId>) -> String {
    let mut subscriptions: Vec<String> = pending
        .iter()
        .map(|(tr_id, tr_key)| format!("{tr_id}/{tr_key}"))
        .collect();
    subscriptions.sort();
    subscriptions.join(", ")
}

async fn confirm_subscriptions<R, W>(
    read: &mut R,
    write: &mut W,
    tx: &mpsc::Sender<FeedEvent>,
    subscriptions: &[SubscriptionId],
    aes_key_iv: &mut Option<(String, String)>,
    timeout: Duration,
) -> AppResult<()>
where
    R: Stream<Item = Result<Message, WsError>> + Unpin,
    W: Sink<Message, Error = WsError> + Unpin,
{
    let mut pending: HashSet<SubscriptionId> = subscriptions.iter().cloned().collect();
    let confirmation = async {
        while !pending.is_empty() {
            if tx.is_closed() {
                return Ok(());
            }
            let message = read
                .next()
                .await
                .ok_or_else(|| AppError::Kis("웹소켓 구독 확인 중 스트림이 종료됐습니다".into()))?
                .map_err(ws_err)?;
            match message {
                Message::Text(raw) => {
                    let txt = raw.as_str();
                    if txt.starts_with('{') {
                        if is_pingpong(txt) {
                            write
                                .send(Message::Text(raw.clone()))
                                .await
                                .map_err(ws_err)?;
                        } else if let Some(ack) = subscribe_ack(txt) {
                            apply_subscribe_ack(&mut pending, aes_key_iv, ack)?;
                        }
                    } else if !forward_data_frame(txt, aes_key_iv, tx).await {
                        return Ok(());
                    }
                }
                Message::Ping(payload) => {
                    write.send(Message::Pong(payload)).await.map_err(ws_err)?;
                }
                Message::Close(_) => {
                    return Err(AppError::Kis(
                        "웹소켓 구독 확인 중 서버가 연결을 종료했습니다".into(),
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    };

    match tokio::time::timeout(timeout, confirmation).await {
        Ok(result) => result?,
        Err(_) => {
            return Err(AppError::Kis(format!(
                "웹소켓 필수 구독 확인 시간 초과({}초): {}",
                timeout.as_secs_f64(),
                pending_subscriptions(&pending)
            )));
        }
    }
    if !tx.is_closed() {
        let _ = tx.send(FeedEvent::Conn(true)).await;
    }
    Ok(())
}

async fn run_once(cfg: &WsConfig, tx: &mpsc::Sender<FeedEvent>) -> AppResult<()> {
    let (ws, _) = connect_async(cfg.url.as_str())
        .await
        .map_err(|e| AppError::Kis(format!("웹소켓 연결 실패: {e}")))?;
    let (mut write, mut read) = ws.split();

    let subscriptions = required_subscriptions(cfg);
    send_subscriptions(&mut write, &cfg.approval_key, &subscriptions).await?;
    let mut aes_key_iv: Option<(String, String)> = None;
    confirm_subscriptions(
        &mut read,
        &mut write,
        tx,
        &subscriptions,
        &mut aes_key_iv,
        SUBSCRIBE_ACK_TIMEOUT,
    )
    .await?;
    if tx.is_closed() {
        return Ok(());
    }

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
                    } else if let Some(ack) = subscribe_ack(txt) {
                        if !ack.success {
                            return Err(AppError::Kis(format!(
                                "웹소켓 구독 실패({}/{}): {}",
                                ack.tr_id, ack.tr_key, ack.message
                            )));
                        }
                        if let Some(keys) = ack.aes_key_iv {
                            aes_key_iv = Some(keys);
                        }
                    }
                } else if !forward_data_frame(txt, &aes_key_iv, tx).await {
                    return Ok(());
                }
            }
            Message::Ping(p) => write.send(Message::Pong(p)).await.map_err(ws_err)?,
            Message::Close(_) => return Err(AppError::Kis("서버가 웹소켓을 종료했습니다".into())),
            _ => {}
        }
    }
    Err(AppError::Kis("웹소켓 스트림 종료".into()))
}

fn ws_err(e: WsError) -> AppError {
    AppError::Kis(format!("웹소켓 오류: {e}"))
}

fn is_pingpong(txt: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(txt)
        .map(|v| v["header"]["tr_id"].as_str() == Some("PINGPONG"))
        .unwrap_or(false)
}

fn subscribe_ack(txt: &str) -> Option<SubscribeAck> {
    let v: serde_json::Value = serde_json::from_str(txt).ok()?;
    let rt_cd = v["body"]["rt_cd"].as_str()?;
    let tr_id = v["header"]["tr_id"].as_str()?.to_string();
    let tr_key = v["header"]["tr_key"].as_str().unwrap_or("?").to_string();
    let message = v["body"]["msg1"]
        .as_str()
        .unwrap_or("(메시지 없음)")
        .trim()
        .to_string();
    let aes_key_iv = v["body"]["output"]["key"]
        .as_str()
        .zip(v["body"]["output"]["iv"].as_str())
        .map(|(key, iv)| (key.to_string(), iv.to_string()));
    Some(SubscribeAck {
        tr_id,
        tr_key,
        success: rt_cd == "0",
        message,
        aes_key_iv,
    })
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
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Instant;

    #[derive(Default)]
    struct RecordingSink {
        sent: Arc<Mutex<Vec<(Instant, Message)>>>,
    }

    impl Sink<Message> for RecordingSink {
        type Error = WsError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            self.sent.lock().unwrap().push((Instant::now(), item));
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    fn ack_message(tr_id: &str, tr_key: &str, rt_cd: &str, msg1: &str) -> Message {
        Message::Text(
            serde_json::json!({
                "header": { "tr_id": tr_id, "tr_key": tr_key },
                "body": { "rt_cd": rt_cd, "msg1": msg1 }
            })
            .to_string()
            .into(),
        )
    }

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
    fn 구독응답은_tr과_종목과_복호화키를_보존한다() {
        let failure = subscribe_ack(
            r#"{"header":{"tr_id":"H0STCNT0","tr_key":"0193T0"},"body":{"rt_cd":"9","msg1":"INVALID APPROVAL KEY"}}"#,
        )
        .unwrap();
        assert_eq!(failure.tr_id, "H0STCNT0");
        assert_eq!(failure.tr_key, "0193T0");
        assert!(!failure.success);
        assert_eq!(failure.message, "INVALID APPROVAL KEY");

        let success = subscribe_ack(
            r#"{"header":{"tr_id":"H0STCNI0","tr_key":"test-id"},"body":{"rt_cd":"0","msg1":"SUBSCRIBE SUCCESS","output":{"key":"secret-key","iv":"secret-iv"}}}"#,
        )
        .unwrap();
        assert!(success.success);
        assert_eq!(
            success.aes_key_iv,
            Some(("secret-key".into(), "secret-iv".into()))
        );
        assert!(subscribe_ack(r#"{"header":{"tr_id":"PINGPONG"}}"#).is_none());
    }

    #[tokio::test]
    async fn 구독요청은_각각_50ms_이상_간격을_둔다() {
        let mut sink = RecordingSink::default();
        let sent = Arc::clone(&sink.sent);
        let subscriptions = vec![
            ("H0STCNT0".into(), "000660".into()),
            ("H0STASP0".into(), "000660".into()),
            ("H0STCNI0".into(), "test-id".into()),
        ];

        send_subscriptions(&mut sink, "approval", &subscriptions)
            .await
            .unwrap();

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), subscriptions.len());
        for pair in sent.windows(2) {
            assert!(
                pair[1].0.duration_since(pair[0].0) >= SUBSCRIBE_INTERVAL,
                "구독 요청 간격이 {:?}로 너무 짧습니다",
                pair[1].0.duration_since(pair[0].0)
            );
        }
    }

    #[tokio::test]
    async fn 모든_구독확인_전_시세를_보존하고_완료후에만_연결된다() {
        let subscriptions = vec![
            ("H0STCNT0".into(), "000660".into()),
            ("H0STASP0".into(), "000660".into()),
        ];
        let mut fields = vec!["0"; 46];
        fields[0] = "000660";
        fields[1] = "100001";
        fields[2] = "185000";
        fields[12] = "10";
        let data = Message::Text(format!("0|H0STCNT0|001|{}", fields.join("^")).into());
        let messages = vec![
            Ok(ack_message("H0STCNT0", "000660", "0", "SUBSCRIBE SUCCESS")),
            Ok(data),
            Ok(ack_message("H0STASP0", "000660", "0", "SUBSCRIBE SUCCESS")),
        ];
        let mut read = futures_util::stream::iter(messages);
        let mut write = RecordingSink::default();
        let (tx, mut rx) = mpsc::channel(4);
        let mut aes = None;

        confirm_subscriptions(
            &mut read,
            &mut write,
            &tx,
            &subscriptions,
            &mut aes,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        assert!(matches!(rx.recv().await, Some(FeedEvent::Quote(_))));
        assert!(matches!(rx.recv().await, Some(FeedEvent::Conn(true))));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn 구독거절은_tr과_종목을_포함해_연결실패가_된다() {
        let subscriptions = vec![("H0STASP0".into(), "0193T0".into())];
        let mut read = futures_util::stream::iter(vec![Ok(ack_message(
            "H0STASP0",
            "0193T0",
            "9",
            "TOO MANY REQUESTS",
        ))]);
        let mut write = RecordingSink::default();
        let (tx, mut rx) = mpsc::channel(1);
        let mut aes = None;

        let error = confirm_subscriptions(
            &mut read,
            &mut write,
            &tx,
            &subscriptions,
            &mut aes,
            Duration::from_secs(1),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("H0STASP0/0193T0"), "{error}");
        assert!(error.contains("TOO MANY REQUESTS"), "{error}");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn 확인되지_않은_필수구독은_시간초과와_미확인목록을_남긴다() {
        let subscriptions = vec![
            ("H0STCNT0".into(), "000660".into()),
            ("H0STASP0".into(), "0197X0".into()),
        ];
        let messages = vec![Ok(ack_message(
            "H0STCNT0",
            "000660",
            "0",
            "SUBSCRIBE SUCCESS",
        ))];
        let mut read = futures_util::stream::iter(messages)
            .chain(futures_util::stream::pending::<Result<Message, WsError>>());
        let mut write = RecordingSink::default();
        let (tx, mut rx) = mpsc::channel(1);
        let mut aes = None;

        let error = confirm_subscriptions(
            &mut read,
            &mut write,
            &tx,
            &subscriptions,
            &mut aes,
            Duration::from_millis(10),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("시간 초과"), "{error}");
        assert!(error.contains("H0STASP0/0197X0"), "{error}");
        assert!(rx.try_recv().is_err());
    }
}
