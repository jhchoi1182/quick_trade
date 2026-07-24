//! KIS 웹소켓 구독 진단 프로브.
//!
//! 앱과 동일한 시세 구독(체결 + 호가)을 등록하고 모든 ack 원문과
//! (TR, 종목)별 데이터 프레임 수를 출력한다. 체결통보(H0STCNI0)는 실행 중인
//! 앱의 통보 채널과 세션 혼선을 일으킬 수 있어 의도적으로 제외한다.
//!
//! 사용법: cargo run --example ws_probe [관찰초수=60]
//! 장 마감 후에는 ack 거절 여부만, 장중에는 종목별 프레임 밀도까지 실측된다.
//! 주의: 실행 중인 앱과 병행 세션이 생기므로 장중에는 앱 종료 또는 무포지션 상태에서 실행.

use futures_util::{SinkExt, StreamExt};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const REAL_BASE: &str = "https://openapi.koreainvestment.com:9443";
const REAL_WS: &str = "ws://ops.koreainvestment.com:21000";
const SUBSCRIBE_INTERVAL: Duration = Duration::from_millis(50);
const SUMMARY_INTERVAL: Duration = Duration::from_secs(10);

type SubId = (String, String);

fn now_hms() -> String {
    chrono::Local::now().format("%H:%M:%S%.3f").to_string()
}

fn config_path() -> std::path::PathBuf {
    dirs::config_dir()
        .expect("설정 폴더를 찾을 수 없습니다")
        .join("easy-scalping")
        .join("config.json")
}

fn string_list(config: &serde_json::Value, key: &str) -> Vec<String> {
    config[key]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item["code"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// kis/mod.rs::build_subs와 동일한 규칙으로 구독 목록을 구성한다 (체결통보 제외).
fn build_probe_subs(config: &serde_json::Value) -> Vec<SubId> {
    let auto = &config["autoSymbols"];
    let auto_codes: Vec<String> = ["underlying", "leverage", "inverse"]
        .iter()
        .filter_map(|k| auto[*k].as_str().map(str::to_string))
        .collect();

    let mut trade_codes = string_list(config, "tradeSymbols");
    trade_codes.extend(auto_codes.clone());
    trade_codes.sort();
    trade_codes.dedup();

    let mut all_codes = string_list(config, "chartSymbols");
    all_codes.extend(string_list(config, "tradeSymbols"));
    all_codes.extend(auto_codes);
    let mut seen = std::collections::HashSet::new();
    all_codes.retain(|code| seen.insert(code.clone()));

    let mut subs = Vec::new();
    for code in &all_codes {
        let tr = if trade_codes.contains(code) {
            "H0STCNT0"
        } else {
            "H0UNCNT0"
        };
        subs.push((tr.to_string(), code.clone()));
    }
    for code in &trade_codes {
        subs.push(("H0STASP0".to_string(), code.clone()));
    }
    subs
}

async fn issue_approval_key(app_key: &str, app_secret: &str) -> String {
    let body = serde_json::json!({
        "grant_type": "client_credentials",
        "appkey": app_key,
        "secretkey": app_secret,
    });
    let resp = reqwest::Client::new()
        .post(format!("{REAL_BASE}/oauth2/Approval"))
        .json(&body)
        .send()
        .await
        .expect("접속키 발급 요청 실패");
    let v: serde_json::Value = resp.json().await.expect("접속키 응답 파싱 실패");
    v["approval_key"]
        .as_str()
        .unwrap_or_else(|| panic!("approval_key 발급 실패: {v}"))
        .to_string()
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

struct SubStats {
    ack: Option<(String, String)>,
    frames: u64,
    last_frame: Option<Instant>,
}

fn print_summary(stats: &BTreeMap<SubId, SubStats>) {
    println!("--- [{}] 구독별 상태 요약 ---", now_hms());
    for ((tr, key), stat) in stats {
        let ack = match &stat.ack {
            Some((rt_cd, msg)) => format!("ack rt_cd={rt_cd} \"{msg}\""),
            None => "ack 미수신".to_string(),
        };
        let last = match stat.last_frame {
            Some(at) => format!("{:.1}초 전", at.elapsed().as_secs_f64()),
            None => "-".to_string(),
        };
        println!(
            "  {tr}/{key:<8} {ack:<40} 데이터 {:>5}건, 마지막 {last}",
            stat.frames
        );
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let observe_secs: u64 = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    let raw = std::fs::read_to_string(config_path()).expect("config.json 읽기 실패");
    let config: serde_json::Value = serde_json::from_str(&raw).expect("config.json 파싱 실패");
    let app_key = config["appKey"].as_str().expect("appKey 없음").to_string();
    let app_secret = config["appSecret"]
        .as_str()
        .expect("appSecret 없음")
        .to_string();

    let subs = build_probe_subs(&config);
    println!("[{}] 구독 {}건 준비:", now_hms(), subs.len());
    for (tr, key) in &subs {
        println!("  {tr}/{key}");
    }

    let approval_key = issue_approval_key(&app_key, &app_secret).await;
    println!("[{}] 접속키 발급 완료", now_hms());

    let (ws, _) = connect_async(REAL_WS).await.expect("웹소켓 연결 실패");
    println!("[{}] 웹소켓 연결 성공: {REAL_WS}", now_hms());
    let (mut write, mut read) = ws.split();

    let mut stats: BTreeMap<SubId, SubStats> = subs
        .iter()
        .map(|sub| {
            (
                sub.clone(),
                SubStats {
                    ack: None,
                    frames: 0,
                    last_frame: None,
                },
            )
        })
        .collect();

    // 전송 실패나 즉시 종료 시에도 서버가 보낸 사유 프레임을 놓치지 않도록
    // 구독 전송과 수신을 병행한다.
    let mut send_queue: std::collections::VecDeque<SubId> = subs.iter().cloned().collect();
    let mut send_timer = tokio::time::interval(SUBSCRIBE_INTERVAL);
    while !send_queue.is_empty() {
        tokio::select! {
            _ = send_timer.tick() => {
                let (tr, key) = send_queue.pop_front().unwrap();
                if let Err(error) = write
                    .send(Message::Text(subscribe_msg(&approval_key, &tr, &key).into()))
                    .await
                {
                    println!("[{}] 구독 전송 실패({tr}/{key}): {error}", now_hms());
                    // 수신 버퍼에 남은 서버 메시지(종료 사유 등)를 마저 읽는다.
                    while let Ok(Some(msg)) =
                        tokio::time::timeout(Duration::from_secs(2), read.next()).await
                    {
                        println!("[{}] 종료 직전 수신: {msg:?}", now_hms());
                    }
                    return;
                }
            }
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(raw))) => {
                        println!("[{}] 구독 중 수신: {}", now_hms(), raw.as_str());
                    }
                    Some(Ok(Message::Close(frame))) => {
                        println!("[{}] 구독 중 서버 종료: {frame:?}", now_hms());
                        return;
                    }
                    other => println!("[{}] 구독 중 수신(기타): {other:?}", now_hms()),
                }
            }
        }
    }
    println!("[{}] 구독 전송 완료 — {observe_secs}초 관찰 시작", now_hms());

    let deadline = Instant::now() + Duration::from_secs(observe_secs);
    let mut summary_timer = tokio::time::interval(SUMMARY_INTERVAL);
    summary_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    summary_timer.tick().await; // 첫 즉시 틱 소비

    loop {
        let msg = tokio::select! {
            _ = tokio::time::sleep_until(deadline.into()) => break,
            _ = summary_timer.tick() => {
                print_summary(&stats);
                continue;
            }
            msg = read.next() => msg,
        };
        let Some(Ok(msg)) = msg else {
            println!("[{}] 웹소켓 스트림 종료/오류: {msg:?}", now_hms());
            break;
        };
        match msg {
            Message::Text(raw) => {
                let txt = raw.as_str();
                if txt.starts_with('{') {
                    let v: serde_json::Value = match serde_json::from_str(txt) {
                        Ok(v) => v,
                        Err(_) => {
                            println!("[{}] JSON 파싱 불가: {txt}", now_hms());
                            continue;
                        }
                    };
                    if v["header"]["tr_id"].as_str() == Some("PINGPONG") {
                        let _ = write.send(Message::Text(raw.clone())).await;
                        println!("[{}] PINGPONG 에코", now_hms());
                        continue;
                    }
                    println!("[{}] ack 원문: {txt}", now_hms());
                    let tr = v["header"]["tr_id"].as_str().unwrap_or("?").to_string();
                    let key = v["header"]["tr_key"].as_str().unwrap_or("?").to_string();
                    let rt_cd = v["body"]["rt_cd"].as_str().unwrap_or("?").to_string();
                    let msg1 = v["body"]["msg1"].as_str().unwrap_or("").trim().to_string();
                    if let Some(stat) = stats.get_mut(&(tr, key)) {
                        stat.ack = Some((rt_cd, msg1));
                    }
                } else {
                    let mut parts = txt.splitn(4, '|');
                    let (_flag, tr, _cnt, payload) = (
                        parts.next().unwrap_or(""),
                        parts.next().unwrap_or("?"),
                        parts.next().unwrap_or(""),
                        parts.next().unwrap_or(""),
                    );
                    let code = payload.split('^').next().unwrap_or("?");
                    if let Some(stat) = stats.get_mut(&(tr.to_string(), code.to_string())) {
                        if stat.frames == 0 {
                            println!("[{}] 첫 데이터 수신: {tr}/{code}", now_hms());
                        }
                        stat.frames += 1;
                        stat.last_frame = Some(Instant::now());
                    } else {
                        println!("[{}] 요청 외 데이터 프레임: {tr}/{code}", now_hms());
                    }
                }
            }
            Message::Ping(payload) => {
                let _ = write.send(Message::Pong(payload)).await;
            }
            Message::Close(frame) => {
                println!("[{}] 서버가 연결을 종료: {frame:?}", now_hms());
                break;
            }
            _ => {}
        }
    }

    println!("=== 최종 결과 ===");
    print_summary(&stats);
    let silent: Vec<String> = stats
        .iter()
        .filter(|(_, stat)| stat.frames == 0)
        .map(|((tr, key), _)| format!("{tr}/{key}"))
        .collect();
    if silent.is_empty() {
        println!("모든 구독에서 데이터 수신됨");
    } else {
        println!("데이터 0건 구독 (장중이라면 문제): {}", silent.join(", "));
    }
}
