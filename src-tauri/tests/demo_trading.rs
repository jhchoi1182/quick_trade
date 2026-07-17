//! 데모 모드 매매 루프 통합 테스트: 주문 → 체결 이벤트 → 잔고/포지션 반영

use quick_trade_lib::broker::Broker;
use quick_trade_lib::mock::MockBroker;
use quick_trade_lib::types::{FeedEvent, Settings, Side};
use quick_trade_lib::util::{buy_limit_price, max_buy_qty};

#[tokio::test]
async fn demo_buy_then_sell_roundtrip() {
    let settings = Settings::default();
    let broker = MockBroker::new(&settings);
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let handles = broker.start_feed(settings.all_codes(), tx).await.unwrap();

    // 피드 시작 알림 수신
    let first = rx.recv().await.unwrap();
    assert!(matches!(first, FeedEvent::Conn(true)));

    let code = "0193T0";
    let quote = broker.snapshot(code).await.unwrap();
    assert!(quote.ask1 > 0.0);

    // 엔진과 동일한 산식으로 최대 수량 매수
    let account0 = broker.account().await.unwrap();
    let limit = buy_limit_price(quote.ask1 as u64, settings.buffer_ticks, true);
    let qty = max_buy_qty(account0.cash, limit);
    assert!(qty > 0);

    broker.place_buy(code, qty, limit, true).await.unwrap();

    // Fill 이벤트가 와야 한다 (피드 Quote들 사이에 섞여 옴)
    let fill = wait_for_fill(&mut rx).await;
    assert_eq!(fill.code, code);
    assert!(matches!(fill.side, Side::Buy));
    assert_eq!(fill.qty, qty);

    // 잔고 반영: 포지션 생성 + 현금 감소
    let account1 = broker.account().await.unwrap();
    assert!(account1.cash < account0.cash);
    let pos = account1.positions.iter().find(|p| p.code == code).unwrap();
    assert_eq!(pos.qty, qty);

    // 전량 매도
    broker.place_sell_market(code, qty).await.unwrap();
    let fill = wait_for_fill(&mut rx).await;
    assert!(matches!(fill.side, Side::Sell));
    assert_eq!(fill.qty, qty);

    let account2 = broker.account().await.unwrap();
    assert!(account2.positions.iter().all(|p| p.code != code));
    assert!(account2.cash > account1.cash);

    for h in handles {
        h.abort();
    }
}

#[tokio::test]
async fn demo_ioc_below_ask_does_not_fill() {
    let settings = Settings::default();
    let broker = MockBroker::new(&settings);
    let code = "0193T0";
    let quote = broker.snapshot(code).await.unwrap();

    // 매도1호가보다 낮은 지정가 IOC → 미체결, 잔고 불변
    let ack = broker
        .place_buy(code, 10, quote.bid1 as u64, true)
        .await
        .unwrap();
    assert!(ack.message.contains("미체결"));

    let account = broker.account().await.unwrap();
    assert_eq!(account.cash, 10_000_000);
    assert!(account.positions.is_empty());
}

async fn wait_for_fill(rx: &mut tokio::sync::mpsc::Receiver<FeedEvent>) -> quick_trade_lib::types::FillEvent {
    for _ in 0..200 {
        match tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv()).await {
            Ok(Some(FeedEvent::Fill(f))) => return f,
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    panic!("Fill 이벤트를 받지 못함");
}
