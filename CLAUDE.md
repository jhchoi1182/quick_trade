# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

QuickTrade — 단일종목 ETF(레버리지/곱버스) 스캘핑용 원클릭 트레이딩 데스크톱 앱.
본주 차트를 보다가 **클릭 한 번에 최대 수량 매수 / 보유 전량 매도**. Tauri 2(Rust) + React + TypeScript, Windows/macOS.
코드·주석·커밋 메시지는 한국어로 작성한다.

## 명령어

```bash
npm install
npm run tauri dev      # 개발 실행 (KIS 키 없으면 자동 데모 모드)
npm run tauri build    # 배포 빌드 (.msi / .dmg는 각 OS에서)

npm test               # 프론트 테스트 (vitest, 1회 실행)
npx vitest run src/lib/candles.test.ts   # 단일 프론트 테스트 파일
npm run build          # 타입체크(tsc) + vite 빌드

cd src-tauri && cargo test                          # Rust 전체 테스트
cd src-tauri && cargo test --test demo_trading      # 통합 테스트만
cd src-tauri && cargo test buy_limit                # 이름으로 단일 테스트
```

- 개발 서버 포트 **41730(dev) / 41731(HMR)** 은 회사 프로젝트와 안 겹치게 예약한 값 — **바꾸지 말 것** (`vite.config.ts`, `tauri.conf.json`).
- 앱은 `tauri-plugin-single-instance`로 이중 실행이 차단된다 (이중 주문 방지). 두 번째 실행은 기존 창만 포커스.
- Windows에서 `cargo test`가 되는 건 `build.rs`가 `tests.manifest`(comctl32 v6)를 테스트 exe에 임베드해 주기 때문이다. 이 두 파일을 지우거나 링크 인자를 건드리면 테스트가 STATUS_ENTRYPOINT_NOT_FOUND(0xc0000139)로 시작조차 못 한다.

## 아키텍처

### 브로커 추상화가 핵심 seam
`broker.rs`의 `Broker` trait이 실전/모의(KIS)와 데모(Mock)를 통일한다. 엔진·커맨드는 이 trait에만 의존하므로 새 기능은 trait에 메서드를 추가하고 `kis/mod.rs`(`KisBroker`)와 `mock.rs`(`MockBroker`) 양쪽에 구현한다.

- `TradeMode::Demo` → `MockBroker` (가상 시세 + 모의 체결, KIS 연결 없음)
- `TradeMode::Real` / `Paper` → `KisBroker` (실제 KIS REST + WebSocket)

### 엔진 생명주기와 재시작 판정 (가장 중요한 규칙)
`engine.rs`의 `Engine`은 브로커 + 시세/잔고 캐시 + 백그라운드 태스크(WS 피드 소비, 주기 잔고 갱신, 차트 워밍업)를 소유한다. `EngineHandle`이 drop되면 모든 태스크가 abort된다.

**설정 저장 시 엔진을 재시작할지 여부가 유량 초과(EGW00201)의 근본 원인이었다.** `commands.rs`의 `needs_engine_restart`가 이를 판정한다:
- **브로커 관련 필드**(app_key/secret, cano, acnt_prdt_cd, hts_id, mode, exchange, 종목 코드) 변경 → 엔진 재시작
- **UI 전용 필드**(opacity, chart_interval, theme, 라벨) 변경 → 재시작 없이 `Engine::update_settings`로 반영만

새 설정 필드를 추가할 때 이 함수를 반드시 갱신하라. UI 필드를 재시작 목록에 넣으면 슬라이더 드래그마다 WS 재접속 + 백필 버스트가 나 유량 초과가 재발한다. (`update_settings`가 읽는 필드는 `Engine.settings`가 `RwLock`인 이유이기도 하다.)

### 유량 제어 (KIS 게이트웨이 방어) — 함부로 완화하지 말 것
KIS는 초당 거래건수를 고정 초 단위로 집계하므로 버스트에 취약하다. 다층 방어가 있다:
1. `rest.rs`의 `RateLimiter` — **균일 간격** 리미터(실전 90ms, 모의 550ms). 슬라이딩 윈도우가 아니라 호출 사이 최소 간격을 강제해 초 경계 버스트를 원천 차단.
2. GET(읽기 전용)은 EGW00201 시 지수 백오프(0.5→1→2→4s) 4회 재시도. POST(주문)는 재시도하지 않는다.
3. `candle_cache.rs` — 종목별 1분봉 캐시(TTL 60s) + **단일 비행(single-flight)**. 동시 요청은 실제 백필 1회로 합쳐진다.
4. `quote.rs`의 백필은 페이지당 `BACKFILL_PAGE_DELAY`(40ms)를 더해 첫 로드 폭주를 막는다.

유량 초과가 재발하면 1차 조치는 `rest.rs`의 gap_ms 증가.

### 데이터 흐름
- **실시간**: `kis/ws.rs`(또는 mock 피드)가 파이프 문자 프레임을 파싱 → `FeedEvent`(Quote/Book/Fill/Conn) → `mpsc` 채널 → `engine.rs::consume_feed` → Tauri `emit` → 프론트 `useTauriEvents.ts` → zustand 스토어.
- **요청/응답**: 프론트 `lib/tauri.ts`의 `invoke` → `commands.rs`의 `#[tauri::command]` → 엔진 → 브로커 REST. 차트(get_candles)·잔고(get_account)·주문(buy_max/sell_all)이 이 경로.
- 커맨드 등록은 `lib.rs`의 `invoke_handler`에 있다. 새 커맨드는 여기 추가.

### 주문 규약 (사용자 확정 설계)
- **매수**: 현재가 +3% IOC지정가(`util.rs::BUY_PREMIUM_PCT`), 예수금의 95%(`util.rs::CASH_USE_RATIO`) 기준 최대 수량. 프리미엄 덕에 급등 중에도 즉시 전량 체결되고, 미체결 잔량은 거래소가 즉시 자동취소 → 호가창에 안 쌓임. **모의투자는 IOC 미지원이라 일반 지정가로 대체**(`engine.rs`에서 `mode==Paper`면 `ioc=false`).
- **매수 수량의 예수금 기준은 미수없는매수금액(`nrcvb_buy_amt`)** — `ord_psbl_cash`는 장중 매도 반복 시 미정산 매도대금 탓에 KIS의 실제 허용액과 어긋나 "주문가능금액 초과" 거부가 난다(`kis/account.rs`). 그래도 거부되면 KIS 매수가능수량(`nrcvb_buy_qty`)으로 **1회 자동 재주문**한다(`engine.rs::retry_buy_with_psbl`) — 첫 주문이 확정 거부된 뒤라 이중 주문 위험 없음.
- **매도**: 보유 전량 시장가.
- **강제 손절(기계적 강제, 사용자 설정 불가)**: 실시간 체결가 기준 수익률이 `util.rs::STOP_LOSS_PCT`(-0.6%) **이하**로 떨어지면 `engine.rs::consume_feed`가 매 틱 감시하다 보유 전량 시장가로 자동 매도한다(`arm_stop_loss_if_breached`→`force_stop_loss`). 판정 기준은 화면 표시 수익률과 같은 `(price/avg-1)*100`(`util.rs::pnl_rate`). 발동 후 `STOP_LOSS_RELOCK`(5초) 잠금으로 연속 틱 중복 매도·거부 시 주문 폭주를 막고, 잔고에서 포지션이 사라지거나 재매수하면 잠금이 풀려 재무장한다. **의도적으로 Settings/UI에 노출하지 않는다** — 상수를 바꾸는 것 외엔 못 건드린다. 모든 모드 공통(데모 포함)이라 실전 전 데모로 검증된다.
- **오클릭 방지·확인 절차 없음** (속도 우선, 의도된 설계). 실전 모드는 클릭 = 즉시 주문.
- KIS TR ID는 신형 체계: 매수 `TTTC0012U`/매도 `TTTC0011U`(모의는 `VTTC…`). 주문구분 `00`지정가/`01`시장가/`11`IOC지정가 (`kis/order.rs`).

### 시장/모드 차이
| | 시세 TR | 시장 구분 | 주문 거래소 |
|---|---|---|---|
| Real | 차트 전용 H0UN~ (통합) / **매매 종목 H0ST~ (KRX)** | UN (프리 08시~애프터 20시) | 설정값 KRX/SOR |
| Paper | H0ST~ (KRX) | J (09:00~15:30) | KRX 강제 |
| Demo | mock | — | — |

당일·과거 분봉 모두 통합(UN) 지원이 불확실해 실패 시 KRX(J)로 폴백한다(`quote.rs`). **실시간 웹소켓도 같은 이유로 매매 종목(신형 코드 ETF/ETN)은 실전에서도 KRX 단독 TR로 구독한다**(`kis/mod.rs::build_subs`) — 통합 TR이 시세를 안 내려주면 보유 수익률 틱 갱신이 끊긴다. 같은 종목을 두 TR로 겹쳐 구독하면 차트 거래량이 이중 집계되므로 코드당 체결가 구독은 1회.

### 시간 규약 — "가짜 epoch"
`Candle.time` / `Quote.ts`는 **KST 벽시계 시각을 UTC epoch처럼 취급한 가짜 초**다(`util.rs::now_kst_fake_epoch`). lightweight-charts가 UTC로 렌더링하므로 이렇게 해야 한국 시간이 그대로 표시된다. 새 시각 필드도 이 규약을 따르고 `util.rs`의 헬퍼를 쓸 것.

### 종목 코드
신형 코드는 **영문 포함 6자리**(예: `0193T0`). 숫자만 가정하는 파싱·검증 금지. ETF/ETN은 호가단위 5원 고정(`SymbolConfig.etf`, `util.rs::tick_size`).

### 프론트엔드
- zustand 스토어: `marketStore`(시세), `accountStore`(잔고/연결), `settingsStore`(설정, 백엔드 동기화), `uiStore`(셀렉터/토스트/창 상태, localStorage 영속).
- `MiniChart.tsx`: 백엔드에서 1분봉을 받아 `lib/candles.ts::aggregate`로 10/15분봉 집계 + 실시간 틱으로 마지막 봉/이동평균(5/20/60/120) 갱신. 종목별 1분봉을 프론트에서도 캐시(stale-while-revalidate)해 유량 절약.
- 설정 저장은 두 갈래: `save()`는 즉시, opacity/chartInterval 같은 UI 값은 `scheduleSilentSave`(400ms 디바운스)로 조용히 저장. 디바운스 콜백은 예약 시점 객체가 아니라 **발사 시점의 최신 스토어 상태**를 저장한다(모달 저장 되덮기 방지).
- 타입은 Rust `types.rs`(serde `camelCase`)와 TS `types.ts`가 손으로 맞춰져 있다. 한쪽을 바꾸면 다른 쪽도 갱신.

## 설정 저장 / 보안
- 설정 파일: `%APPDATA%\quick-trade\config.json` (macOS `~/Library/Application Support/quick-trade/`), 토큰은 같은 폴더 `token.json`. 파일이 없거나 깨지면 기본값(**데모 모드**)으로 로드(`config.rs`).
- **키를 평문 저장**한다 (편의 > 보안, 사용자 확정). 공용 PC 금지. 보안 강화 리팩터를 임의로 하지 말 것.
- `Settings`에 필드 추가 시 `#[serde(default = ...)]`로 구버전 config.json 하위 호환을 지켜라 (`types.rs`의 테스트가 이를 검증).

## 실가동 검증 체크리스트 (모의/실전 첫 가동 시)
데모까지만 검증된 상태다. KIS 키 발급 후 처음 붙일 때 아래를 확인하라:
1. **실전 당일 분봉 UN 지원 여부** — 미지원이면 자동으로 J 폴백된다(`quote.rs`). 로그에 "당일 분봉 통합(UN) 조회 실패" 경고가 반복되면 `today_minutes`를 J 기본으로 바꿔 호출 낭비를 없앨 것.
2. **정규장 외 시간(NXT 프리 08시대/애프터 16~20시) 주문** — 시장가(`01`)·IOC(`11`)는 정규장 외 시간·SOR에서 거부될 수 있다. 애프터마켓 매도가 실패하면 시간대별로 지정가 주문으로 분기하는 로직이 필요하다.
3. **매수 거부 시 자동 재주문 동작** — "주문가능금액 부족/초과" 거절은 KIS 매수가능수량으로 1회 재주문된다(`engine.rs::retry_buy_with_psbl`, 로그 "매수 거부 → …재주문"). 재주문까지 반복 실패하면 그때 `util.rs::CASH_USE_RATIO`(0.95)를 낮춰라.
4. **PC 시계 동기화** — 시세 신선도 판정(`engine.rs::QUOTE_FRESH_SECS`)이 거래소 체결시각과 로컬 시계를 비교하므로, 시계가 수십 초 틀어지면 주문마다 불필요한 REST 스냅샷 폴백이 발생한다.
5. **웹소켓 구독 실패 로그** — 접속은 됐는데 시세가 없으면 "웹소켓 구독 실패" 경고 로그부터 확인(`ws.rs`). 종목별로 "실시간 체결가 첫 수신" info 로그(`engine.rs::consume_feed`)가 찍히는지 보면 어떤 코드가 실제 시세를 받는지 즉시 판별된다.
