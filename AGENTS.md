# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

EasyScalping — 단일종목 ETF(레버리지/곱버스) 스캘핑용 원클릭 트레이딩 데스크톱 앱.
본주 차트를 보다가 **클릭 한 번에 최대 수량 매수 / 보유 전량 매도**. Tauri 2(Rust) + React + TypeScript, Windows/macOS.
코드·주석·커밋 메시지는 한국어로 작성한다.

## 명령어

```bash
npm install
npm run tauri dev      # 개발 실행 (KIS 실전 설정·확인 필요)
npm run tauri build    # 배포 빌드 (.msi / .dmg는 각 OS에서)

npm test               # 프론트 테스트 (vitest, 1회 실행)
npx vitest run src/lib/candles.test.ts   # 단일 프론트 테스트 파일
npm run build          # 타입체크(tsc) + vite 빌드

cd src-tauri && cargo test                          # Rust 전체 테스트
cd src-tauri && cargo test buy_limit                # 이름으로 단일 테스트
```

- 개발 서버 포트 **41730(dev) / 41731(HMR)** 은 회사 프로젝트와 안 겹치게 예약한 값 — **바꾸지 말 것** (`vite.config.ts`, `tauri.conf.json`).
- 앱은 `tauri-plugin-single-instance`로 이중 실행이 차단된다 (이중 주문 방지). 두 번째 실행은 기존 창만 포커스.
- Windows에서 `cargo test`가 되는 건 `build.rs`가 `tests.manifest`(comctl32 v6)를 테스트 exe에 임베드해 주기 때문이다. 이 두 파일을 지우거나 링크 인자를 건드리면 테스트가 STATUS_ENTRYPOINT_NOT_FOUND(0xc0000139)로 시작조차 못 한다.

## 아키텍처

### 브로커 추상화가 핵심 seam
`broker.rs`의 `Broker` trait이 KIS 실전 연결을 추상화한다. 제품 런타임은 `KisBroker`만 사용하며 `mock.rs`는 `#[cfg(test)]` 테스트 더블이다. 거래 제어는 브로커 환경과 별개인 `ControlMode::Manual | Auto | Shadow`로 나뉜다.

### 엔진 생명주기와 재시작 판정 (가장 중요한 규칙)
`engine.rs`의 `Engine`은 브로커 + 시세/잔고 캐시 + 백그라운드 태스크(WS 피드 소비, 주기 잔고 갱신, 차트 워밍업)를 소유한다. `EngineHandle`이 drop되면 모든 태스크가 abort된다.

**설정 저장 시 엔진을 재시작할지 여부가 유량 초과(EGW00201)의 근본 원인이었다.** `commands.rs`의 `needs_engine_restart`가 이를 판정한다:
- **브로커 관련 필드**(app_key/secret, cano, acnt_prdt_cd, hts_id, 실전확인, exchange, 종목 코드) 변경 → 엔진 재시작
- **UI 전용 필드**(opacity, chart_interval, theme, 라벨) 변경 → 재시작 없이 `Engine::update_settings`로 반영만

새 설정 필드를 추가할 때 이 함수를 반드시 갱신하라. UI 필드를 재시작 목록에 넣으면 슬라이더 드래그마다 WS 재접속 + 백필 버스트가 나 유량 초과가 재발한다. (`update_settings`가 읽는 필드는 `Engine.settings`가 `RwLock`인 이유이기도 하다.)

### 유량 제어 (KIS 게이트웨이 방어) — 함부로 완화하지 말 것
KIS는 초당 거래건수를 고정 초 단위로 집계하므로 버스트에 취약하다. 다층 방어가 있다:
1. `rest.rs`의 `RateLimiter` — 실전 **120ms 균일 간격** 리미터 + 유량 초과 시 전역 쿨다운. 슬라이딩 윈도우가 아니라 호출 사이 최소 간격을 강제하고, 한 요청이 백오프하는 동안 다른 GET/POST도 함께 멈춰 버스트를 차단.
2. GET(읽기 전용)은 EGW00201 시 지수 백오프(0.5→1→2→4s) 4회 재시도. POST(주문)는 재시도하지 않으며, 게이트웨이가 명시적으로 돌려준 EGW00201만 확정 미접수로 종결한다. 전송 타임아웃·주문번호 누락은 계속 불명확 상태로 조정한다.
3. `market_history.rs` — 최초/실제 연결 공백에만 백필하는 1분봉 `MarketHistory` + **단일 비행(single-flight)**. 정상 연결 중에는 실시간 체결로 갱신한다.
4. `quote.rs`의 백필은 페이지당 `BACKFILL_PAGE_DELAY`(40ms)를 더해 첫 로드 폭주를 막는다.

유량 초과가 재발하면 1차 조치는 `rest.rs`의 gap_ms 증가.

### 데이터 흐름
- **실시간**: `kis/ws.rs`가 파이프 문자 프레임을 파싱 → `FeedEvent`(Quote/Book/Fill/Conn) → `mpsc` 채널 → `engine.rs::consume_feed` → Tauri `emit` → 프론트 `useTauriEvents.ts` → zustand 스토어.
- **요청/응답**: 프론트 `lib/tauri.ts`의 `invoke` → `commands.rs`의 `#[tauri::command]` → 엔진 → 브로커 REST. 차트(get_candles)·잔고(get_account)·주문(buy_max/sell_all)이 이 경로.
- 커맨드 등록은 `lib.rs`의 `invoke_handler`에 있다. 새 커맨드는 여기 추가.

### 자동매매(Auto/Shadow) — OpenAI 기반 OCO 판단 엔진
`src-tauri/src/automation/`이 Manual 위에 얹히는 두 번째 매매 계층이다. Auto는 판단대로 KIS에 실주문을 내고, Shadow는 같은 판단·체결 로직을 브로커 접근이 아예 없는 페이퍼 시뮬레이터(`shadow.rs`)로 돌린다(수동 실주문과 동시 실행 가능). 모듈 분담:
- `oco.rs` — 네트워크·시계·주문 실행을 소유하지 않는 순수 상태 머신. 호출자가 단조 시간과 체결 순번을 주입.
- `openai.rs` — OpenAI Responses API 클라이언트. `MODEL`(`gpt-5.6-sol`)과 `SYSTEM_PROMPT`/`PROMPT_VERSION`을 상수로 고정 — **`SYSTEM_PROMPT`에 동적 값을 넣지 말 것**, 이 고정 접두사가 프롬프트 캐시 재사용의 키다.
- `runtime.rs`의 `AutomationRuntime` — mode/phase/OCO 그룹/포지션을 한 잠금 안에서 소유하는 단일 소유자. 모든 상태 전이는 `revision` 카운터로 보호되어, 모드 전환 뒤 늦게 돌아온 이전 LLM 응답 같은 낡은 콜백이 최신 상태를 덮어쓰지 못한다.
- `schedule.rs` — KST 5분 정규 슬롯(09:05~15:05), 시나리오 만료(다음 슬롯 또는 15:10 중 이른 쪽), 15:15 강제 청산 경계.
- `shadow.rs` — 실계좌·브로커 객체를 아예 받지 않는 섀도 체결 시뮬레이터.

판단 주기: `engine.rs::automation_scheduler`가 1초마다 `automation_tick`을 호출하고, Auto·Shadow가 무포지션인 정규 슬롯마다 `run_llm_decision`이 `build_llm_input`으로 1·3·5·15분봉 지표 + `chart_image.rs`가 합성한 PNG 차트 + 본주/레버리지/곱버스 호가 스냅샷을 모아 `openai::OpenAiClient::decide`를 호출한다. 모델은 `ModelDecision`(시장 국면 + 상품당 최대 1개, 총 2개 이하의 추세/반전 시나리오)을 반환하고, `oco::validate_decision`이 가격 순서·최소 확인거리(10bp)·목표수익률 범위를 의미 검증한 뒤 본주 호가단위로 정규화한다. `OcoGroup`은 확인가를 3초·3틱 연속 충족해야만 발동하는 OCO(둘 중 하나만 승리)로 무장한다.

Auto 진입은 KIS POST 전에 `PendingAutoEntry`/`OwnedPosition`을 먼저 저장해 재시작 중 크래시도 `ledger.rs`의 `orders` 테이블(주문 신원의 소스 오브 트루스)로 조정할 수 있게 한다. Auto·Shadow 전용 강제 청산(기계적, Settings 미노출)은 손절 `-0.3%`, 목표수익률이 `0.3%`를 넘을 때만 무장되는 수익보호선 `+0.3%`(무장 후 되밀리면 청산), 첫 체결 후 최대 보유 10분(`MAX_HOLD_SECONDS`), 15:15 강제 청산이며, 여러 사유가 겹치면 `market_close > stop_loss > profit_guard > max_holding` 순으로 승격한다. Auto 매매 종목은 SK하이닉스 000660(본주)/0193T0(레버리지)/0197X0(곱버스)로 고정되어 `commands.rs::save_settings`가 `auto_symbols` 변경 저장 자체를 거부한다.

`ledger.rs`는 SQLite(`trading.db`)에 `sessions`/`llm_decisions`/`decision_scenarios`/`orders`/`fills`/`fill_notices`/`trades`/`runtime_state` 테이블을 유지한다. 재시작 후 주문·포지션 조정과 프론트 거래/LLM 판단 기록 조회(`list_trade_records`/`list_llm_decisions`)의 근거가 이 장부다.

### 주문 규약 (사용자 확정 설계)
- **매수**: 현재가 +3% IOC지정가(`util.rs::BUY_PREMIUM_PCT`), 예수금의 95%(`util.rs::CASH_USE_RATIO`) 기준 최대 수량. 프리미엄 덕에 급등 중에도 즉시 체결을 시도하고, 미체결 잔량은 거래소가 즉시 자동취소 → 호가창에 안 쌓임.
- **매수 수량의 예수금 기준은 미수없는매수금액(`nrcvb_buy_amt`)** — `ord_psbl_cash`는 장중 매도 반복 시 미정산 매도대금 탓에 KIS의 실제 허용액과 어긋나 "주문가능금액 초과" 거부가 난다(`kis/account.rs`). 그래도 거부되면 KIS 매수가능수량(`nrcvb_buy_qty`)으로 **1회 자동 재주문**한다(`engine.rs::retry_buy_with_psbl`) — 첫 주문이 확정 거부된 뒤라 이중 주문 위험 없음.
- **매도**: 보유 전량 시장가.
- **강제 손절(기계적 강제, 사용자 설정 불가)**: Manual은 실시간 체결가 기준 단순 수익률 `-0.6%`, Auto·Shadow는 `-0.3%` 이하에서 청산한다. 판정 기준은 화면과 같은 `(price/avg-1)*100`이며 수수료·세금을 섞지 않는다. Manual의 연속 틱 중복 매도는 `STOP_LOSS_RELOCK`(5초)으로 막고, Auto·Shadow는 각 상태 엔진의 단일 청산 전환으로 직렬화한다. **의도적으로 Settings/UI에 노출하지 않는다.**
- **오클릭 방지·확인 절차 없음** (속도 우선, 의도된 설계). 실전 모드는 클릭 = 즉시 주문.
- KIS TR ID는 신형 실전 체계: 매수 `TTTC0012U`/매도 `TTTC0011U`. 주문구분 `00`지정가/`01`시장가/`11`IOC지정가 (`kis/order.rs`).

### 시장/모드 차이

KIS 실전만 연결하며 차트 전용 종목은 H0UN~(통합), 매매 종목은 H0ST~(KRX) 시세를 사용한다. 주문 거래소는 설정값 KRX/SOR이다. Manual/Auto/Shadow는 같은 실전 시세·계좌 위의 제어 모드이며 Shadow만 주문 POST를 타입 수준에서 차단한다(자동매매 판단 로직 자체는 위 "자동매매(Auto/Shadow)" 절 참고).

당일·과거 분봉 모두 통합(UN) 지원이 불확실해 실패 시 KRX(J)로 폴백한다(`quote.rs`). **실시간 웹소켓도 같은 이유로 매매 종목(신형 코드 ETF/ETN)은 실전에서도 KRX 단독 TR로 구독한다**(`kis/mod.rs::build_subs`) — 통합 TR이 시세를 안 내려주면 보유 수익률 틱 갱신이 끊긴다. 같은 종목을 두 TR로 겹쳐 구독하면 차트 거래량이 이중 집계되므로 코드당 체결가 구독은 1회.

### 시간 규약 — "가짜 epoch"
`Candle.time` / `Quote.ts`는 **KST 벽시계 시각을 UTC epoch처럼 취급한 가짜 초**다(`util.rs::now_kst_fake_epoch`). lightweight-charts가 UTC로 렌더링하므로 이렇게 해야 한국 시간이 그대로 표시된다. 새 시각 필드도 이 규약을 따르고 `util.rs`의 헬퍼를 쓸 것.

### 종목 코드
신형 코드는 **영문 포함 6자리**(예: `0193T0`). 숫자만 가정하는 파싱·검증 금지. ETF/ETN은 호가단위 5원 고정(`SymbolConfig.etf`, `util.rs::tick_size`).

### 프론트엔드
- zustand 스토어: `marketStore`(시세), `accountStore`(잔고/연결), `settingsStore`(설정, 백엔드 동기화), `uiStore`(셀렉터/토스트/창 상태, localStorage 영속), `automationStore`(자동매매 스냅샷).
- `MiniChart.tsx`: 백엔드에서 1분봉을 받아 `lib/candles.ts::aggregate`로 10/15분봉 집계 + 실시간 틱으로 마지막 봉/이동평균(5/20/60/120) 갱신. 종목별 1분봉을 프론트에서도 캐시(stale-while-revalidate)해 유량 절약.
- `automationStore.ts`/`AutomationPanel.tsx`: `get_automation_status` 폴링 + `automation-state` emit을 함께 받는다. `keepNewest`가 `runtimeGeneration` 다음 `revision` 순으로 비교해, 이벤트 순서가 뒤바뀌어도 오래된 스냅샷이 최신 상태를 덮어쓰지 않게 한다. `automationPresentation.ts`는 phase/시나리오 상태를 화면 문구로 변환하고, `llmUsage.ts`는 OpenAI 토큰 사용량·비용을 계산한다.
- 설정 저장은 두 갈래: `save()`는 즉시, opacity/chartInterval 같은 UI 값은 `scheduleSilentSave`(400ms 디바운스)로 조용히 저장. 디바운스 콜백은 예약 시점 객체가 아니라 **발사 시점의 최신 스토어 상태**를 저장한다(모달 저장 되덮기 방지).
- 타입은 Rust `types.rs`(serde `camelCase`)와 TS `types.ts`가 손으로 맞춰져 있다. 한쪽을 바꾸면 다른 쪽도 갱신.

## 설정 저장 / 보안
- 설정 파일: `%APPDATA%\easy-scalping\config.json` (macOS `~/Library/Application Support/easy-scalping/`), 토큰은 같은 폴더 `token.json`, 거래 장부는 `trading.db`. 파일이 없거나 깨지면 수동 모드·실전 미확인 기본값으로 로드한다.
- **키를 평문 저장**한다 (편의 > 보안, 사용자 확정). KIS 키뿐 아니라 OpenAI API 키(`Settings.openai_api_key`, Auto/Shadow 판단용)도 같은 정책으로 평문 저장된다. 공용 PC 금지. 보안 강화 리팩터를 임의로 하지 말 것.
- `Settings`에 필드 추가 시 `#[serde(default = ...)]`로 구버전 config.json 하위 호환을 지켜라 (`types.rs`의 테스트가 이를 검증).

## 실가동 검증 체크리스트

KIS 키 발급 후 처음 붙이거나 장중 Shadow를 시운전할 때 아래를 확인하라:
1. **실전 당일 분봉 UN 지원 여부** — 미지원이면 자동으로 J 폴백된다(`quote.rs`). 로그에 "당일 분봉 통합(UN) 조회 실패" 경고가 반복되면 `today_minutes`를 J 기본으로 바꿔 호출 낭비를 없앨 것.
2. **정규장 외 시간(NXT 프리 08시대/애프터 16~20시) 주문** — 시장가(`01`)·IOC(`11`)는 정규장 외 시간·SOR에서 거부될 수 있다. 애프터마켓 매도가 실패하면 시간대별로 지정가 주문으로 분기하는 로직이 필요하다.
3. **매수 거부 시 자동 재주문 동작** — "주문가능금액 부족/초과" 거절은 KIS 매수가능수량으로 1회 재주문된다(`engine.rs::retry_buy_with_psbl`, 로그 "매수 거부 → …재주문"). 재주문까지 반복 실패하면 그때 `util.rs::CASH_USE_RATIO`(0.95)를 낮춰라.
4. **PC 시계 동기화** — 시세 신선도 판정(`engine.rs::QUOTE_FRESH_SECS`)이 거래소 체결시각과 로컬 시계를 비교하므로, 시계가 수십 초 틀어지면 주문마다 불필요한 REST 스냅샷 폴백이 발생한다.
5. **웹소켓 구독 실패 로그** — 접속은 됐는데 시세가 없으면 "웹소켓 구독 실패" 경고 로그부터 확인(`ws.rs`). 종목별로 "실시간 체결가 첫 수신" info 로그(`engine.rs::consume_feed`)가 찍히는지 보면 어떤 코드가 실제 시세를 받는지 즉시 판별된다.
6. **Auto/Shadow 첫 시운전** — OpenAI API 키가 비었거나 무효면 정규 슬롯마다 판단 실패가 기록된다(`list_llm_decisions`로 확인). Shadow로 먼저 하루 이상 관찰해 시나리오 발동·강제 청산(손절/수익보호/최대보유/15:15)이 기대대로 도는지 본 뒤 Auto로 전환할 것.
