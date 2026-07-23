//! OpenAI Responses API를 이용한 SK하이닉스 OCO 판단 클라이언트.
//!
//! 요청 계약은 이 모듈에 고정한다. 호출 스케줄·상태 revision 확인·시나리오의
//! 방향/호가단위 의미 검증은 상위 자동매매 엔진의 책임이다.

use std::time::{Duration, Instant};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::chart_image::{IndicatorPayload, TimeframeIndicatorPayload};
use crate::types::{MarketRegime, ModelDecision, ModelScenario, ProductKind, Quote, SetupType};

pub const MODEL: &str = "gpt-5.6-sol";
pub const PROMPT_VERSION: &str = "sk-hynix-oco-v4";
const RESPONSES_URL: &str = "https://api.openai.com/v1/responses";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(240);
/// Responses API의 이 한도는 추론 토큰과 가시 출력 토큰을 합쳐서 센다.
/// `reasoning.effort`가 `max`라 추론 몫이 수천 토큰에 이를 수 있고, 한도를 넘기면
/// 응답이 `incomplete(max_output_tokens)`로 잘려 그 슬롯의 판단이 통째로 버려진다.
/// (추론 토큰 요금은 그대로 청구되므로 잘림은 순손실이다.)
/// 구조화 출력 자체는 시나리오 2개 기준 1천 토큰 미만이므로 여유를 크게 둔다.
/// 실제 요청 수명은 상위 엔진의 판단 슬롯 만료와 HTTP 240초 제한 중 먼저 닿는 값이다.
const MAX_OUTPUT_TOKENS: u32 = 32_768;

/// 프롬프트 캐시의 고정 접두사가 되므로 동적 값을 추가하지 않는다.
pub const SYSTEM_PROMPT: &str = concat!(
    "You are an expert discretionary intraday scalper for leveraged and 2x inverse ETFs linked ",
    "to SK hynix (000660). Treat each invocation independently and use only the supplied market ",
    "payload and chart. First determine regime and usable room to the next opposing level from ",
    "the 15- and 5-minute context. Then use the 3- and 1-minute views for entry timing. Judge ",
    "price-volume interaction as a whole: repeated tests of a level, wick response, volume ",
    "exhaustion or expansion, compression, and whether the next candles follow through. A forming ",
    "candle is partial; never compare its raw volume directly with a completed candle. Use its ",
    "formingProgressPct only to put partial volume in context. Consider CONTINUATION and REVERSAL ",
    "setups equally. CONTINUATION means an upward breakout for LEVERAGE or a downward breakdown ",
    "for INVERSE. REVERSAL means a defended support rebound for LEVERAGE or a rejected resistance ",
    "decline for INVERSE. All referencePrice, confirmationPrice, and invalidationPrice values are ",
    "prices of the underlying stock, not ETF prices. Let S be the supplied underlying price, R the ",
    "reference price, C the confirmation price, and I the invalidation price. Required ordering is ",
    "LEVERAGE CONTINUATION I<S<R<C; INVERSE CONTINUATION C<R<S<I; LEVERAGE REVERSAL I<R<S<C; ",
    "INVERSE REVERSAL C<S<R<I. C must be at least 10 basis points beyond R in the entry direction. ",
    "C is the final model-selected confirmation level; the executor adds no percentage buffer. ",
    "Return no scenario for a range midpoint, conflicting timeframes, an already extended move, ",
    "poor room to the next opposing level, weak evidence, or inadequate ETF liquidity. Do not ",
    "force both directions. Return at most one scenario per product and only the strongest valid ",
    "candidates. asOfEpoch, tradeTs, and bookTs share one seconds scale; use them only to judge ",
    "freshness. ETF quotes and level-1 books are for execution feasibility and liquidity only, ",
    "not for inferring market direction. Do not use or infer past decisions, trades, PnL, loss ",
    "streaks, indices, news, disclosures, or deeper order-book levels. targetReturnPct is the ETF ",
    "return target from actual fill, between 0.2 and 2.0 in 0.1 increments; account for the ",
    "product's approximately +2x or -2x exposure without multiplying the target again. Prefer ",
    "0.2 or 0.3 unless the chart clearly offers more room. The executor separately applies a ",
    "-0.3% stop and exits 10 minutes after first fill or at 15:15. Write decisionSummaryKo and ",
    "rationaleKo in Korean. Return only the specified JSON."
);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketQuoteInput {
    pub code: String,
    pub price: f64,
    pub change_rate: f64,
    pub ask1: f64,
    pub bid1: f64,
    pub ask1_qty: u64,
    pub bid1_qty: u64,
    pub spread: f64,
    pub last_trade_volume: f64,
    pub trade_ts: i64,
    pub book_ts: i64,
}

impl From<&Quote> for MarketQuoteInput {
    fn from(quote: &Quote) -> Self {
        Self {
            code: quote.code.clone(),
            price: quote.price,
            change_rate: quote.change_rate,
            ask1: quote.ask1,
            bid1: quote.bid1,
            ask1_qty: quote.ask1_qty,
            bid1_qty: quote.bid1_qty,
            spread: quote.ask1 - quote.bid1,
            last_trade_volume: quote.volume,
            trade_ts: quote.trade_ts,
            book_ts: quote.book_ts,
        }
    }
}

/// 한 번의 독립적인 LLM 판단에 허용된 동적 입력만 담는다.
#[derive(Debug, Clone)]
pub struct DecisionInput {
    pub as_of_kst: String,
    pub underlying: MarketQuoteInput,
    pub leverage: MarketQuoteInput,
    pub inverse: MarketQuoteInput,
    pub indicators: IndicatorPayload,
    pub chart_png: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_write_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
}

#[derive(Debug, Clone)]
pub struct DecisionResult {
    pub response_id: String,
    pub decision: ModelDecision,
    pub usage: TokenUsage,
    pub latency_ms: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum OpenAiError {
    #[error("OpenAI API 키가 비어 있습니다")]
    MissingApiKey,
    #[error("OpenAI 판단 입력이 올바르지 않습니다: {0}")]
    InvalidInput(String),
    #[error("OpenAI 요청 오류: {0}")]
    Http(#[from] reqwest::Error),
    #[error("OpenAI API HTTP 오류({status}): {message}")]
    HttpStatus { status: u16, message: String },
    #[error("OpenAI API 오류: {0}")]
    Api(String),
    #[error("OpenAI가 판단을 거부했습니다: {0}")]
    Refused(String),
    #[error("OpenAI 응답이 완료되지 않았습니다: {0}")]
    Incomplete(String),
    #[error("OpenAI 응답 형식이 올바르지 않습니다: {0}")]
    InvalidResponse(String),
}

/// OpenAI 요청이 실패하더라도 응답에서 확인할 수 있었던 비용 장부 정보는 보존한다.
/// 요청 전 입력 검증 실패처럼 API에 도달하지 않은 오류는 기본값(0/None)을 사용한다.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct DecisionFailure {
    #[source]
    pub error: OpenAiError,
    pub response_id: Option<String>,
    pub usage: TokenUsage,
    pub latency_ms: u64,
}

impl DecisionFailure {
    fn local(error: OpenAiError) -> Self {
        Self {
            error,
            response_id: None,
            usage: TokenUsage::default(),
            latency_ms: 0,
        }
    }

    fn after_request(
        error: OpenAiError,
        value: Option<&Value>,
        latency_ms: u64,
        fallback_response_id: Option<String>,
    ) -> Self {
        Self {
            error,
            response_id: value
                .and_then(response_id_from_value)
                .or(fallback_response_id),
            usage: value.map(usage_from_value).unwrap_or_default(),
            latency_ms,
        }
    }
}

#[derive(Clone)]
pub struct OpenAiClient {
    http: reqwest::Client,
    api_key: String,
}

impl OpenAiClient {
    pub fn new(api_key: impl Into<String>) -> Result<Self, OpenAiError> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(OpenAiError::MissingApiKey);
        }
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .user_agent("easy-scalping/0.1")
            .build()?;
        Ok(Self { http, api_key })
    }

    /// 단 한 번만 요청한다. 429·잘림·거부를 포함한 모든 실패는 호출자에게 반환하며
    /// 이 클라이언트 내부에서는 재시도하지 않는다.
    pub async fn decide(&self, input: &DecisionInput) -> Result<DecisionResult, DecisionFailure> {
        let body = build_request_body(input).map_err(DecisionFailure::local)?;
        let started = Instant::now();
        let response = self
            .http
            .post(RESPONSES_URL)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                DecisionFailure::after_request(
                    OpenAiError::Http(error),
                    None,
                    elapsed_ms(started),
                    None,
                )
            })?;
        let status = response.status();
        let header_response_id = response
            .headers()
            .get("x-request-id")
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned);
        let response_body = response.text().await.map_err(|error| {
            DecisionFailure::after_request(
                OpenAiError::Http(error),
                None,
                elapsed_ms(started),
                header_response_id.clone(),
            )
        })?;
        let latency_ms = elapsed_ms(started);
        let parsed = serde_json::from_str::<Value>(&response_body);

        if !status.is_success() {
            return Err(DecisionFailure::after_request(
                http_status_error(status, &response_body),
                parsed.as_ref().ok(),
                latency_ms,
                header_response_id,
            ));
        }

        let value = parsed.map_err(|error| {
            DecisionFailure::after_request(
                OpenAiError::InvalidResponse(format!("응답 JSON 파싱 실패: {error}")),
                None,
                latency_ms,
                header_response_id.clone(),
            )
        })?;
        parse_response_value(&value, latency_ms).map_err(|mut failure| {
            if failure.response_id.is_none() {
                failure.response_id = header_response_id;
            }
            failure
        })
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QuoteInputs<'a> {
    sk_hynix: &'a MarketQuoteInput,
    leverage_etf: &'a MarketQuoteInput,
    inverse_etf: &'a MarketQuoteInput,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DynamicDecisionInput<'a> {
    as_of_kst: &'a str,
    quotes: QuoteInputs<'a>,
    indicators: &'a IndicatorPayload,
}

/// 장부 해시와 실제 API 본문이 같은 바이트를 사용하도록 동적 텍스트 직렬화를 한곳에 둔다.
pub(crate) fn serialized_dynamic_input(input: &DecisionInput) -> Result<String, OpenAiError> {
    validate_input(input)?;
    serde_json::to_string(&DynamicDecisionInput {
        as_of_kst: &input.as_of_kst,
        quotes: QuoteInputs {
            sk_hynix: &input.underlying,
            leverage_etf: &input.leverage,
            inverse_etf: &input.inverse,
        },
        indicators: &input.indicators,
    })
    .map_err(|error| OpenAiError::InvalidInput(error.to_string()))
}

fn build_request_body(input: &DecisionInput) -> Result<Value, OpenAiError> {
    let dynamic_text = serialized_dynamic_input(input)?;
    let chart_data_url = format!(
        "data:image/png;base64,{}",
        BASE64_STANDARD.encode(&input.chart_png)
    );

    Ok(json!({
        "model": MODEL,
        "reasoning": {
            "effort": "max",
            "context": "current_turn"
        },
        "store": false,
        "text": {
            "verbosity": "low",
            "format": decision_json_schema()
        },
        "max_output_tokens": MAX_OUTPUT_TOKENS,
        "input": [
            {
                "role": "system",
                "content": [{ "type": "input_text", "text": SYSTEM_PROMPT }]
            },
            {
                "role": "user",
                "content": [
                    { "type": "input_text", "text": dynamic_text },
                    { "type": "input_image", "image_url": chart_data_url, "detail": "original" }
                ]
            }
        ]
    }))
}

fn decision_json_schema() -> Value {
    json!({
        "type": "json_schema",
        "name": "sk_hynix_oco_decision",
        "strict": true,
        "schema": {
            "type": "object",
            "properties": {
                "marketRegime": {
                    "type": "string",
                    "enum": ["UPTREND", "DOWNTREND", "RANGE", "TRANSITION", "UNCLEAR"]
                },
                "decisionSummaryKo": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 160
                },
                "scenarios": {
                    "type": "array",
                    "minItems": 0,
                    "maxItems": 2,
                    "items": {
                        "type": "object",
                        "properties": {
                            "product": {
                                "type": "string",
                                "enum": ["LEVERAGE", "INVERSE"]
                            },
                            "setupType": {
                                "type": "string",
                                "enum": ["CONTINUATION", "REVERSAL"]
                            },
                            "referencePrice": {
                                "type": "integer",
                                "exclusiveMinimum": 0
                            },
                            "confirmationPrice": {
                                "type": "integer",
                                "exclusiveMinimum": 0
                            },
                            "invalidationPrice": {
                                "type": "integer",
                                "exclusiveMinimum": 0
                            },
                            "targetReturnPct": {
                                "type": "number",
                                "minimum": 0.2,
                                "maximum": 2.0,
                                "multipleOf": 0.1
                            },
                            "rationaleKo": {
                                "type": "string",
                                "minLength": 1,
                                "maxLength": 120
                            }
                        },
                        "required": [
                            "product",
                            "setupType",
                            "referencePrice",
                            "confirmationPrice",
                            "invalidationPrice",
                            "targetReturnPct",
                            "rationaleKo"
                        ],
                        "additionalProperties": false
                    }
                }
            },
            "required": ["marketRegime", "decisionSummaryKo", "scenarios"],
            "additionalProperties": false
        }
    })
}

fn validate_input(input: &DecisionInput) -> Result<(), OpenAiError> {
    if input.as_of_kst.trim().is_empty() {
        return Err(OpenAiError::InvalidInput("KST 기준시각이 비어 있음".into()));
    }
    validate_quote("SK하이닉스", &input.underlying)?;
    validate_quote("레버리지 ETF", &input.leverage)?;
    validate_quote("곱버스 ETF", &input.inverse)?;
    if input.indicators.as_of_epoch <= 0 {
        return Err(OpenAiError::InvalidInput("지표 기준시각이 0 이하임".into()));
    }
    validate_day(&input.indicators)?;
    validate_indicator("1분봉", &input.indicators.one_minute, 1)?;
    validate_indicator("3분봉", &input.indicators.three_minute, 3)?;
    validate_indicator("5분봉", &input.indicators.five_minute, 5)?;
    validate_indicator("15분봉", &input.indicators.fifteen_minute, 15)?;

    const PNG_SIGNATURE: &[u8; 8] = b"\x89PNG\r\n\x1a\n";
    if !input.chart_png.starts_with(PNG_SIGNATURE) {
        return Err(OpenAiError::InvalidInput(
            "차트 이미지가 PNG 형식이 아님".into(),
        ));
    }
    Ok(())
}

fn validate_quote(label: &str, quote: &MarketQuoteInput) -> Result<(), OpenAiError> {
    let numeric = [
        quote.price,
        quote.change_rate,
        quote.ask1,
        quote.bid1,
        quote.spread,
        quote.last_trade_volume,
    ];
    if quote.code.trim().is_empty() || numeric.iter().any(|value| !value.is_finite()) {
        return Err(OpenAiError::InvalidInput(format!(
            "{label} 시세에 빈 코드 또는 유한하지 않은 값이 있음"
        )));
    }
    if quote.price <= 0.0 || quote.ask1 <= 0.0 || quote.bid1 <= 0.0 {
        return Err(OpenAiError::InvalidInput(format!(
            "{label} 체결가·1호가가 0 이하임"
        )));
    }
    if quote.ask1 < quote.bid1
        || quote.spread < 0.0
        || quote.last_trade_volume < 0.0
        || quote.trade_ts <= 0
        || quote.book_ts <= 0
    {
        return Err(OpenAiError::InvalidInput(format!(
            "{label} 호가·체결량·시각이 올바르지 않음"
        )));
    }
    Ok(())
}

fn validate_day(indicators: &IndicatorPayload) -> Result<(), OpenAiError> {
    let day = &indicators.day;
    let values = [
        ("시가", day.open),
        ("고가", day.high),
        ("저가", day.low),
        ("HLC3 거래량가중평균", day.hlc3_volume_weighted_average),
    ];
    for (label, value) in values {
        if let Some(value) = value {
            if !value.is_finite() || value <= 0.0 {
                return Err(OpenAiError::InvalidInput(format!(
                    "당일 {label}가 올바르지 않음"
                )));
            }
        }
    }
    if let (Some(high), Some(low)) = (day.high, day.low) {
        if high < low {
            return Err(OpenAiError::InvalidInput(
                "당일 고가가 저가보다 낮음".into(),
            ));
        }
    }
    Ok(())
}

fn validate_indicator(
    label: &str,
    indicator: &TimeframeIndicatorPayload,
    expected_interval: u32,
) -> Result<(), OpenAiError> {
    if indicator.interval_minutes != expected_interval {
        return Err(OpenAiError::InvalidInput(format!(
            "{label} 주기가 {}분으로 지정됨",
            indicator.interval_minutes
        )));
    }
    if indicator.completed_candles.len() > 30 {
        return Err(OpenAiError::InvalidInput(format!(
            "{label} 완성봉이 30개를 초과함"
        )));
    }
    let mut previous_time = None;
    for candle in indicator
        .completed_candles
        .iter()
        .chain(indicator.forming_candle.iter())
    {
        validate_candle(label, candle)?;
        if let Some(previous) = previous_time {
            if candle.time <= previous {
                return Err(OpenAiError::InvalidInput(format!(
                    "{label} 봉 시각이 오름차순이 아님"
                )));
            }
        }
        previous_time = Some(candle.time);
    }
    match (indicator.forming_candle, indicator.forming_progress_pct) {
        (Some(_), Some(progress)) if progress.is_finite() && (0.0..=100.0).contains(&progress) => {}
        (None, None) => {}
        _ => {
            return Err(OpenAiError::InvalidInput(format!(
                "{label} 형성봉과 진행률 조합이 올바르지 않음"
            )));
        }
    }
    for (name, value, allow_zero) in [
        ("MA5", indicator.moving_averages.ma5, false),
        ("MA20", indicator.moving_averages.ma20, false),
        ("MA60", indicator.moving_averages.ma60, false),
        ("MA120", indicator.moving_averages.ma120, false),
        ("평균거래량5", indicator.average_volumes.volume5, true),
        ("평균거래량20", indicator.average_volumes.volume20, true),
    ] {
        if let Some(value) = value {
            let invalid = !value.is_finite()
                || if allow_zero {
                    value < 0.0
                } else {
                    value <= 0.0
                };
            if invalid {
                return Err(OpenAiError::InvalidInput(format!(
                    "{label} {name}이 올바르지 않음"
                )));
            }
        }
    }
    Ok(())
}

fn validate_candle(label: &str, candle: &crate::types::Candle) -> Result<(), OpenAiError> {
    let numeric = [
        candle.open,
        candle.high,
        candle.low,
        candle.close,
        candle.volume,
    ];
    if candle.time <= 0
        || numeric.iter().any(|value| !value.is_finite())
        || candle.open <= 0.0
        || candle.high <= 0.0
        || candle.low <= 0.0
        || candle.close <= 0.0
        || candle.volume < 0.0
        || candle.high < candle.low
    {
        return Err(OpenAiError::InvalidInput(format!(
            "{label} OHLCV가 올바르지 않음"
        )));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictDecision {
    market_regime: MarketRegime,
    decision_summary_ko: String,
    scenarios: Vec<StrictScenario>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictScenario {
    product: ProductKind,
    setup_type: SetupType,
    reference_price: u64,
    confirmation_price: u64,
    invalidation_price: u64,
    target_return_pct: f64,
    rationale_ko: String,
}

fn parse_response_value(value: &Value, latency_ms: u64) -> Result<DecisionResult, DecisionFailure> {
    parse_response_value_inner(value, latency_ms)
        .map_err(|error| DecisionFailure::after_request(error, Some(value), latency_ms, None))
}

fn parse_response_value_inner(
    value: &Value,
    latency_ms: u64,
) -> Result<DecisionResult, OpenAiError> {
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        return Err(OpenAiError::Api(api_error_message(error)));
    }

    let status = value
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| OpenAiError::InvalidResponse("status 누락".into()))?;
    if status != "completed" {
        let reason = value
            .pointer("/incomplete_details/reason")
            .and_then(Value::as_str)
            .unwrap_or(status);
        return Err(OpenAiError::Incomplete(reason.to_owned()));
    }

    let output = value
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| OpenAiError::InvalidResponse("output 배열 누락".into()))?;
    let mut output_text = String::new();
    for item in output {
        if item.get("type").and_then(Value::as_str) == Some("refusal") {
            return Err(OpenAiError::Refused(refusal_message(item)));
        }
        if item.get("status").and_then(Value::as_str) == Some("incomplete") {
            return Err(OpenAiError::Incomplete("출력 메시지 잘림".into()));
        }
        if item.get("type").and_then(Value::as_str) == Some("output_text") {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                output_text.push_str(text);
            }
        }
        if let Some(content) = item.get("content").and_then(Value::as_array) {
            for part in content {
                match part.get("type").and_then(Value::as_str) {
                    Some("refusal") => {
                        return Err(OpenAiError::Refused(refusal_message(part)));
                    }
                    Some("output_text") => {
                        let text = part.get("text").and_then(Value::as_str).ok_or_else(|| {
                            OpenAiError::InvalidResponse("output_text.text 누락".into())
                        })?;
                        output_text.push_str(text);
                    }
                    _ => {}
                }
            }
        }
    }
    if output_text.trim().is_empty() {
        return Err(OpenAiError::InvalidResponse(
            "구조화 출력이 비어 있음".into(),
        ));
    }

    let strict: StrictDecision = serde_json::from_str(&output_text)
        .map_err(|error| OpenAiError::InvalidResponse(format!("결정 JSON 계약 위반: {error}")))?;
    validate_text_length("decisionSummaryKo", &strict.decision_summary_ko, 160)?;
    if strict.scenarios.len() > 2 {
        return Err(OpenAiError::InvalidResponse(
            "시나리오가 2개를 초과함".into(),
        ));
    }
    if strict.scenarios.len() == 2 && strict.scenarios[0].product == strict.scenarios[1].product {
        return Err(OpenAiError::InvalidResponse(
            "같은 상품의 시나리오가 중복됨".into(),
        ));
    }
    let mut scenarios = Vec::with_capacity(strict.scenarios.len());
    for scenario in strict.scenarios {
        validate_text_length("rationaleKo", &scenario.rationale_ko, 120)?;
        if scenario.reference_price == 0
            || scenario.confirmation_price == 0
            || scenario.invalidation_price == 0
        {
            return Err(OpenAiError::InvalidResponse("시나리오 가격이 0임".into()));
        }
        if !scenario.target_return_pct.is_finite()
            || !(0.2..=2.0).contains(&scenario.target_return_pct)
            || !is_tenth_step(scenario.target_return_pct)
        {
            return Err(OpenAiError::InvalidResponse(
                "targetReturnPct 범위 또는 단위가 올바르지 않음".into(),
            ));
        }
        scenarios.push(ModelScenario {
            product: scenario.product,
            setup_type: scenario.setup_type,
            reference_price: scenario.reference_price,
            confirmation_price: scenario.confirmation_price,
            invalidation_price: scenario.invalidation_price,
            target_return_pct: scenario.target_return_pct,
            rationale_ko: scenario.rationale_ko,
        });
    }

    let usage_value = value
        .get("usage")
        .ok_or_else(|| OpenAiError::InvalidResponse("usage 누락".into()))?;
    let usage = TokenUsage {
        input_tokens: token_field(usage_value, "/input_tokens")?,
        cached_input_tokens: optional_token_field(
            usage_value,
            "/input_tokens_details/cached_tokens",
        )?,
        cache_write_tokens: optional_token_field(
            usage_value,
            "/input_tokens_details/cache_write_tokens",
        )?,
        output_tokens: token_field(usage_value, "/output_tokens")?,
        reasoning_tokens: optional_token_field(
            usage_value,
            "/output_tokens_details/reasoning_tokens",
        )?,
    };
    let response_id = value
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| OpenAiError::InvalidResponse("응답 id 누락".into()))?
        .to_owned();

    Ok(DecisionResult {
        response_id,
        decision: ModelDecision {
            market_regime: strict.market_regime,
            decision_summary_ko: strict.decision_summary_ko,
            scenarios,
        },
        usage,
        latency_ms,
    })
}

fn validate_text_length(label: &str, value: &str, max_chars: usize) -> Result<(), OpenAiError> {
    let length = value.chars().count();
    if value.trim().is_empty() || length > max_chars {
        return Err(OpenAiError::InvalidResponse(format!(
            "{label} 길이가 1~{max_chars}자가 아님"
        )));
    }
    Ok(())
}

fn is_tenth_step(value: f64) -> bool {
    ((value * 10.0).round() - value * 10.0).abs() < 1e-9
}

fn response_id_from_value(value: &Value) -> Option<String> {
    value
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())
        .map(str::to_owned)
}

/// 실패 응답의 usage는 일부 필드만 있거나 형식이 깨져 있을 수 있으므로
/// 확인 가능한 정수 필드만 독립적으로 회수한다.
fn usage_from_value(value: &Value) -> TokenUsage {
    let Some(usage) = value.get("usage") else {
        return TokenUsage::default();
    };
    TokenUsage {
        input_tokens: usage
            .pointer("/input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cached_input_tokens: usage
            .pointer("/input_tokens_details/cached_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_write_tokens: usage
            .pointer("/input_tokens_details/cache_write_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: usage
            .pointer("/output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        reasoning_tokens: usage
            .pointer("/output_tokens_details/reasoning_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    }
}

fn token_field(usage: &Value, pointer: &str) -> Result<u64, OpenAiError> {
    usage
        .pointer(pointer)
        .and_then(Value::as_u64)
        .ok_or_else(|| OpenAiError::InvalidResponse(format!("usage{pointer} 누락 또는 형식 오류")))
}

fn optional_token_field(usage: &Value, pointer: &str) -> Result<u64, OpenAiError> {
    match usage.pointer(pointer) {
        None | Some(Value::Null) => Ok(0),
        Some(value) => value
            .as_u64()
            .ok_or_else(|| OpenAiError::InvalidResponse(format!("usage{pointer} 형식 오류"))),
    }
}

fn refusal_message(value: &Value) -> String {
    value
        .get("refusal")
        .or_else(|| value.get("text"))
        .and_then(Value::as_str)
        .unwrap_or("사유 없음")
        .to_owned()
}

fn api_error_message(value: &Value) -> String {
    value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("알 수 없는 오류")
        .to_owned()
}

fn http_status_error(status: StatusCode, body: &str) -> OpenAiError {
    let message = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| {
            value.get("error").map(api_error_message).or_else(|| {
                value
                    .get("message")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
        })
        .unwrap_or_else(|| {
            let mut compact = body.trim().chars().take(500).collect::<String>();
            if compact.is_empty() {
                compact = "응답 본문 없음".into();
            }
            compact
        });
    OpenAiError::HttpStatus {
        status: status.as_u16(),
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Candle;

    const SAMPLE_AS_OF: i64 = 1_784_715_330;

    fn sample_bars() -> Vec<Candle> {
        let current_minute = SAMPLE_AS_OF.div_euclid(60) * 60;
        (0..1_802)
            .map(|index| {
                let drift = index as f64 * 4.0;
                Candle {
                    time: current_minute - (1_801 - index) as i64 * 60,
                    open: 177_000.0 + drift,
                    high: 177_200.0 + drift,
                    low: 176_800.0 + drift,
                    close: 177_050.0 + drift,
                    volume: 100_000.0 + index as f64 * 10.0,
                }
            })
            .collect()
    }

    fn sample_quote(code: &str, price: f64) -> MarketQuoteInput {
        MarketQuoteInput {
            code: code.into(),
            price,
            change_rate: 1.25,
            ask1: price + 5.0,
            bid1: price - 5.0,
            ask1_qty: 1_200,
            bid1_qty: 900,
            spread: 10.0,
            last_trade_volume: 125.0,
            trade_ts: SAMPLE_AS_OF,
            book_ts: SAMPLE_AS_OF,
        }
    }

    fn sample_input() -> DecisionInput {
        let bars = sample_bars();
        DecisionInput {
            as_of_kst: "2026-07-22T10:15:30+09:00".into(),
            underlying: sample_quote("000660", 185_000.0),
            leverage: sample_quote("0193T0", 15_000.0),
            inverse: sample_quote("0197X0", 8_000.0),
            indicators: crate::chart_image::indicator_payload(&bars, SAMPLE_AS_OF),
            chart_png: crate::chart_image::render_composite_png(&bars, SAMPLE_AS_OF).unwrap(),
        }
    }

    #[test]
    fn responses_api_요청_계약이_고정된다() {
        let sample = sample_input();
        let body = build_request_body(&sample).unwrap();

        assert_eq!(body["model"], MODEL);
        assert_eq!(body["reasoning"]["effort"], "max");
        assert_eq!(body["reasoning"]["context"], "current_turn");
        assert_eq!(body["store"], false);
        assert_eq!(body["text"]["verbosity"], "low");
        assert_eq!(body["max_output_tokens"], MAX_OUTPUT_TOKENS);
        assert_eq!(body["text"]["format"]["type"], "json_schema");
        assert_eq!(body["text"]["format"]["strict"], true);
        assert_eq!(
            body["text"]["format"]["schema"]["properties"]["scenarios"]["maxItems"],
            2
        );
        assert_eq!(
            body["text"]["format"]["schema"]["properties"]["scenarios"]["items"]
                ["additionalProperties"],
            false
        );
        assert_eq!(
            body["text"]["format"]["schema"]["properties"]["decisionSummaryKo"]["maxLength"],
            160
        );
        assert_eq!(
            body["text"]["format"]["schema"]["properties"]["scenarios"]["items"]["properties"]
                ["rationaleKo"]["maxLength"],
            120
        );
        assert!(body.get("tools").is_none());
        assert!(body.get("previous_response_id").is_none());
        assert!(body.get("prompt_cache_options").is_none());

        let input = body["input"].as_array().unwrap();
        assert_eq!(input.len(), 2);
        assert_eq!(input[0]["content"][0]["text"], SYSTEM_PROMPT);
        assert_eq!(PROMPT_VERSION, "sk-hynix-oco-v4");
        assert!(SYSTEM_PROMPT.is_ascii());
        let user_content = input[1]["content"].as_array().unwrap();
        assert_eq!(
            user_content
                .iter()
                .filter(|part| part["type"] == "input_image")
                .count(),
            1
        );
        assert_eq!(user_content[1]["detail"], "original");
        assert!(user_content[1]["image_url"]
            .as_str()
            .unwrap()
            .starts_with("data:image/png;base64,"));

        let dynamic: Value =
            serde_json::from_str(user_content[0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(dynamic["quotes"]["skHynix"]["code"], "000660");
        assert_eq!(dynamic["quotes"]["skHynix"]["lastTradeVolume"], 125.0);
        assert_eq!(dynamic["quotes"]["skHynix"]["tradeTs"], SAMPLE_AS_OF);
        assert_eq!(dynamic["indicators"]["oneMinute"]["intervalMinutes"], 1);
        assert_eq!(
            dynamic["indicators"]["oneMinute"]["completedCandles"]
                .as_array()
                .unwrap()
                .len(),
            30
        );
        assert!(dynamic["indicators"]["day"]["hlc3VolumeWeightedAverage"].is_number());

        assert_eq!(
            serialized_dynamic_input(&sample).unwrap(),
            serialized_dynamic_input(&sample).unwrap()
        );
    }

    #[test]
    fn 완료된_구조화_응답과_토큰을_파싱한다() {
        let response = json!({
            "id": "resp_123",
            "status": "completed",
            "output": [{
                "type": "message",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "text": "{\"marketRegime\":\"RANGE\",\"decisionSummaryKo\":\"상단과 하단의 반응을 각각 대기\",\"scenarios\":[{\"product\":\"LEVERAGE\",\"setupType\":\"REVERSAL\",\"referencePrice\":184000,\"confirmationPrice\":185300,\"invalidationPrice\":183500,\"targetReturnPct\":0.3,\"rationaleKo\":\"지지선 반복 방어 뒤 거래량이 회복됨\"},{\"product\":\"INVERSE\",\"setupType\":\"REVERSAL\",\"referencePrice\":186000,\"confirmationPrice\":184700,\"invalidationPrice\":186500,\"targetReturnPct\":0.2,\"rationaleKo\":\"저항 재시험에서 윗꼬리와 거래량 소진\"}]}"
                }]
            }],
            "usage": {
                "input_tokens": 1800,
                "input_tokens_details": {
                    "cached_tokens": 900,
                    "cache_write_tokens": 120
                },
                "output_tokens": 540,
                "output_tokens_details": { "reasoning_tokens": 500 }
            }
        });

        let result = parse_response_value(&response, 1_234).unwrap();
        assert_eq!(result.response_id, "resp_123");
        assert_eq!(result.latency_ms, 1_234);
        assert_eq!(result.decision.market_regime, MarketRegime::Range);
        assert_eq!(
            result.decision.decision_summary_ko,
            "상단과 하단의 반응을 각각 대기"
        );
        assert_eq!(result.decision.scenarios.len(), 2);
        assert_eq!(result.decision.scenarios[0].product, ProductKind::Leverage);
        assert_eq!(result.decision.scenarios[0].setup_type, SetupType::Reversal);
        assert_eq!(result.decision.scenarios[1].product, ProductKind::Inverse);
        assert_eq!(result.usage.input_tokens, 1_800);
        assert_eq!(result.usage.cached_input_tokens, 900);
        assert_eq!(result.usage.cache_write_tokens, 120);
        assert_eq!(result.usage.reasoning_tokens, 500);
    }

    #[test]
    fn strict_응답은_0개와_1개_시나리오도_허용한다() {
        for (json_text, expected_count) in [
            (
                r#"{"marketRegime":"UNCLEAR","decisionSummaryKo":"시간축 충돌로 대기","scenarios":[]}"#,
                0usize,
            ),
            (
                r#"{"marketRegime":"UPTREND","decisionSummaryKo":"눌림 방어 후 재상승 확인 대기","scenarios":[{"product":"LEVERAGE","setupType":"CONTINUATION","referencePrice":186000,"confirmationPrice":186200,"invalidationPrice":184500,"targetReturnPct":0.3,"rationaleKo":"압축 뒤 거래량 확대 돌파를 대기"}]}"#,
                1usize,
            ),
        ] {
            let response = json!({
                "id": format!("resp_{expected_count}"),
                "status": "completed",
                "output": [{
                    "type": "message",
                    "status": "completed",
                    "content": [{ "type": "output_text", "text": json_text }]
                }],
                "usage": { "input_tokens": 1, "output_tokens": 1 }
            });

            let result = parse_response_value(&response, 1).unwrap();
            assert_eq!(result.decision.scenarios.len(), expected_count);
            assert_eq!(result.usage.cache_write_tokens, 0);
        }
    }

    #[test]
    fn 개별_의미오류는_파싱뒤_oco_검증에서_해당_시나리오만_제외한다() {
        let response = json!({
            "id": "resp_individual_invalid",
            "status": "completed",
            "output": [{
                "type": "message",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "text": "{\"marketRegime\":\"RANGE\",\"decisionSummaryKo\":\"한쪽만 가격 순서가 유효함\",\"scenarios\":[{\"product\":\"LEVERAGE\",\"setupType\":\"CONTINUATION\",\"referencePrice\":186000,\"confirmationPrice\":185500,\"invalidationPrice\":184000,\"targetReturnPct\":0.3,\"rationaleKo\":\"확인가 방향이 잘못된 의도적 테스트\"},{\"product\":\"INVERSE\",\"setupType\":\"CONTINUATION\",\"referencePrice\":184000,\"confirmationPrice\":183500,\"invalidationPrice\":186000,\"targetReturnPct\":0.2,\"rationaleKo\":\"하락 이탈과 거래량 확대\"}]}"
                }]
            }],
            "usage": { "input_tokens": 1, "output_tokens": 1 }
        });

        let parsed = parse_response_value(&response, 1).unwrap();
        let validated =
            crate::automation::oco::validate_decision(185_000, &parsed.decision).unwrap();

        assert_eq!(validated.scenarios.len(), 1);
        assert_eq!(validated.scenarios[0].product, ProductKind::Inverse);
        assert_eq!(validated.rejected.len(), 1);
        assert_eq!(validated.rejected[0].product, ProductKind::Leverage);
    }

    #[test]
    fn 거부와_잘린_응답은_판단으로_취급하지_않는다() {
        let refusal = json!({
            "id": "resp_refusal",
            "status": "completed",
            "output": [{
                "type": "message",
                "status": "completed",
                "content": [{ "type": "refusal", "refusal": "처리할 수 없음" }]
            }],
            "usage": {
                "input_tokens": 101,
                "input_tokens_details": {
                    "cached_tokens": 50,
                    "cache_write_tokens": 7
                },
                "output_tokens": 21,
                "output_tokens_details": { "reasoning_tokens": 20 }
            }
        });
        let refusal_failure = parse_response_value(&refusal, 10).unwrap_err();
        assert!(matches!(refusal_failure.error, OpenAiError::Refused(_)));
        assert_eq!(refusal_failure.response_id.as_deref(), Some("resp_refusal"));
        assert_eq!(refusal_failure.latency_ms, 10);
        assert_eq!(refusal_failure.usage.input_tokens, 101);
        assert_eq!(refusal_failure.usage.cached_input_tokens, 50);
        assert_eq!(refusal_failure.usage.cache_write_tokens, 7);
        assert_eq!(refusal_failure.usage.output_tokens, 21);
        assert_eq!(refusal_failure.usage.reasoning_tokens, 20);

        let incomplete = json!({
            "id": "resp_incomplete",
            "status": "incomplete",
            "incomplete_details": { "reason": "max_output_tokens" },
            "output": [],
            "usage": {
                "input_tokens": 200,
                "input_tokens_details": {
                    "cached_tokens": 150,
                    "cache_write_tokens": 11
                },
                "output_tokens": 40,
                "output_tokens_details": { "reasoning_tokens": 35 }
            }
        });
        let incomplete_failure = parse_response_value(&incomplete, 11).unwrap_err();
        assert!(matches!(
            incomplete_failure.error,
            OpenAiError::Incomplete(reason) if reason == "max_output_tokens"
        ));
        assert_eq!(
            incomplete_failure.response_id.as_deref(),
            Some("resp_incomplete")
        );
        assert_eq!(incomplete_failure.latency_ms, 11);
        assert_eq!(incomplete_failure.usage.input_tokens, 200);
        assert_eq!(incomplete_failure.usage.cached_input_tokens, 150);
        assert_eq!(incomplete_failure.usage.cache_write_tokens, 11);
        assert_eq!(incomplete_failure.usage.output_tokens, 40);
        assert_eq!(incomplete_failure.usage.reasoning_tokens, 35);
    }

    #[test]
    fn 빈_출력과_스키마_외_필드를_거부한다() {
        let empty = json!({
            "id": "resp_empty",
            "status": "completed",
            "output": [],
            "usage": { "input_tokens": 1, "output_tokens": 1 }
        });
        assert!(matches!(
            parse_response_value(&empty, 10).unwrap_err().error,
            OpenAiError::InvalidResponse(_)
        ));

        let extra = json!({
            "id": "resp_extra",
            "status": "completed",
            "output": [{
                "type": "message",
                "content": [{
                    "type": "output_text",
                    "text": "{\"marketRegime\":\"UNCLEAR\",\"decisionSummaryKo\":\"대기\",\"scenarios\":[],\"explanation\":\"매수\"}"
                }]
            }],
            "usage": { "input_tokens": 1, "output_tokens": 1 }
        });
        assert!(matches!(
            parse_response_value(&extra, 10).unwrap_err().error,
            OpenAiError::InvalidResponse(_)
        ));

        let scenario_extra = json!({
            "id": "resp_scenario_extra",
            "status": "completed",
            "output": [{
                "type": "message",
                "content": [{
                    "type": "output_text",
                    "text": "{\"marketRegime\":\"UPTREND\",\"decisionSummaryKo\":\"돌파 대기\",\"scenarios\":[{\"product\":\"LEVERAGE\",\"setupType\":\"CONTINUATION\",\"referencePrice\":186000,\"confirmationPrice\":186200,\"invalidationPrice\":184000,\"targetReturnPct\":0.3,\"rationaleKo\":\"거래량 확대\",\"confidence\":0.9}]}"
                }]
            }],
            "usage": { "input_tokens": 1, "output_tokens": 1 }
        });
        assert!(matches!(
            parse_response_value(&scenario_extra, 10).unwrap_err().error,
            OpenAiError::InvalidResponse(_)
        ));
    }

    #[test]
    fn 문자열_길이와_상품_중복을_방어적으로_거부한다() {
        let too_long = "가".repeat(161);
        let response = |text: String| {
            json!({
                "id": "resp_contract_error",
                "status": "completed",
                "output": [{
                    "type": "message",
                    "content": [{ "type": "output_text", "text": text }]
                }],
                "usage": { "input_tokens": 1, "output_tokens": 1 }
            })
        };
        let long_summary = response(format!(
            "{{\"marketRegime\":\"UNCLEAR\",\"decisionSummaryKo\":\"{too_long}\",\"scenarios\":[]}}"
        ));
        assert!(matches!(
            parse_response_value(&long_summary, 1).unwrap_err().error,
            OpenAiError::InvalidResponse(_)
        ));

        let long_rationale = "나".repeat(121);
        let rationale_response = response(format!(
            "{{\"marketRegime\":\"UPTREND\",\"decisionSummaryKo\":\"돌파 대기\",\"scenarios\":[{{\"product\":\"LEVERAGE\",\"setupType\":\"CONTINUATION\",\"referencePrice\":186000,\"confirmationPrice\":186200,\"invalidationPrice\":184000,\"targetReturnPct\":0.3,\"rationaleKo\":\"{long_rationale}\"}}]}}"
        ));
        assert!(matches!(
            parse_response_value(&rationale_response, 1)
                .unwrap_err()
                .error,
            OpenAiError::InvalidResponse(_)
        ));

        let invalid_target = response(
            r#"{"marketRegime":"UPTREND","decisionSummaryKo":"목표 단위 테스트","scenarios":[{"product":"LEVERAGE","setupType":"CONTINUATION","referencePrice":186000,"confirmationPrice":186200,"invalidationPrice":184000,"targetReturnPct":0.25,"rationaleKo":"잘못된 목표 단위"}]}"#.into(),
        );
        assert!(matches!(
            parse_response_value(&invalid_target, 1).unwrap_err().error,
            OpenAiError::InvalidResponse(_)
        ));

        let duplicate = response(
            r#"{"marketRegime":"UPTREND","decisionSummaryKo":"중복 상품 테스트","scenarios":[{"product":"LEVERAGE","setupType":"CONTINUATION","referencePrice":186000,"confirmationPrice":186200,"invalidationPrice":184000,"targetReturnPct":0.3,"rationaleKo":"첫 후보"},{"product":"LEVERAGE","setupType":"REVERSAL","referencePrice":184000,"confirmationPrice":185300,"invalidationPrice":183500,"targetReturnPct":0.3,"rationaleKo":"중복 후보"}]}"#.into(),
        );
        assert!(matches!(
            parse_response_value(&duplicate, 1).unwrap_err().error,
            OpenAiError::InvalidResponse(_)
        ));
    }

    #[test]
    fn http_오류_json에서도_확인_가능한_텔레메트리를_회수한다() {
        let value = json!({
            "id": "resp_rate_limited",
            "error": { "message": "rate limit" },
            "usage": {
                "input_tokens": 321,
                "input_tokens_details": {
                    "cached_tokens": 300,
                    "cache_write_tokens": 13
                },
                "output_tokens": 9,
                "output_tokens_details": { "reasoning_tokens": 8 }
            }
        });
        let failure = DecisionFailure::after_request(
            OpenAiError::HttpStatus {
                status: 429,
                message: "rate limit".into(),
            },
            Some(&value),
            987,
            None,
        );

        assert!(matches!(
            failure.error,
            OpenAiError::HttpStatus { status: 429, .. }
        ));
        assert_eq!(failure.response_id.as_deref(), Some("resp_rate_limited"));
        assert_eq!(failure.usage.input_tokens, 321);
        assert_eq!(failure.usage.cached_input_tokens, 300);
        assert_eq!(failure.usage.cache_write_tokens, 13);
        assert_eq!(failure.usage.output_tokens, 9);
        assert_eq!(failure.usage.reasoning_tokens, 8);
        assert_eq!(failure.latency_ms, 987);
    }

    #[derive(Clone, Copy)]
    enum 평가기대값 {
        시나리오(ProductKind, SetupType),
        대기,
    }

    struct 평가픽스처 {
        이름: &'static str,
        입력: DecisionInput,
        기대값: 평가기대값,
    }

    #[derive(Clone, Copy)]
    enum 평가패턴 {
        저항반락,
        지지반등,
        상승돌파,
        하락이탈,
        박스중단,
        형성봉거래량착시,
    }

    fn 평가용_분봉(패턴: 평가패턴) -> Vec<Candle> {
        let current_minute = SAMPLE_AS_OF.div_euclid(60) * 60;
        (0_usize..1_802)
            .map(|index| {
                let recent = index.saturating_sub(1_682) as f64;
                let (price, volume): (f64, f64) = if index < 1_682 {
                    (180_000.0 + index as f64 * 2.5, 90_000.0)
                } else {
                    match 패턴 {
                        평가패턴::저항반락 => {
                            let wave = (recent / 12.0).sin().abs();
                            (
                                184_250.0 + wave * 700.0 - (recent - 95.0).max(0.0) * 9.0,
                                180_000.0 - recent * 700.0,
                            )
                        }
                        평가패턴::지지반등 => {
                            let wave = (recent / 12.0).sin().abs();
                            (
                                185_050.0 - wave * 700.0 + (recent - 95.0).max(0.0) * 9.0,
                                120_000.0 + recent * 600.0,
                            )
                        }
                        평가패턴::상승돌파 => {
                            let breakout = (recent - 105.0).max(0.0);
                            (
                                184_500.0 + (recent / 5.0).sin() * 120.0 + breakout * 55.0,
                                80_000.0 + breakout * 18_000.0,
                            )
                        }
                        평가패턴::하락이탈 => {
                            let breakdown = (recent - 105.0).max(0.0);
                            (
                                185_500.0 + (recent / 5.0).sin() * 120.0 - breakdown * 55.0,
                                80_000.0 + breakdown * 18_000.0,
                            )
                        }
                        평가패턴::박스중단 => {
                            (185_000.0 + (recent / 4.0).sin() * 550.0, 100_000.0)
                        }
                        평가패턴::형성봉거래량착시 => (
                            185_000.0 + (recent / 4.0).sin() * 180.0,
                            if index == 1_801 { 95_000.0 } else { 100_000.0 },
                        ),
                    }
                };
                Candle {
                    time: current_minute - (1_801 - index) as i64 * 60,
                    open: price - 20.0,
                    high: price + 90.0,
                    low: price - 90.0,
                    close: price,
                    volume: volume.max(1_000.0),
                }
            })
            .collect()
    }

    fn 평가픽스처들() -> Vec<평가픽스처> {
        let specs = [
            (
                "저항 반복·거래량 감소",
                평가패턴::저항반락,
                평가기대값::시나리오(ProductKind::Inverse, SetupType::Reversal),
            ),
            (
                "지지 반복 방어",
                평가패턴::지지반등,
                평가기대값::시나리오(ProductKind::Leverage, SetupType::Reversal),
            ),
            (
                "거래량 동반 상승 돌파",
                평가패턴::상승돌파,
                평가기대값::시나리오(ProductKind::Leverage, SetupType::Continuation),
            ),
            (
                "거래량 동반 하락 이탈",
                평가패턴::하락이탈,
                평가기대값::시나리오(ProductKind::Inverse, SetupType::Continuation),
            ),
            ("박스 중단", 평가패턴::박스중단, 평가기대값::대기),
            (
                "형성 중 봉 거래량 오판 방지",
                평가패턴::형성봉거래량착시,
                평가기대값::대기,
            ),
        ];
        specs
            .into_iter()
            .map(|(이름, 패턴, 기대값)| {
                let bars = 평가용_분봉(패턴);
                let underlying_price = bars.last().unwrap().close;
                평가픽스처 {
                    이름,
                    기대값,
                    입력: DecisionInput {
                        as_of_kst: "2026-07-22T10:15:30+09:00".into(),
                        underlying: sample_quote("000660", underlying_price),
                        leverage: sample_quote("0193T0", 15_000.0),
                        inverse: sample_quote("0197X0", 8_000.0),
                        indicators: crate::chart_image::indicator_payload(&bars, SAMPLE_AS_OF),
                        chart_png: crate::chart_image::render_composite_png(&bars, SAMPLE_AS_OF)
                            .unwrap(),
                    },
                }
            })
            .collect()
    }

    fn 평가일치(결정: &ModelDecision, 기대값: 평가기대값) -> bool {
        match 기대값 {
            평가기대값::대기 => 결정.scenarios.is_empty(),
            평가기대값::시나리오(product, setup_type) => 결정
                .scenarios
                .iter()
                .any(|scenario| scenario.product == product && scenario.setup_type == setup_type),
        }
    }

    /// 실제 주문 경로와 무관한 수동 평가다.
    /// `cargo test llm_v4_평가 -- --ignored --nocapture`로만 API를 호출한다.
    #[tokio::test]
    #[ignore = "OPENAI_API_KEY와 명시적인 평가 실행이 필요함"]
    async fn llm_v4_평가_픽스처별_3회중_2회_이상_일치한다() {
        let api_key = std::env::var("OPENAI_API_KEY").expect(
            "OPENAI_API_KEY가 없습니다. opt-in LLM 평가를 실행하려면 키를 환경변수로 제공하세요.",
        );
        let client = OpenAiClient::new(api_key).unwrap();
        for fixture in 평가픽스처들() {
            let mut matched = 0;
            for run in 1..=3 {
                let result = client
                    .decide(&fixture.입력)
                    .await
                    .unwrap_or_else(|failure| {
                        panic!("{} {run}회차 API 실패: {failure}", fixture.이름)
                    });
                let validated = crate::automation::oco::validate_decision(
                    fixture.입력.underlying.price.round() as u64,
                    &result.decision,
                )
                .unwrap_or_else(|error| {
                    panic!("{} {run}회차 결정 검증 실패: {error:?}", fixture.이름)
                });
                assert!(
                    validated.rejected.is_empty(),
                    "{} {run}회차 가격 의미 검증 실패: {:?}",
                    fixture.이름,
                    validated.rejected
                );
                if 평가일치(&result.decision, fixture.기대값) {
                    matched += 1;
                }
                eprintln!(
                    "{} {run}회차: {:?} / {}",
                    fixture.이름,
                    result.decision.market_regime,
                    result.decision.decision_summary_ko
                );
            }
            assert!(
                matched >= 2,
                "{}: 기대 결과 일치가 3회 중 {matched}회",
                fixture.이름
            );
        }
    }
}
