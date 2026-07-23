//! OpenAI Responses API를 이용한 SK하이닉스 OCO 판단 클라이언트.
//!
//! 요청 계약은 이 모듈에 고정한다. 호출 스케줄·상태 revision 확인·시나리오의
//! 방향/호가단위 의미 검증은 상위 자동매매 엔진의 책임이다.

use std::time::{Duration, Instant};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::types::{Candle, ModelDecision, ModelScenario, ProductKind, Quote};

pub const MODEL: &str = "gpt-5.6-sol";
pub const PROMPT_VERSION: &str = "sk-hynix-oco-v2";
const RESPONSES_URL: &str = "https://api.openai.com/v1/responses";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(240);

/// 프롬프트 캐시의 고정 접두사가 되므로 동적 값을 추가하지 않는다.
pub const SYSTEM_PROMPT: &str = "당신은 SK하이닉스(000660) 연동 레버리지·곱버스 ETF 스캘핑 신호기다. 매 호출은 독립적이며 제공된 현재 시세와 10·15분봉만 사용한다. 과거 결정·누적손익·연속손실은 고려하지 마라. 횡보·추세 환경에서 명확한 단기 우위가 있는 조건만 0~2개 시나리오로 제시하고, 근거가 약한 반대 방향을 억지로 만들지 마라. LEVERAGE는 현재 본주보다 높은 triggerPrice의 상향 돌파, INVERSE는 낮은 triggerPrice의 하향 이탈 조건이다. triggerPrice에는 차트상 기술적 기준가만 제시하고 가짜 돌파 방지 보정값을 미리 반영하지 마라. 실행기는 LEVERAGE는 위로, INVERSE는 아래로 0.1% 보정해 유효 호가로 정규화한 뒤 먼저 3초·3틱 확인된 하나만 실행한다. 양방향이 모두 타당하면 둘 다 제시한다. targetReturnPct는 선택 ETF의 실제 체결평단 대비 단순 목표수익률로 0.2~2.0%, 0.1% 단위다. 기본 후보는 0.2/0.3%이며 명확한 여지가 있을 때만 높여라. 약 ±2배 상품의 특성과 각 ETF의 직접 시세·호가를 반영하되 프로그램은 목표를 다시 2배 환산하지 않는다. 실행기는 -0.3% 손절, 첫 체결 10분 후 청산, 15:15 청산을 별도로 수행한다. 지정 JSON만 반환하라.";

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
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndicatorInput {
    pub interval_minutes: u32,
    pub current: Candle,
    pub ma5: f64,
    pub ma20: f64,
    pub ma60: f64,
    pub ma120: f64,
}

/// 한 번의 독립적인 LLM 판단에 허용된 동적 입력만 담는다.
#[derive(Debug, Clone)]
pub struct DecisionInput {
    pub as_of_kst: String,
    pub underlying: MarketQuoteInput,
    pub leverage: MarketQuoteInput,
    pub inverse: MarketQuoteInput,
    pub ten_minute: IndicatorInput,
    pub fifteen_minute: IndicatorInput,
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

fn build_request_body(input: &DecisionInput) -> Result<Value, OpenAiError> {
    validate_input(input)?;

    let dynamic_input = json!({
        "asOfKst": input.as_of_kst,
        "quotes": {
            "skHynix": input.underlying,
            "leverageEtf": input.leverage,
            "inverseEtf": input.inverse,
        },
        "indicators": {
            "tenMinute": input.ten_minute,
            "fifteenMinute": input.fifteen_minute,
        },
    });
    let dynamic_text = serde_json::to_string(&dynamic_input)
        .map_err(|error| OpenAiError::InvalidInput(error.to_string()))?;
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
        "max_output_tokens": 4096,
        "input": [
            {
                "role": "system",
                "content": [{ "type": "input_text", "text": SYSTEM_PROMPT }]
            },
            {
                "role": "user",
                "content": [
                    { "type": "input_text", "text": dynamic_text },
                    { "type": "input_image", "image_url": chart_data_url, "detail": "high" }
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
                            "triggerPrice": {
                                "type": "integer",
                                "exclusiveMinimum": 0
                            },
                            "targetReturnPct": {
                                "type": "number",
                                "minimum": 0.2,
                                "maximum": 2.0,
                                "multipleOf": 0.1
                            }
                        },
                        "required": ["product", "triggerPrice", "targetReturnPct"],
                        "additionalProperties": false
                    }
                }
            },
            "required": ["scenarios"],
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
    validate_indicator("10분봉", &input.ten_minute, 10)?;
    validate_indicator("15분봉", &input.fifteen_minute, 15)?;

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
    Ok(())
}

fn validate_indicator(
    label: &str,
    indicator: &IndicatorInput,
    expected_interval: u32,
) -> Result<(), OpenAiError> {
    if indicator.interval_minutes != expected_interval {
        return Err(OpenAiError::InvalidInput(format!(
            "{label} 주기가 {}분으로 지정됨",
            indicator.interval_minutes
        )));
    }
    let candle = indicator.current;
    let numeric = [
        candle.open,
        candle.high,
        candle.low,
        candle.close,
        candle.volume,
        indicator.ma5,
        indicator.ma20,
        indicator.ma60,
        indicator.ma120,
    ];
    if numeric.iter().any(|value| !value.is_finite())
        || candle.open <= 0.0
        || candle.high <= 0.0
        || candle.low <= 0.0
        || candle.close <= 0.0
        || candle.volume < 0.0
        || indicator.ma5 <= 0.0
        || indicator.ma20 <= 0.0
        || indicator.ma60 <= 0.0
        || indicator.ma120 <= 0.0
    {
        return Err(OpenAiError::InvalidInput(format!(
            "{label} OHLCV 또는 이동평균이 올바르지 않음"
        )));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictDecision {
    scenarios: Vec<StrictScenario>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictScenario {
    product: ProductKind,
    trigger_price: u64,
    target_return_pct: f64,
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
    if strict.scenarios.len() > 2 {
        return Err(OpenAiError::InvalidResponse(
            "시나리오가 2개를 초과함".into(),
        ));
    }
    let mut scenarios = Vec::with_capacity(strict.scenarios.len());
    for scenario in strict.scenarios {
        // strict schema가 정상 적용된 API 응답에서는 범위·단위가 이미 보장된다.
        // 그래도 방어적으로 잘못된 값이 들어오면 여기서 결정 전체를 버리지 않고
        // OCO 의미 검증으로 넘겨 해당 시나리오만 invalid 처리한다. 전체 무효는
        // 최대 개수·중복 상품처럼 결정 자체가 모호한 경우에만 맡긴다.
        scenarios.push(ModelScenario {
            product: scenario.product,
            trigger_price: scenario.trigger_price,
            target_return_pct: scenario.target_return_pct,
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
        decision: ModelDecision { scenarios },
        usage,
        latency_ms,
    })
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

    fn sample_candle() -> Candle {
        Candle {
            time: 1_753_152_600,
            open: 184_000.0,
            high: 185_500.0,
            low: 183_500.0,
            close: 185_000.0,
            volume: 123_456.0,
        }
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
        }
    }

    fn sample_indicator(interval_minutes: u32) -> IndicatorInput {
        IndicatorInput {
            interval_minutes,
            current: sample_candle(),
            ma5: 184_800.0,
            ma20: 183_500.0,
            ma60: 180_000.0,
            ma120: 175_000.0,
        }
    }

    fn sample_input() -> DecisionInput {
        DecisionInput {
            as_of_kst: "2026-07-22T10:15:00+09:00".into(),
            underlying: sample_quote("000660", 185_000.0),
            leverage: sample_quote("0193T0", 15_000.0),
            inverse: sample_quote("0197X0", 8_000.0),
            ten_minute: sample_indicator(10),
            fifteen_minute: sample_indicator(15),
            chart_png: b"\x89PNG\r\n\x1a\nchart".to_vec(),
        }
    }

    #[test]
    fn responses_api_요청_계약이_고정된다() {
        let body = build_request_body(&sample_input()).unwrap();

        assert_eq!(body["model"], MODEL);
        assert_eq!(body["reasoning"]["effort"], "max");
        assert_eq!(body["reasoning"]["context"], "current_turn");
        assert_eq!(body["store"], false);
        assert_eq!(body["text"]["verbosity"], "low");
        assert_eq!(body["max_output_tokens"], 4096);
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
        assert!(body.get("tools").is_none());
        assert!(body.get("previous_response_id").is_none());
        assert!(body.get("prompt_cache_options").is_none());

        let input = body["input"].as_array().unwrap();
        assert_eq!(input.len(), 2);
        assert_eq!(input[0]["content"][0]["text"], SYSTEM_PROMPT);
        let user_content = input[1]["content"].as_array().unwrap();
        assert_eq!(
            user_content
                .iter()
                .filter(|part| part["type"] == "input_image")
                .count(),
            1
        );
        assert_eq!(user_content[1]["detail"], "high");
        assert!(user_content[1]["image_url"]
            .as_str()
            .unwrap()
            .starts_with("data:image/png;base64,"));

        let dynamic: Value =
            serde_json::from_str(user_content[0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(dynamic["quotes"]["skHynix"]["code"], "000660");
        assert_eq!(dynamic["indicators"]["tenMinute"]["intervalMinutes"], 10);
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
                    "text": "{\"scenarios\":[{\"product\":\"LEVERAGE\",\"triggerPrice\":186000,\"targetReturnPct\":0.3},{\"product\":\"INVERSE\",\"triggerPrice\":183500,\"targetReturnPct\":0.2}]}"
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
        assert_eq!(result.decision.scenarios.len(), 2);
        assert_eq!(result.decision.scenarios[0].product, ProductKind::Leverage);
        assert_eq!(result.decision.scenarios[1].product, ProductKind::Inverse);
        assert_eq!(result.usage.input_tokens, 1_800);
        assert_eq!(result.usage.cached_input_tokens, 900);
        assert_eq!(result.usage.cache_write_tokens, 120);
        assert_eq!(result.usage.reasoning_tokens, 500);
    }

    #[test]
    fn strict_응답은_0개와_1개_시나리오도_허용한다() {
        for (json_text, expected_count) in [
            (r#"{"scenarios":[]}"#, 0usize),
            (
                r#"{"scenarios":[{"product":"LEVERAGE","triggerPrice":186000,"targetReturnPct":0.3}]}"#,
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
                    "text": "{\"scenarios\":[{\"product\":\"LEVERAGE\",\"triggerPrice\":0,\"targetReturnPct\":0.25},{\"product\":\"INVERSE\",\"triggerPrice\":183500,\"targetReturnPct\":0.2}]}"
                }]
            }],
            "usage": { "input_tokens": 1, "output_tokens": 1 }
        });

        let parsed = parse_response_value(&response, 1).unwrap();
        let validated =
            crate::automation::oco::validate_decision(185_000, &parsed.decision.scenarios).unwrap();

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
                    "text": "{\"scenarios\":[],\"explanation\":\"매수\"}"
                }]
            }],
            "usage": { "input_tokens": 1, "output_tokens": 1 }
        });
        assert!(matches!(
            parse_response_value(&extra, 10).unwrap_err().error,
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
}
