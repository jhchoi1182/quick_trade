export interface LlmTokenUsage {
  inputTokens: number;
  cachedInputTokens: number;
  cacheWriteTokens: number;
  outputTokens: number;
  reasoningTokens: number;
}

/**
 * Responses API의 캐시 읽기·쓰기 토큰은 입력 토큰의 세부 계측값이고
 * reasoningTokens는 outputTokens의 세부값이다. 서로 더하지 않고 그대로 표시한다.
 */
export function formatLlmTokenUsage(usage: LlmTokenUsage): string {
  const format = (value: number) => value.toLocaleString("ko-KR");
  return [
    `입력 ${format(usage.inputTokens)}`,
    `캐시 읽기 ${format(usage.cachedInputTokens)}`,
    `캐시 쓰기 ${format(usage.cacheWriteTokens)}`,
    `출력 ${format(usage.outputTokens)}`,
    `추론 ${format(usage.reasoningTokens)}`,
  ].join(" · ");
}
