import { describe, expect, it } from "vitest";
import { formatLlmTokenUsage } from "./llmUsage";

describe("LLM 토큰 계측 표시", () => {
  it("캐시 읽기·쓰기와 추론을 입력·출력 총합에 이중계산하지 않는다", () => {
    expect(formatLlmTokenUsage({
      inputTokens: 1_800,
      cachedInputTokens: 900,
      cacheWriteTokens: 120,
      outputTokens: 540,
      reasoningTokens: 500,
    })).toBe("입력 1,800 · 캐시 읽기 900 · 캐시 쓰기 120 · 출력 540 · 추론 500");
  });
});
