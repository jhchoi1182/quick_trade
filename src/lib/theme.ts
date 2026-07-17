import type { MA_PERIODS } from "./candles";

type MaPeriod = (typeof MA_PERIODS)[number];

/**
 * 차트 전용 고정 색상. 무채색 테마는 UI 크롬(버튼·배지 등)에만 적용되고,
 * 차트는 가독성을 위해 테마와 무관하게 항상 기본 색을 유지한다 (사용자 확정 사항).
 */
export const CHART_COLORS = {
  up: "#e5484d",
  down: "#3b82f6",
  text: "#9aa0a6",
  ma: { 5: "#f0b90b", 20: "#e056fd", 60: "#26a69a", 120: "#8d99ae" } as Record<MaPeriod, string>,
};
