import type { MA_PERIODS } from "./candles";
import type { ThemeName } from "../types";

type MaPeriod = (typeof MA_PERIODS)[number];

export interface ChartPalette {
  up: string;
  down: string;
  volUp: string;
  volDown: string;
  text: string;
  grid: string;
  border: string;
  ma: Record<MaPeriod, string>;
}

/**
 * 테마별 차트 팔레트. mono는 업무 중 위장용 회색조로, 차트까지 무채색으로 표시한다.
 * 캔들 up/down은 styles.css의 mono --up/--down과 값을 맞춘다.
 */
const PALETTES: Record<ThemeName, ChartPalette> = {
  default: {
    up: "#e5484d",
    down: "#3b82f6",
    volUp: "rgba(229,72,77,0.4)",
    volDown: "rgba(59,130,246,0.4)",
    text: "#9aa0a6",
    grid: "rgba(255,255,255,0.05)",
    border: "rgba(255,255,255,0.1)",
    ma: { 5: "#f0b90b", 20: "#e056fd", 60: "#26a69a", 120: "#8d99ae" },
  },
  mono: {
    up: "#c9c9c9",
    down: "#686868",
    volUp: "rgba(201,201,201,0.35)",
    volDown: "rgba(104,104,104,0.35)",
    text: "#7d7d7d",
    grid: "rgba(255,255,255,0.05)",
    border: "rgba(255,255,255,0.1)",
    ma: { 5: "#e8e8e8", 20: "#b0b0b0", 60: "#848484", 120: "#585858" },
  },
};

export function chartColors(theme: ThemeName): ChartPalette {
  return PALETTES[theme] ?? PALETTES.default;
}
