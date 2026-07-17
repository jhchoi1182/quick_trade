export function formatPrice(n: number): string {
  return n.toLocaleString("ko-KR");
}

/** 예수금 등 큰 금액을 좁은 상태줄에 맞게 축약 (예: 3,204,500 → 320.4만) */
export function formatCompactKrw(n: number): string {
  if (n >= 100_000_000) return `${(n / 100_000_000).toFixed(2)}억`;
  if (n >= 10_000) return `${Math.floor(n / 10_000).toLocaleString("ko-KR")}만`;
  return n.toLocaleString("ko-KR");
}

export function formatRate(rate: number): string {
  const sign = rate > 0 ? "+" : "";
  return `${sign}${rate.toFixed(2)}%`;
}

/** 등락/손익 색상 클래스 (국내 관례: 상승 빨강, 하락 파랑) */
export function rateClass(v: number): string {
  if (v > 0) return "up";
  if (v < 0) return "down";
  return "flat";
}
