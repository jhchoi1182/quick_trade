/**
 * 실제 체결평단과 실시간 체결가로 계산한 단순 수익률(%).
 * 입력이 유효하지 않으면 다른 수익률 값으로 대체하지 않고 null을 반환한다.
 */
export function livePnlRate(avgPrice: number, price: number | undefined): number | null {
  if (!price || price <= 0 || avgPrice <= 0) return null;
  return (price / avgPrice - 1) * 100;
}
