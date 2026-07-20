/**
 * 실시간 시세로 파생 계산한 수익률(%). 입력이 유효하지 않으면 null을 반환해
 * 호출부가 백엔드 스냅샷 값(30초 주기)으로 폴백하게 한다.
 * KIS 서버값(evlu_pfls_rt)은 수수료를 반영하지만 이 파생값은 미반영 — 소수점 차이는 허용.
 */
export function livePnlRate(avgPrice: number, price: number | undefined): number | null {
  if (!price || price <= 0 || avgPrice <= 0) return null;
  return (price / avgPrice - 1) * 100;
}
