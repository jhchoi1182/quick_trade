import { useRef, useState } from "react";
import { buyMax, sellAll } from "../lib/tauri";
import { useUiStore } from "../stores/uiStore";
import { formatPrice } from "../lib/format";

/**
 * 원클릭 즉시 주문 버튼.
 * 응답 대기 중(수십 ms)에만 같은 방향 재클릭을 잠가 의도치 않은 이중 발사를 막는다.
 * 확인 팝업 등 추가 절차는 두지 않는다 (사용자 확정 사항).
 */
export function TradeButtons() {
  const tradeCode = useUiStore((s) => s.tradeCode);
  const pushToast = useUiStore((s) => s.pushToast);
  const [busy, setBusy] = useState<"buy" | "sell" | null>(null);
  const busyRef = useRef<"buy" | "sell" | null>(null);

  const fire = async (side: "buy" | "sell") => {
    if (!tradeCode) {
      pushToast("error", "매매 종목이 선택되지 않았습니다");
      return;
    }
    if (busyRef.current === side) return;
    busyRef.current = side;
    setBusy(side);
    try {
      const result = side === "buy" ? await buyMax(tradeCode) : await sellAll(tradeCode);
      if (result.ok) {
        const label = side === "buy" ? "매수 주문" : "매도 주문";
        // 시장가 매도는 price가 0으로 내려온다
        const priceLabel = result.price > 0 ? formatPrice(result.price) : "시장가";
        pushToast("info", `${label} ${result.qty}주 @ ${priceLabel}`);
      } else {
        pushToast("error", result.message);
      }
    } catch (err) {
      pushToast("error", String(err));
    } finally {
      busyRef.current = null;
      setBusy(null);
    }
  };

  return (
    <div className="trade-buttons">
      <button className="btn-buy" disabled={busy === "buy"} onClick={() => void fire("buy")}>
        매수
      </button>
      <button className="btn-sell" disabled={busy === "sell"} onClick={() => void fire("sell")}>
        매도
      </button>
    </div>
  );
}
