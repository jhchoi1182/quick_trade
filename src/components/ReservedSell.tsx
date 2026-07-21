import { useRef, useState } from "react";
import { placeReservedSell, cancelReservedSell } from "../lib/tauri";
import { useUiStore } from "../stores/uiStore";
import { useAccountStore } from "../stores/accountStore";
import { useReservationStore } from "../stores/reservationStore";
import { formatPrice } from "../lib/format";

const PRESETS = [0.2, 0.3, 0.5];

/**
 * 예약 매도 패널. 평단 대비 목표% 이상 첫 호가에 보유 전량 지정가 매도를 걸어둔다.
 * 상태(대기/체결/취소)는 백엔드 "reservation" 이벤트로만 갱신되며 여기서는 읽기만 한다.
 * 체결 알림은 공통 "매도 체결" 토스트가 담당한다.
 */
export function ReservedSell() {
  const tradeCode = useUiStore((s) => s.tradeCode);
  const reservedPct = useUiStore((s) => s.reservedPct);
  const setReservedPct = useUiStore((s) => s.setReservedPct);
  const pushToast = useUiStore((s) => s.pushToast);
  const position = useAccountStore((s) => s.positions[tradeCode]);
  const reservation = useReservationStore((s) => s.reservations[tradeCode]);

  const [pctInput, setPctInput] = useState(String(reservedPct));
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);

  const parsedPct = Number(pctInput);
  const hasHolding = !!position && position.qty > 0;

  const place = async () => {
    if (busyRef.current) return;
    if (!Number.isFinite(parsedPct) || parsedPct <= 0) {
      pushToast("error", "목표 수익률을 0보다 크게 입력하세요");
      return;
    }
    busyRef.current = true;
    setBusy(true);
    try {
      setReservedPct(parsedPct); // 마지막 사용 % 기억
      const result = await placeReservedSell(tradeCode, parsedPct);
      if (result.ok) {
        pushToast("info", `예약 매도 ${result.qty}주 @ ${formatPrice(result.price)}`);
      } else {
        pushToast("error", result.message);
      }
    } catch (e) {
      pushToast("error", String(e));
    } finally {
      busyRef.current = false;
      setBusy(false);
    }
  };

  const cancel = async () => {
    if (busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    try {
      const result = await cancelReservedSell(tradeCode);
      if (result.ok) {
        pushToast("info", "예약 매도 취소됨");
      } else {
        pushToast("error", result.message);
      }
    } catch (e) {
      pushToast("error", String(e));
    } finally {
      busyRef.current = false;
      setBusy(false);
    }
  };

  if (reservation) {
    return (
      <div className="reserved-sell active">
        <span className="reserved-status">
          예약 <b>{formatPrice(reservation.targetPrice)}원</b>
          <span className="reserved-pct">+{reservation.targetPct}%</span>
          <span className="reserved-qty">{reservation.qty.toLocaleString("ko-KR")}주</span>
        </span>
        <button className="reserved-cancel" disabled={busy} onClick={() => void cancel()}>
          예약 취소
        </button>
      </div>
    );
  }

  const preview =
    hasHolding && Number.isFinite(parsedPct) && parsedPct > 0
      ? Math.round(position.avgPrice * (1 + parsedPct / 100))
      : null;

  return (
    <div className="reserved-sell">
      <div className="reserved-head">
        <span className="reserved-title">예약 매도 (평단 대비)</span>
        {preview !== null && <span className="reserved-preview">≈ {formatPrice(preview)}원</span>}
      </div>
      <div className="reserved-controls">
        <div className="reserved-presets">
          {PRESETS.map((p) => (
            <button key={p} className={parsedPct === p ? "active" : ""} onClick={() => setPctInput(String(p))}>
              {p}%
            </button>
          ))}
        </div>
        <div className="reserved-input">
          <input
            type="number"
            step={0.1}
            min={0.1}
            value={pctInput}
            onChange={(e) => setPctInput(e.target.value)}
            aria-label="목표 수익률 %"
          />
          <span className="reserved-unit">%</span>
        </div>
        <button
          className="reserved-arm"
          disabled={busy || !hasHolding}
          title={hasHolding ? "" : "보유 종목이 있어야 예약할 수 있습니다"}
          onClick={() => void place()}
        >
          예약
        </button>
      </div>
    </div>
  );
}
