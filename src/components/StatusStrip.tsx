import { useAccountStore } from "../stores/accountStore";
import { useSettingsStore } from "../stores/settingsStore";
import { useUiStore } from "../stores/uiStore";
import { formatCompactKrw, formatRate, rateClass } from "../lib/format";

const MODE_LABEL: Record<string, string> = { real: "실전", paper: "모의", demo: "데모" };

export function StatusStrip() {
  const tradeCode = useUiStore((s) => s.tradeCode);
  const position = useAccountStore((s) => s.positions[tradeCode]);
  const cash = useAccountStore((s) => s.cash);
  const connected = useAccountStore((s) => s.connected);
  const mode = useSettingsStore((s) => s.settings?.mode ?? "demo");
  const opacity = useSettingsStore((s) => s.settings?.opacity ?? 1);
  const setOpacity = useSettingsStore((s) => s.setOpacity);

  return (
    <div className="status-strip">
      <div className="status-row">
        {position && position.qty > 0 ? (
          <span className="holding">
            보유 {position.qty.toLocaleString("ko-KR")}주{" "}
            <span className={rateClass(position.pnlRate)}>{formatRate(position.pnlRate)}</span>
          </span>
        ) : (
          <span className="holding flat">보유 없음</span>
        )}
        <span className="cash">예수금 {formatCompactKrw(cash)}</span>
        <span className={`conn-dot ${connected ? "on" : "off"}`} title={connected ? "실시간 연결됨" : "연결 끊김"}>
          ●
        </span>
        <span className={`mode-badge mode-${mode}`}>{MODE_LABEL[mode]}</span>
      </div>
      <div className="opacity-row">
        <span className="opacity-label">투명도</span>
        <input
          type="range"
          min={0.3}
          max={1}
          step={0.05}
          value={opacity}
          onChange={(e) => setOpacity(Number(e.target.value))}
        />
      </div>
    </div>
  );
}
