import type { ControlMode } from "../types";
import { useAutomationStore } from "../stores/automationStore";
import { useUiStore } from "../stores/uiStore";

const MODES: ReadonlyArray<{ value: ControlMode; label: string }> = [
  { value: "manual", label: "수동" },
  { value: "auto", label: "자동" },
  { value: "shadow", label: "섀도" },
];

export function ModeBar() {
  const mode = useAutomationStore((s) => s.snapshot?.mode);
  const changing = useAutomationStore((s) => s.changing);
  const changeMode = useAutomationStore((s) => s.changeMode);
  const pushToast = useUiStore((s) => s.pushToast);

  const choose = async (next: ControlMode) => {
    try {
      await changeMode(next);
    } catch (error) {
      pushToast("error", `모드 전환 실패: ${String(error)}`);
    }
  };

  return (
    <div className="control-mode-bar" aria-label="매매 제어 모드">
      {MODES.map(({ value, label }) => (
        <button
          key={value}
          className={mode === value ? `active mode-${value}` : ""}
          disabled={changing || mode === undefined}
          aria-pressed={mode === value}
          onClick={() => void choose(value)}
        >
          {label}
        </button>
      ))}
    </div>
  );
}
