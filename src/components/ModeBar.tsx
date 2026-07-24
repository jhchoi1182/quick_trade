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
  const changingTo = useAutomationStore((s) => s.changingTo);
  const changeMode = useAutomationStore((s) => s.changeMode);
  const pushToast = useUiStore((s) => s.pushToast);

  const choose = async (next: ControlMode) => {
    try {
      const confirmed = await changeMode(next);
      const label = MODES.find(({ value }) => value === confirmed.mode)?.label ?? confirmed.mode;
      pushToast("success", `${label} 모드로 전환했습니다`);
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
          disabled={changingTo !== null || mode === undefined}
          aria-busy={changingTo === value}
          aria-pressed={mode === value}
          onClick={() => void choose(value)}
        >
          {changingTo === value ? `${label} 전환 중…` : label}
        </button>
      ))}
    </div>
  );
}
