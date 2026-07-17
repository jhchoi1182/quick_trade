import { getCurrentWindow } from "@tauri-apps/api/window";
import { LogicalSize } from "@tauri-apps/api/dpi";
import { useSettingsStore } from "../stores/settingsStore";
import { useUiStore } from "../stores/uiStore";

/** 접었을 때 창 높이: 타이틀바 + 매매바 + 버튼 + 상태줄만 남는 크기 */
export const COLLAPSED_HEIGHT = 168;

/** 맨위로 고정 적용 + 상태 갱신 (시작 시 복원과 토글 버튼이 공용) */
export async function applyAlwaysOnTop(on: boolean): Promise<void> {
  await getCurrentWindow().setAlwaysOnTop(on);
  useUiStore.getState().setAlwaysOnTop(on);
}

/** 차트 접기/펼치기 + 창 높이 동기화 */
export async function applyChartCollapsed(collapse: boolean): Promise<void> {
  const ui = useUiStore.getState();
  const win = getCurrentWindow();
  const factor = await win.scaleFactor();
  const size = (await win.innerSize()).toLogical(factor);
  if (collapse) {
    ui.setExpandedHeight(Math.round(size.height));
    await win.setSize(new LogicalSize(size.width, COLLAPSED_HEIGHT));
  } else {
    await win.setSize(new LogicalSize(size.width, Math.max(ui.expandedHeight, 340)));
  }
  ui.setChartCollapsed(collapse);
}

export function TitleBar() {
  const chartSymbols = useSettingsStore((s) => s.settings?.chartSymbols ?? []);
  const chartCode = useUiStore((s) => s.chartCode);
  const collapsed = useUiStore((s) => s.chartCollapsed);
  const alwaysOnTop = useUiStore((s) => s.alwaysOnTop);
  const setChartCode = useUiStore((s) => s.setChartCode);
  const setSettingsOpen = useUiStore((s) => s.setSettingsOpen);

  return (
    <div className="titlebar" data-tauri-drag-region>
      <select
        className="selector chart-selector"
        value={chartCode}
        onChange={(e) => setChartCode(e.target.value)}
        title="차트 종목"
      >
        {chartSymbols.map((sym) => (
          <option key={sym.code} value={sym.code}>
            {sym.label}
          </option>
        ))}
      </select>
      <div className="titlebar-spacer" data-tauri-drag-region />
      <button
        className="win-btn"
        title={collapsed ? "차트 펼치기" : "차트 접기"}
        onClick={() =>
          void applyChartCollapsed(!collapsed).catch((err) =>
            useUiStore.getState().pushToast("error", `창 크기 조절 실패: ${String(err)}`),
          )
        }
      >
        {collapsed ? "▾" : "▴"}
      </button>
      <button
        className={alwaysOnTop ? "win-btn active" : "win-btn"}
        title={alwaysOnTop ? "맨위로 고정 해제" : "맨위로 고정"}
        onClick={() =>
          void applyAlwaysOnTop(!alwaysOnTop).catch((err) =>
            useUiStore.getState().pushToast("error", `맨위로 고정 실패: ${String(err)}`),
          )
        }
      >
        <svg
          width="12"
          height="12"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          aria-hidden="true"
        >
          <path d="M12 17v5" />
          <path d="M9 10.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24V16a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V7a1 1 0 0 1 1-1 2 2 0 0 0 0-4H8a2 2 0 0 0 0 4 1 1 0 0 1 1 1z" />
        </svg>
      </button>
      <button className="win-btn" title="설정" onClick={() => setSettingsOpen(true)}>
        ⚙
      </button>
      <button className="win-btn" title="최소화" onClick={() => void getCurrentWindow().minimize()}>
        ─
      </button>
      <button className="win-btn win-close" title="닫기" onClick={() => void getCurrentWindow().close()}>
        ✕
      </button>
    </div>
  );
}
