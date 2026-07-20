import { useEffect, useState } from "react";
import type { Settings, SymbolConfig, ThemeName, TradeMode } from "../types";
import { useSettingsStore } from "../stores/settingsStore";
import { useUiStore } from "../stores/uiStore";

function updateSymbol(list: SymbolConfig[], idx: number, patch: Partial<SymbolConfig>): SymbolConfig[] {
  return list.map((item, i) => (i === idx ? { ...item, ...patch } : item));
}

interface SymbolListEditorProps {
  title: string;
  list: SymbolConfig[];
  onChange: (next: SymbolConfig[]) => void;
}

function SymbolListEditor({ title, list, onChange }: SymbolListEditorProps) {
  return (
    <div className="form-section">
      <div className="form-section-title">{title}</div>
      {list.map((sym, i) => (
        <div className="symbol-row" key={i}>
          <input
            className="symbol-label"
            placeholder="라벨"
            value={sym.label}
            onChange={(e) => onChange(updateSymbol(list, i, { label: e.target.value }))}
          />
          <input
            className="symbol-code"
            placeholder="종목코드"
            value={sym.code}
            onChange={(e) => onChange(updateSymbol(list, i, { code: e.target.value.trim().toUpperCase() }))}
          />
          <label className="symbol-etf" title="ETF/ETN이면 호가단위 5원 고정">
            <input
              type="checkbox"
              checked={sym.etf}
              onChange={(e) => onChange(updateSymbol(list, i, { etf: e.target.checked }))}
            />
            ETF
          </label>
          <button className="row-del" onClick={() => onChange(list.filter((_, j) => j !== i))}>
            ✕
          </button>
        </div>
      ))}
      <button className="row-add" onClick={() => onChange([...list, { code: "", label: "", etf: true }])}>
        + 종목 추가
      </button>
    </div>
  );
}

export function SettingsModal() {
  const settings = useSettingsStore((s) => s.settings);
  const save = useSettingsStore((s) => s.save);
  const open = useUiStore((s) => s.settingsOpen);
  const setOpen = useUiStore((s) => s.setSettingsOpen);
  const pushToast = useUiStore((s) => s.pushToast);

  const [draft, setDraft] = useState<Settings | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (open && settings) setDraft(structuredClone(settings));
  }, [open, settings]);

  if (!open || !draft) return null;

  const patch = (p: Partial<Settings>) => setDraft((d) => (d ? { ...d, ...p } : d));

  const validate = (): string | null => {
    if (draft.mode !== "demo") {
      if (!draft.appKey || !draft.appSecret) return "실전/모의 모드에는 APP KEY와 SECRET이 필요합니다";
      if (!/^\d{8}$/.test(draft.cano)) return "계좌번호 앞 8자리를 숫자로 입력하세요";
      if (!/^\d{2}$/.test(draft.acntPrdtCd)) return "계좌 상품코드 2자리를 입력하세요";
    }
    const codes = [...draft.tradeSymbols, ...draft.chartSymbols];
    if (draft.tradeSymbols.length === 0) return "매매 종목이 최소 1개 필요합니다";
    if (draft.chartSymbols.length === 0) return "차트 종목이 최소 1개 필요합니다";
    for (const s of codes) {
      if (!/^[A-Z0-9]{6}$/.test(s.code)) return `종목코드는 6자리 영숫자여야 합니다: "${s.code || s.label}"`;
      if (!s.label) return "라벨이 비어있는 종목이 있습니다";
    }
    return null;
  };

  const onSave = async () => {
    const err = validate();
    if (err) {
      pushToast("error", err);
      return;
    }
    setSaving(true);
    try {
      await save(draft);
      // 엔진 재시작 여부는 백엔드가 변경 필드에 따라 판단하므로 여기서 단정하지 않는다
      pushToast("success", "설정 저장 완료");
      setOpen(false);
    } catch (e) {
      pushToast("error", String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="modal-backdrop">
      <div className="modal">
        <div className="modal-title">설정</div>
        <div className="modal-body">
          <div className="form-section">
            <div className="form-section-title">모드</div>
            <div className="mode-toggle">
              {(["demo", "paper", "real"] as TradeMode[]).map((m) => (
                <button
                  key={m}
                  className={draft.mode === m ? "active" : ""}
                  onClick={() => patch({ mode: m })}
                >
                  {m === "demo" ? "데모" : m === "paper" ? "모의투자" : "실전"}
                </button>
              ))}
            </div>
            {draft.mode === "demo" && <div className="form-hint">데모: API 키 없이 가상 시세/체결로 동작</div>}
          </div>

          <div className="form-section">
            <div className="form-section-title">테마</div>
            <div className="mode-toggle">
              {(
                [
                  ["default", "기본"],
                  ["mono", "무채색"],
                ] as [ThemeName, string][]
              ).map(([value, label]) => (
                <button
                  key={value}
                  className={draft.theme === value ? "active" : ""}
                  onClick={() => patch({ theme: value })}
                >
                  {label}
                </button>
              ))}
            </div>
            {draft.theme === "mono" && (
              <div className="form-hint">무채색: 차트·버튼까지 회색조 — 업무 중 위장용</div>
            )}
          </div>

          <div className="form-section">
            <div className="form-section-title">한국투자증권 API</div>
            <input
              placeholder="APP KEY"
              value={draft.appKey}
              onChange={(e) => patch({ appKey: e.target.value.trim() })}
            />
            <input
              placeholder="APP SECRET"
              type="password"
              value={draft.appSecret}
              onChange={(e) => patch({ appSecret: e.target.value.trim() })}
            />
            <div className="inline-inputs">
              <input
                placeholder="계좌번호 8자리"
                value={draft.cano}
                onChange={(e) => patch({ cano: e.target.value.trim() })}
              />
              <input
                placeholder="상품코드 (01)"
                value={draft.acntPrdtCd}
                onChange={(e) => patch({ acntPrdtCd: e.target.value.trim() })}
              />
            </div>
            <input
              placeholder="HTS ID (실시간 체결통보용)"
              value={draft.htsId}
              onChange={(e) => patch({ htsId: e.target.value.trim() })}
            />
          </div>

          <SymbolListEditor
            title="매매 종목 (매수/매도 버튼 대상)"
            list={draft.tradeSymbols}
            onChange={(next) => patch({ tradeSymbols: next })}
          />
          <SymbolListEditor
            title="차트 종목"
            list={draft.chartSymbols}
            onChange={(next) => patch({ chartSymbols: next })}
          />

          <div className="form-section">
            <div className="form-section-title">주문 거래소</div>
            <div className="mode-toggle">
              {(
                [
                  ["KRX", "KRX"],
                  ["SOR", "SOR (스마트 라우팅)"],
                ] as ["KRX" | "SOR", string][]
              ).map(([value, label]) => (
                <button
                  key={value}
                  className={draft.exchange === value ? "active" : ""}
                  onClick={() => patch({ exchange: value })}
                >
                  {label}
                </button>
              ))}
            </div>
            <div className="form-hint">
              SOR: KRX/NXT 중 유리한 호가로 자동 라우팅 (모의투자는 KRX 고정)
            </div>
          </div>
        </div>
        <div className="modal-actions">
          <button className="btn-cancel" onClick={() => setOpen(false)}>
            취소
          </button>
          <button className="btn-save" disabled={saving} onClick={() => void onSave()}>
            저장
          </button>
        </div>
      </div>
    </div>
  );
}
