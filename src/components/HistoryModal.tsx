import { useEffect, useRef, useState } from "react";
import { listLlmDecisions, listTradeRecords } from "../lib/tauri";
import { formatPrice, formatRate, rateClass } from "../lib/format";
import { formatLlmTokenUsage } from "../lib/llmUsage";
import { useUiStore } from "../stores/uiStore";
import type {
  CursorPage,
  DecisionScenarioProduct,
  HistoryCursor,
  LlmDecisionRecord,
  TradeOrigin,
  TradeRecord,
} from "../types";

type HistoryTab = "real" | "llm" | "shadow";
type HistoryItem = TradeRecord | LlmDecisionRecord;

const TAB_LABEL: Record<HistoryTab, string> = {
  real: "실거래",
  llm: "LLM 판단",
  shadow: "섀도",
};

const ORIGIN_LABEL: Record<TradeOrigin, string> = {
  manual: "수동",
  auto: "자동",
  auto_handoff: "자동→수동",
  shadow: "섀도",
  external: "외부",
};

const DECISION_PRODUCT_LABEL: Record<DecisionScenarioProduct, string> = {
  leverage: "레버리지",
  inverse: "곱버스",
};

function formatDateTime(fakeKstEpoch: number | null | undefined): string {
  if (!fakeKstEpoch) return "-";
  const date = new Date(fakeKstEpoch * 1000);
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hour = String(date.getUTCHours()).padStart(2, "0");
  const minute = String(date.getUTCMinutes()).padStart(2, "0");
  const second = String(date.getUTCSeconds()).padStart(2, "0");
  return `${month}-${day} ${hour}:${minute}:${second}`;
}

function fetchPage(tab: HistoryTab, cursor: HistoryCursor): Promise<CursorPage<HistoryItem>> {
  if (tab === "llm") return listLlmDecisions(cursor);
  return listTradeRecords(tab, cursor);
}

function TradeRecordRow({ record }: { record: TradeRecord }) {
  return (
    <article className="history-card trade-record-card">
      <div className="history-card-head">
        <span className={`origin-tag origin-${record.origin}`}>{ORIGIN_LABEL[record.origin]}</span>
        <b>{record.code}</b>
        <time>{formatDateTime(record.enteredAt)}</time>
      </div>
      <div className="history-prices">
        <span>{record.entryQty.toLocaleString("ko-KR")}주</span>
        <span>진입 {formatPrice(record.entryAvgPrice)}</span>
        <span>청산 {record.exitAvgPrice ? formatPrice(record.exitAvgPrice) : "보유 중"}</span>
      </div>
      <div className="history-result">
        {record.pnlRate !== null && record.pnlRate !== undefined ? (
          <b className={rateClass(record.pnlRate)}>{formatRate(record.pnlRate)}</b>
        ) : (
          <span className="flat">수익률 대기</span>
        )}
        {record.exitReason ? <span>{record.exitReason}</span> : null}
        {record.exitedAt ? <time>{formatDateTime(record.exitedAt)}</time> : null}
      </div>
    </article>
  );
}

function DecisionRecordRow({ record }: { record: LlmDecisionRecord }) {
  const selected = record.scenarios.find((scenario) => scenario.status === "triggered");
  return (
    <article className="history-card decision-record-card">
      <div className="history-card-head">
        <span className="origin-tag origin-auto">LLM</span>
        <b>{record.status}</b>
        <time>{formatDateTime(record.asOfTs)}</time>
      </div>
      <div className="decision-scenarios">
        {record.scenarios.length === 0 ? (
          <span className="flat">SKIP</span>
        ) : (
          record.scenarios.slice(0, 2).map((scenario, index) => (
            <div key={scenario.id ?? `${scenario.product}-${index}`}>
              <span>{scenario.product === "leverage" ? "↑" : "↓"}</span>
              <b>{DECISION_PRODUCT_LABEL[scenario.product]}</b>
              <span>{formatPrice(scenario.triggerPrice)}</span>
              <span>+{scenario.targetReturnPct.toFixed(1)}%</span>
              <span className={`decision-status status-${scenario.status}`}>{scenario.status}</span>
            </div>
          ))
        )}
      </div>
      <div className="decision-meta">
        {selected ? <span>선택 {DECISION_PRODUCT_LABEL[selected.product]}</span> : null}
        {record.inputTokens > 0 || record.cacheWriteTokens > 0 || record.outputTokens > 0 ? (
          <span>{formatLlmTokenUsage(record)}</span>
        ) : null}
        <span>{record.latencyMs}ms</span>
      </div>
      {record.error ? <div className="history-error">{record.error}</div> : null}
    </article>
  );
}

export default function HistoryModal() {
  const setOpen = useUiStore((s) => s.setHistoryOpen);
  const historyRevision = useUiStore((s) => s.historyRevision);
  const [tab, setTab] = useState<HistoryTab>("real");
  const [items, setItems] = useState<HistoryItem[]>([]);
  const [nextCursor, setNextCursor] = useState<HistoryCursor>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const requestSeq = useRef(0);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpen(false);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [setOpen]);

  useEffect(() => {
    const seq = ++requestSeq.current;
    setItems([]);
    setNextCursor(null);
    setError(null);
    setLoading(true);
    void fetchPage(tab, null)
      .then((page) => {
        if (requestSeq.current !== seq) return;
        setItems(page.items);
        setNextCursor(page.nextCursor);
      })
      .catch((reason) => {
        if (requestSeq.current === seq) setError(String(reason));
      })
      .finally(() => {
        if (requestSeq.current === seq) setLoading(false);
      });
  }, [tab, historyRevision]);

  const loadMore = async () => {
    if (loading || nextCursor === null) return;
    const seq = ++requestSeq.current;
    setLoading(true);
    setError(null);
    try {
      const page = await fetchPage(tab, nextCursor);
      if (requestSeq.current !== seq) return;
      setItems((current) => [...current, ...page.items]);
      setNextCursor(page.nextCursor);
    } catch (reason) {
      if (requestSeq.current === seq) setError(String(reason));
    } finally {
      if (requestSeq.current === seq) setLoading(false);
    }
  };

  return (
    <div className="modal-backdrop history-backdrop" role="presentation" onMouseDown={() => setOpen(false)}>
      <div
        className="modal history-modal"
        role="dialog"
        aria-modal="true"
        aria-label="거래 및 판단 기록"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="history-titlebar">
          <b>기록</b>
          <button aria-label="기록 닫기" onClick={() => setOpen(false)}>
            ✕
          </button>
        </div>
        <div className="history-tabs">
          {(Object.keys(TAB_LABEL) as HistoryTab[]).map((value) => (
            <button
              key={value}
              className={tab === value ? "active" : ""}
              onClick={() => setTab(value)}
            >
              {TAB_LABEL[value]}
            </button>
          ))}
        </div>
        <div className="history-list">
          {items.map((item) =>
            "asOfTs" in item ? (
              <DecisionRecordRow key={`decision-${item.id}`} record={item} />
            ) : (
              <TradeRecordRow key={`trade-${item.id}`} record={item} />
            ),
          )}
          {loading && items.length === 0 ? <div className="history-message">불러오는 중…</div> : null}
          {!loading && !error && items.length === 0 ? <div className="history-message">기록이 없습니다</div> : null}
          {error ? <div className="history-message history-error">불러오기 실패: {error}</div> : null}
          {nextCursor !== null ? (
            <button className="history-more" disabled={loading} onClick={() => void loadMore()}>
              {loading ? "불러오는 중…" : "더 보기"}
            </button>
          ) : null}
        </div>
      </div>
    </div>
  );
}
