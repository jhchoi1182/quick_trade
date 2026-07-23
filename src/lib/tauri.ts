import { invoke } from "@tauri-apps/api/core";
import type {
  AccountSnapshot,
  AutomationSnapshot,
  Candle,
  ControlMode,
  CursorPage,
  HistoryCursor,
  LlmDecisionRecord,
  OrderResult,
  Reservation,
  RuntimeResyncResult,
  Settings,
  TradeRecord,
  TradeRecordKind,
} from "../types";

export function getSettings(): Promise<Settings> {
  return invoke<Settings>("get_settings");
}

export function saveSettings(settings: Settings): Promise<void> {
  return invoke<void>("save_settings", { settings });
}

export function getCandles(code: string): Promise<Candle[]> {
  return invoke<Candle[]>("get_candles", { code });
}

export function getAccount(): Promise<AccountSnapshot> {
  return invoke<AccountSnapshot>("get_account");
}

export function buyMax(code: string): Promise<OrderResult> {
  return invoke<OrderResult>("buy_max", { code });
}

export function sellAll(code: string): Promise<OrderResult> {
  return invoke<OrderResult>("sell_all", { code });
}

export function placeReservedSell(code: string, targetPct: number): Promise<OrderResult> {
  return invoke<OrderResult>("place_reserved_sell", { code, targetPct });
}

export function cancelReservedSell(code: string): Promise<OrderResult> {
  return invoke<OrderResult>("cancel_reserved_sell", { code });
}

export function getReservations(): Promise<Reservation[]> {
  return invoke<Reservation[]>("get_reservations");
}

export function getAutomationStatus(): Promise<AutomationSnapshot> {
  return invoke<AutomationSnapshot>("get_automation_status");
}

export function setControlMode(mode: ControlMode): Promise<AutomationSnapshot> {
  return invoke<AutomationSnapshot>("set_control_mode", { mode });
}

export function resetRuntimeAndResync(): Promise<RuntimeResyncResult> {
  return invoke<RuntimeResyncResult>("reset_runtime_and_resync");
}

function normalizePage<T>(value: CursorPage<T> | T[]): CursorPage<T> {
  return Array.isArray(value) ? { items: value, nextCursor: null } : value;
}

export async function listTradeRecords(
  kind: TradeRecordKind,
  cursor: HistoryCursor = null,
  limit = 40,
): Promise<CursorPage<TradeRecord>> {
  const page = await invoke<CursorPage<TradeRecord> | TradeRecord[]>("list_trade_records", {
    kind,
    cursor,
    limit,
  });
  return normalizePage(page);
}

export async function listLlmDecisions(
  cursor: HistoryCursor = null,
  limit = 40,
): Promise<CursorPage<LlmDecisionRecord>> {
  const page = await invoke<CursorPage<LlmDecisionRecord> | LlmDecisionRecord[]>("list_llm_decisions", {
    cursor,
    limit,
  });
  return normalizePage(page);
}
