import { invoke } from "@tauri-apps/api/core";
import type { AccountSnapshot, Candle, OrderResult, Settings } from "../types";

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
