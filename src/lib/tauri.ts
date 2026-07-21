import { invoke } from "@tauri-apps/api/core";
import type { AccountSnapshot, Candle, OrderResult, Reservation, Settings } from "../types";

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
