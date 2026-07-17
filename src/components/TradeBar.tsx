import { useAccountStore } from "../stores/accountStore";
import { useMarketStore } from "../stores/marketStore";
import { useSettingsStore } from "../stores/settingsStore";
import { useUiStore } from "../stores/uiStore";
import { formatPrice, formatRate, rateClass } from "../lib/format";

export function TradeBar() {
  const tradeSymbols = useSettingsStore((s) => s.settings?.tradeSymbols ?? []);
  const tradeCode = useUiStore((s) => s.tradeCode);
  const setTradeCode = useUiStore((s) => s.setTradeCode);
  const quote = useMarketStore((s) => s.quotes[tradeCode]);
  const positions = useAccountStore((s) => s.positions);

  return (
    <div className="trade-bar">
      <select
        className="selector trade-selector"
        value={tradeCode}
        onChange={(e) => setTradeCode(e.target.value)}
        title="매매 종목"
      >
        {tradeSymbols.map((sym) => {
          const held = (positions[sym.code]?.qty ?? 0) > 0;
          return (
            <option key={sym.code} value={sym.code}>
              {held ? `● ${sym.label}` : sym.label}
            </option>
          );
        })}
      </select>
      {quote ? (
        <div className={`trade-price ${rateClass(quote.changeRate)}`}>
          <span className="price">{formatPrice(quote.price)}</span>
          <span className="rate">{formatRate(quote.changeRate)}</span>
        </div>
      ) : (
        <div className="trade-price flat">
          <span className="price">-</span>
        </div>
      )}
    </div>
  );
}
