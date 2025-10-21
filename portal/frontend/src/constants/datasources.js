export const DATASOURCE_IDS = {
  ALPACA: 'ALPACA',
  IBKR: 'IBKR',
  CCXT: 'CCXT',
};

export const DATASOURCE_OPTIONS = [
  { value: DATASOURCE_IDS.ALPACA, label: 'Markets (Alpaca)' },
  { value: DATASOURCE_IDS.IBKR, label: 'Interactive Brokers (TWS)' },
  { value: DATASOURCE_IDS.CCXT, label: 'Crypto (CCXT)' },
];

export const MARKET_PROVIDERS = [
  { value: 'alpaca', label: 'Alpaca (Equities)' },
  { value: 'yfinance', label: 'Yahoo Finance' },
];

export const IB_EXCHANGES = [
  { value: 'SMART', label: 'SMART Routing', description: 'IBKR smart order router' },
  { value: 'NYSE', label: 'NYSE', description: 'New York Stock Exchange' },
  { value: 'NASDAQ', label: 'NASDAQ', description: 'Nasdaq Global Select Market' },
  { value: 'ARCA', label: 'ARCA', description: 'NYSE Arca equities' },
  { value: 'CBOE', label: 'CBOE', description: 'Cboe Global Markets' },
  { value: 'GLOBEX', label: 'GLOBEX', description: 'CME Globex futures' },
  { value: 'NYMEX', label: 'NYMEX', description: 'NY Mercantile Exchange futures' },
];

export const CRYPTO_EXCHANGES = [
  { value: 'binanceus', label: 'Binance US', category: 'CEX' },
  { value: 'coinbase', label: 'Coinbase Advanced', category: 'CEX' },
  { value: 'kraken', label: 'Kraken', category: 'CEX' },
  { value: 'gemini', label: 'Gemini', category: 'CEX' },
  { value: 'kucoin', label: 'KuCoin', category: 'CEX' },
  { value: 'bitfinex', label: 'Bitfinex', category: 'CEX' },
  { value: 'deribit', label: 'Deribit', category: 'CEX' },
  { value: 'apex', label: 'Apex', category: 'DEX' },
  { value: 'defx', label: 'DefX', category: 'DEX' },
  { value: 'hyperliquid', label: 'Hyperliquid', category: 'DEX' },
  { value: 'woofipro', label: 'Woofi Pro', category: 'DEX' },
  { value: 'wavesexchange', label: 'Waves Exchange', category: 'DEX' },
];

export const DEFAULT_DATASOURCE = DATASOURCE_IDS.ALPACA;
export const DEFAULT_MARKET_PROVIDER = 'alpaca';
export const DEFAULT_CRYPTO_EXCHANGE = 'binanceus';
export const DEFAULT_IB_EXCHANGE = 'SMART';
