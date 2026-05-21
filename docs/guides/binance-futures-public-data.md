# Binance Futures Public Data Setup

This guide defines the v1 setup for using Binance public futures archives as a
Quant-Trad research data source.

Status: the Quant-Trad file/archive ingestion provider is not implemented yet.
This guide is the target operator contract for that provider.

## Goal

Use Binance USD-M perpetual futures archives as a simple derivative research
market without account setup, broker gateways, futures rollovers, or paid data
subscriptions.

The intended flow is:

```text
Binance public zip archive
  -> Quant-Trad provider ingestion
  -> normalized candle store
  -> normal bot backtest/runtime/report path
```

Backtests should never read Binance files directly. Files are source inputs to
ingestion only. The canonical runtime source remains Quant-Trad's persisted
candle catalog.

## Where The User Is Needed

No Binance account is required for this public historical data path.

You only need to provide:

- internet access from the machine doing ingestion,
- the research universe, such as `BTCUSDT,ETHUSDT,SOLUSDT`,
- the timeframe, such as `1h`,
- the date range to backfill,
- confirmation that Binance public futures data is acceptable as a research
  source even if Binance execution is not the target venue.

Do not create Binance API keys for this v1. Execution, account balances, private
WebSocket streams, and live order placement are out of scope.

## Source Data

Binance publishes public market data at:

- `https://data.binance.vision`
- `https://github.com/binance/binance-public-data`

The public data repository states that daily and monthly files are available,
all symbols are supported, and futures kline files are sourced from the futures
API endpoints.

For v1, use USD-M futures monthly kline archives:

```text
https://data.binance.vision/data/futures/um/monthly/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{YYYY-MM}.zip
```

Example:

```text
https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip
```

Checksum files sit beside the zip:

```text
https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip.CHECKSUM
```

Observed checksum format:

```text
bf673f3d10804a951e8bac56dd2473486f113025971d43ebe5258ec40f9bfeb3  BTCUSDT-1h-2024-01.zip
```

Archive listing can be read from the public S3 XML endpoint:

```text
https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/monthly/klines/BTCUSDT/1h/
```

The listing returns `Contents` entries with fields like `Key`,
`LastModified`, `ETag`, `Size`, and `StorageClass`.

## CSV Format

A monthly kline zip contains one CSV file.

Observed file name:

```text
BTCUSDT-1h-2024-01.csv
```

Observed header:

```csv
open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore
```

Observed row:

```csv
1704067200000,42314.00,42603.20,42289.60,42503.50,8459.477,1704070799999,359196345.08716,88278,4687.976,199033806.82405,0
```

Normalize this into Quant-Trad candles as:

| Binance column | Quant-Trad meaning |
| --- | --- |
| `open_time` | candle timestamp, Unix milliseconds, UTC |
| `open` | open |
| `high` | high |
| `low` | low |
| `close` | close |
| `volume` | base asset volume |
| `quote_volume` | quote asset volume, provider metadata |
| `count` | number of trades, provider metadata |
| `close_time` | provider metadata |
| `taker_buy_volume` | provider metadata |
| `taker_buy_quote_volume` | provider metadata |

Do not synthesize missing rows. Missing candles must remain visible to
continuity checks.

## V1 Scope

Use only:

- market type: `um` / USD-M futures,
- contract type: perpetual futures,
- archive type: `monthly/klines`,
- interval: start with `1h`,
- symbols without delivery suffixes, such as `BTCUSDT` or `ETHUSDT`.

Avoid in v1:

- COIN-M futures,
- delivery/quarterly contracts,
- symbols with expiry suffixes,
- live Binance futures API,
- private account APIs,
- order execution,
- funding-rate modeling,
- custom synthetic continuous contracts.

Funding should be reported as a known caveat until Quant-Trad models it
explicitly.

## Intended Provider Identity

Recommended provider and venue names:

```text
provider_id: BINANCE_FUTURES_PUBLIC_DATA
venue_id: BINANCE_USDM_PUBLIC
asset_class: crypto_derivatives
market_type: um
contract_type: perpetual
supportsHistorical: true
supportsLive: false
supportsOrders: false
required_secrets: []
```

The provider should classify these instruments as derivative instruments that
can short and may have funding, but it must not imply live execution support.

## Intended Local Layout

Use ignored local storage for raw archive cache and operation logs:

```text
logs/data/binance_futures_public_data/
  raw/
    futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip
    futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip.CHECKSUM
  manifests/
  ingest-events.ndjson
```

The raw cache is disposable. The normalized candle store is the canonical source
after ingestion.

Every ingest operation should record:

- provider id,
- venue id,
- symbol,
- interval,
- requested start/end,
- source URL,
- archive file name,
- checksum status,
- row count,
- first/last candle timestamp,
- duplicate timestamp count,
- continuity summary,
- error code/message when applicable.

## Intended CLI

These commands are the target interface after the provider is implemented:

```bash
qt data providers doctor binance-futures-public-data
```

Checks:

- archive host is reachable,
- sample listing can be read,
- sample zip can be downloaded,
- checksum can be verified,
- CSV columns match the expected kline format,
- database connection is available through `PG_DSN`.

```bash
qt data backfill \
  --provider binance-futures-public-data \
  --venue binance-usdm-public \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT \
  --timeframe 1h \
  --start 2024-01-01T00:00:00Z \
  --end 2024-12-31T23:59:59Z
```

Downloads matching monthly archives, verifies checksums, normalizes rows, writes
to the candle store, and emits an ingest manifest.

```bash
qt data coverage \
  --provider binance-futures-public-data \
  --venue binance-usdm-public \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT \
  --timeframe 1h \
  --start 2024-01-01T00:00:00Z \
  --end 2024-12-31T23:59:59Z
```

Reports row counts, expected counts, gaps, duplicates, first/last timestamps,
and source provenance.

Until these commands exist, use this guide as the implementation contract, not
as a runnable command reference.

## First Universe

Start small. Recommended initial universe:

```text
BTCUSDT
ETHUSDT
SOLUSDT
BNBUSDT
XRPUSDT
```

After the ingestion path is stable, expand cautiously:

```text
DOGEUSDT
ADAUSDT
AVAXUSDT
LINKUSDT
LTCUSDT
```

Do not ingest every archived symbol as the first pass. The goal is to validate
the provider boundary and research loop, not maximize the symbol universe.

## Suggested First Backfill

Use one liquid symbol, one timeframe, and one complete historical year:

```bash
qt data backfill \
  --provider binance-futures-public-data \
  --venue binance-usdm-public \
  --symbols BTCUSDT \
  --timeframe 1h \
  --start 2024-01-01T00:00:00Z \
  --end 2024-12-31T23:59:59Z
```

Then run coverage:

```bash
qt data coverage \
  --provider binance-futures-public-data \
  --venue binance-usdm-public \
  --symbols BTCUSDT \
  --timeframe 1h \
  --start 2024-01-01T00:00:00Z \
  --end 2024-12-31T23:59:59Z
```

Only after coverage is clean should bots or experiment plans point at the new
provider/venue.

## Manual Smoke Checks

These checks require no Quant-Trad provider implementation.

Read an archive listing:

```bash
python -c "import urllib.request; url='https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/monthly/klines/BTCUSDT/1h/'; print(urllib.request.urlopen(url, timeout=30).read().decode('utf-8')[:1000])"
```

Inspect one zip and first rows:

```bash
python -c "import urllib.request, zipfile, io; url='https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip'; raw=urllib.request.urlopen(url, timeout=30).read(); z=zipfile.ZipFile(io.BytesIO(raw)); name=z.namelist()[0]; print(name); print('\\n'.join(z.read(name).decode('utf-8').splitlines()[:5]))"
```

Read checksum:

```bash
python -c "import urllib.request; url='https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip.CHECKSUM'; print(urllib.request.urlopen(url, timeout=30).read().decode('utf-8'))"
```

## Runtime And Reporting Provenance

Runs using this data should preserve:

```text
data_provider: BINANCE_FUTURES_PUBLIC_DATA
venue: BINANCE_USDM_PUBLIC
market_type: um
contract_type: perpetual
source_archive: Binance public data
execution_mode: simulated backtest
live_execution_supported: false
funding_mode: not modeled
```

Reports should make it clear that Binance public archives are the data source
and Quant-Trad simulation is the execution model.

## Failure Rules

Fail loud when:

- checksum verification fails,
- zip is unreadable,
- CSV header is unexpected,
- timestamps are not parseable as Unix milliseconds,
- OHLC values are missing or non-numeric,
- duplicate timestamps cannot be resolved deterministically,
- requested archive files are missing,
- persistence is unavailable.

Warn, but keep evidence, when:

- a symbol has no archive for part of the requested range,
- latest current-month data is incomplete,
- source files were re-downloaded with a changed checksum,
- coverage has gaps after successful ingestion.

## What Not To Do

- Do not use Binance execution APIs for this v1.
- Do not require Binance account credentials.
- Do not read zip files directly from runtime.
- Do not fill missing candles silently.
- Do not mix Coinbase and Binance candles under the same provider identity.
- Do not model funding as a hidden fee.
- Do not expand to all archived symbols before the first small universe is clean.

## Next Implementation Step

Implement one provider-backed ingestion path:

```text
BinanceFuturesPublicDataProvider.fetch_from_api(...)
  -> archive URL planner
  -> checksum verifier
  -> kline CSV parser
  -> canonical DataFrame
  -> existing DataPersistence.write_dataframe(...)
```

The CLI should remain thin and call backend/provider contracts rather than
duplicating parsing or persistence logic.

