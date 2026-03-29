# V1 Market Data Field Spec

## Purpose

This document defines the minimum market data field set for the first usable research data layer.

The goal of V1 is:

- support stock pool and ETF pool filtering
- support 10-year daily research data
- support basic cleaning and validation
- support later offline backtest input

V1 does not include:

- minute or tick data
- complex point-in-time financial fields
- deep corporate action reconstruction
- futures continuous contract rules

## V1 Data Policy

- Stocks: qfq daily bars
- ETFs: qfq daily bars
- Price fields: `open`, `high`, `low`, `close`
- Turnover fields: `volume`, `amount`
- Suspension handling: keep trading dates, do not fabricate prices
- Missing values: only allowed in warmup handling; otherwise fail validation

## Layer 1: Market Bar Data

These fields are required for every stock and ETF daily record.

| Field | Type | Meaning |
|---|---|---|
| `market` | string | Market code, e.g. `CN`, `US` |
| `symbol` | string | Unique tradable symbol |
| `security_type` | string | `stock` or `etf` |
| `trade_date` | string | Trading date in ISO format |
| `open` | float | Adjusted open price |
| `high` | float | Adjusted high price |
| `low` | float | Adjusted low price |
| `close` | float | Adjusted close price |
| `volume` | float | Daily traded volume |
| `amount` | float | Daily traded turnover amount |
| `adjustment_mode` | string | Fixed as `qfq` in V1 |
| `is_suspended` | bool | Whether the instrument is suspended on the trading date |
| `listed_date` | string | Listing date |
| `delisted_date` | string | Delisting date, empty if still listed |

## Layer 2: Security Attributes

These fields are used for pool filtering, not as daily bar fields.

### Stock Attributes

| Field | Type | Meaning |
|---|---|---|
| `exchange` | string | Exchange code |
| `industry_level_1` | string | Primary industry classification |
| `industry_level_2` | string | Secondary industry classification |
| `market_cap` | float | Total market capitalization |
| `float_market_cap` | float | Float market capitalization |
| `is_st` | bool | Whether the stock is ST or *ST |

### ETF Attributes

| Field | Type | Meaning |
|---|---|---|
| `exchange` | string | Exchange code |
| `etf_category` | string | Broad category, e.g. broad index, sector, theme |
| `etf_theme` | string | Strategy or theme label |
| `aum` | float | Assets under management |
| `benchmark_index` | string | Linked benchmark index |

## Usage Boundary

V1 should be used for:

- stock elite pool filtering
- ETF pool filtering
- daily strategy research
- first-pass validation and backtesting input

V1 should not yet be used as proof of:

- point-in-time corporate action completeness
- fundamental factor correctness
- execution-level simulation correctness
- minute or intraday strategy readiness
