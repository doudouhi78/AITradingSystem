# V1 Market Pool Filtering Rules

## Scope

This document defines the first-pass filtering rules for four research pools:

- China ETF pool
- China stock elite pool
- US ETF pool
- US stock elite pool

These are hard filters for V1.
They are designed to remove obviously poor trading targets first.
They are not alpha rules.

## 1. China ETF Pool V1

### Keep Direction

Prefer:

- broad index ETFs
- major sector ETFs
- repeatable long-life theme ETFs

### Hard Filters

- listed for at least 1 year
- fund size `>= 5 billion CNY`
- 60-day average turnover `>= 30 million CNY`
- exclude leveraged ETFs
- exclude inverse ETFs
- exclude obvious one-wave thematic ETFs

### Preferred Whitelist Direction

- CSI 300
- CSI 500
- CSI 1000
- ChiNext
- STAR 50
- broker
- semiconductor
- healthcare
- consumption
- dividend
- non-ferrous metals
- energy

## 2. China Stock Elite Pool V1

### Hard Filters

- exclude `ST` and `*ST`
- exclude stocks with persistent losses in recent 2 years
- exclude total market cap `< 8 billion CNY`
- exclude 60-day average turnover `< 100 million CNY`
- exclude long-term suspended or liquidity-abnormal stocks
- exclude obvious sunset industries
- exclude pure one-shot theme-driven stocks

### Keep Direction

Prefer companies with:

- industry growth space
- real earnings ability
- business clarity
- not just story-driven valuation
- market structure suitable for medium-term trading

## 3. US ETF Pool V1

### Keep Direction

Prefer:

- broad index ETFs
- major sector ETFs
- long-life thematic ETFs

### Hard Filters

- listed for at least 1 year
- AUM `>= 100 million USD`
- 60-day average turnover `>= 5 million USD`
- exclude leveraged ETFs
- exclude inverse ETFs
- exclude illiquid niche ETFs

### Preferred Whitelist Direction

- SPY
- QQQ
- IWM
- DIA
- XLK
- XLE
- XLF
- XLV
- XLI
- SMH
- SOXX

## 4. US Stock Elite Pool V1

### Hard Filters

- exclude OTC and obvious shell-like stocks
- exclude total market cap `< 2 billion USD`
- exclude 60-day average turnover `< 10 million USD`
- exclude companies with persistent losses and worsening fundamentals
- exclude extreme event-only story stocks
- exclude structurally broken charts with frequent abnormal gaps

### Keep Direction

Prefer companies with:

- medium to large market cap
- strong liquidity
- clear business position
- explainable fundamentals
- price structure suitable for trend or medium-term trading

## Usage Rule

V1 filtering rule is used to build tradable pools first.
It is not used to prove strategy edge.

The order is:

1. filter the pool
2. build the daily data layer
3. run later strategy drafting and validation
