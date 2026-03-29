# AI Trading System Starter

This starter project was bootstrapped from the LangGraphLab agent engine.

## What is included

- `src/ai_dev_os/`: reusable agent engine code copied from the current system
- `memory/roles/trading_zone/`: a fresh two-role trading starter pack
- `config/agents.json`: sanitized config skeleton
- `docs/`: bootstrap notes and next-step guidance

## Starter roles

1. `market_architect`
   - Expands trading goals into structured research / system tasks
   - Clarifies assumptions, risk boundaries, and phase priorities
2. `strategy_operator`
   - Executes concrete trading research / implementation tasks
   - Produces structured outputs, raises checkpoints when decisions are needed

## Important

This is a starter scaffold, not a finished trading system.
The engine code is copied in so the project can evolve independently from LangGraphLab.
