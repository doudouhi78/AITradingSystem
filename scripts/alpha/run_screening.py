from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(r"D:\AITradingSystem")
IN_PATH = ROOT / "runtime" / "alpha_research" / "phase2" / "ic_batch_result.json"
REGISTRY_PATH = ROOT / "src" / "alpha_research" / "registry" / "factor_registry.json"

CATEGORY_MAP = {
    "momentum": "price_momentum",
    "turnover": "volume_liquidity",
    "volume": "volume_liquidity",
    "volatility": "volume_liquidity",
    "pb": "fundamental",
    "market_cap": "fundamental",
    "rsi": "technical",
    "atr": "technical",
    "macd": "technical",
    "bbands": "technical",
}


def infer_category(factor_name: str) -> str:
    for key, category in CATEGORY_MAP.items():
        if key in factor_name:
            return category
    return "unknown"


def build_hypothesis(factor_name: str) -> str:
    if "52wk" in factor_name:
        return "接近52周高点的标的更容易延续强势"
    if "momentum_1d_reversal" in factor_name:
        return "短期超跌反弹存在1日反转效应"
    if "momentum" in factor_name:
        return "价格延续性使过去强势标的未来继续跑赢"
    if "turnover_acceleration" in factor_name:
        return "成交额加速扩张代表资金关注度提升"
    if "turnover" in factor_name:
        return "成交持续活跃的标的更容易保持强势"
    if "volume_price_divergence" in factor_name:
        return "价量同向强化优于价量背离"
    if "volatility" in factor_name:
        return "波动结构本身包含风险补偿或拥挤信息"
    if "market_cap" in factor_name:
        return "小市值效应可能带来超额收益"
    if "rsi" in factor_name:
        return "相对强弱指标包含趋势/超买超卖信息"
    if "atr" in factor_name:
        return "归一化波动率可区分真实趋势与噪声"
    if "macd" in factor_name:
        return "MACD 柱状值反映趋势动能变化"
    if "bbands" in factor_name:
        return "价格在布林带中的位置反映趋势与拥挤度"
    return "待补充"


def build_trading_implication(factor_name: str) -> str:
    if "market_cap" in factor_name:
        return "优先关注小市值一侧，并结合流动性过滤"
    if "volatility" in factor_name:
        return "结合波动率约束做风险过滤或排序"
    return "作为横截面排序因子，结合后续组合权重优化使用"


def main() -> None:
    if not IN_PATH.exists():
        raise FileNotFoundError(IN_PATH)
    payload = json.loads(IN_PATH.read_text(encoding="utf-8"))
    registry = []
    summary = {}
    for universe_name, universe_payload in payload.get("universes", {}).items():
        results = universe_payload.get("results", [])
        passed = []
        rejected = []
        for item in results:
            if item.get("passed"):
                passed.append(item["factor_name"])
                registry.append({
                    "factor_id": item["factor_name"],
                    "category": infer_category(item["factor_name"]),
                    "hypothesis": build_hypothesis(item["factor_name"]),
                    "universe": universe_name,
                    "ic_mean_10d": item.get("ic_mean", {}).get("10", 0.0),
                    "icir_10d": item.get("icir", {}).get("10", 0.0),
                    "decay_halflife_days": item.get("decay_halflife"),
                    "status": "入库",
                    "trading_implication": build_trading_implication(item["factor_name"]),
                })
            else:
                rejected.append({
                    "factor_name": item["factor_name"],
                    "reason": item.get("screen_reason") or item.get("error"),
                })
        summary[universe_name] = {
            "passed": passed,
            "rejected_count": len(rejected),
            "deduplicated_passed_factors": universe_payload.get("deduplicated_passed_factors", []),
        }

    REGISTRY_PATH.write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({
        "registry_count": len(registry),
        "summary": summary,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
