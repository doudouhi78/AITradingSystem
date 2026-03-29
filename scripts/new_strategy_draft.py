from __future__ import annotations

from datetime import datetime
from pprint import pprint


def main() -> None:
    card = {
        "card_id": "DRAFT-20260329-001",
        "title": "510300 双均线交叉草稿",
        "instrument": "510300",
        "strategy_family": "trend_following",
        "hypothesis": "当宽基 ETF 进入中期趋势阶段时，短周期均线会先于长周期均线拐头并形成延续。双均线交叉能用简单规则捕捉趋势开始，并在趋势减弱时退出。",
        "entry_rule": "MA10 上穿 MA30，次日开盘买入",
        "exit_rule": "MA10 下穿 MA30，次日开盘卖出",
        "key_params": {"fast_window": 10, "slow_window": 30},
        "created_at": datetime.now().astimezone().isoformat(),
    }
    pprint(card)


if __name__ == "__main__":
    main()
