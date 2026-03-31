from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class BaseStrategy(ABC):
    """所有策略的基类，统一输入输出接口。"""

    strategy_id: str = ""
    strategy_name: str = ""
    strategy_type: str = ""

    @abstractmethod
    def generate_signals(self, data: pd.DataFrame) -> Any:
        """T 日收盘计算，T+1 日执行。"""

    def get_config(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "strategy_type": self.strategy_type,
        }

    def entry_summary(self) -> str:
        return self.strategy_name

    def exit_summary(self) -> str:
        return "strategy_specific_exit"
