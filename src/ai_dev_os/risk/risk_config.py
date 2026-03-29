from dataclasses import dataclass


@dataclass
class ATRConfig:
    # ATR 计算周期（交易日）
    # 行业参考：Wilder 原始设计为14日，是趋势跟踪系统最常用值
    period: int = 14
    # 止损距离倍数：stop_price = entry_price - ATR * multiplier
    # 行业参考：2.0 是 Van Tharp / Turtle Trading 经典设置
    multiplier: float = 2.0


@dataclass
class PositionSizingConfig:
    # 每笔交易最大风险敞口（占账户净值比例）
    # 行业参考：专业趋势跟踪系统通常用 0.5%-2%，保守起步用 1%
    risk_per_trade: float = 0.01
    # 仓位上限（防止极低波动时计算出超大仓位）
    # 行业参考：单标的不超过账户 60% 是常见上限
    max_position_fraction: float = 0.60
    # 仓位下限（低于此值跳过交易，避免极小仓位）
    min_position_fraction: float = 0.05
