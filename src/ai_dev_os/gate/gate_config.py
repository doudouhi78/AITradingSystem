from dataclasses import dataclass


@dataclass
class DrawdownGateConfig:
    # 组合从峰值回撤超过此值则暂停新入场
    # 行业参考：趋势跟踪系统通常用 10%-15%，默认取中间值
    max_drawdown_threshold: float = 0.12


@dataclass
class TrendGateConfig:
    # 大盘指数均线窗口（交易日）
    # 行业参考：120日为半年线，是趋势系统最常用的大盘过滤
    ma_window: int = 120
    # 指数代码（510300=沪深300ETF，作为大盘代理）
    index_instrument: str = "510300"


@dataclass
class BreadthGateConfig:
    # 上涨股票占比低于此值则关闭入场
    # 行业参考：50%为多空平衡线，40%表示明显偏空
    min_adv_ratio: float = 0.40
    # 计算宽度时使用的股票池（None=使用全部已有Parquet数据）
    universe_filter: str = "all"


@dataclass
class VolGateConfig:
    # 波动率高于历史分位数此值时限仓
    # 行业参考：80%分位是常见的"高波动"阈值
    vol_percentile_threshold: float = 0.80
    # 计算滚动波动率的窗口（交易日）
    vol_window: int = 20
    # 历史分位数回溯窗口（交易日）
    vol_lookback: int = 252


@dataclass
class GateSchedulerConfig:
    # Gate合并策略：strict=任一触发即阻断，layered=按优先级
    merge_strategy: str = "strict"
