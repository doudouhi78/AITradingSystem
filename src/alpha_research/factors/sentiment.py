from __future__ import annotations

import pandas as pd


def factor_northbound_flow_5d(*args, **kwargs) -> pd.Series:
    """TODO Phase 2 后期补充：北向资金接口在当前 AkShare 版本不稳定。"""
    raise NotImplementedError("northbound flow factor is pending Phase 2 later sprint")


def factor_margin_balance_change_5d(*args, **kwargs) -> pd.Series:
    """TODO Phase 2 后期补充：融资融券接口待统一。"""
    raise NotImplementedError("margin balance factor is pending Phase 2 later sprint")
