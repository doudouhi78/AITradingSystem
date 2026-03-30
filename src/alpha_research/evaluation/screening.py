from __future__ import annotations


def screen_factor(ic_result: dict) -> tuple[bool, str]:
    ic_10d = abs(ic_result.get("ic_mean", {}).get("10", 0.0))
    if ic_10d <= 0.03:
        return False, f"IC均值不足：{ic_10d:.4f} <= 0.03"

    icir_10d = abs(ic_result.get("icir", {}).get("10", 0.0))
    if icir_10d <= 0.5:
        return False, f"ICIR不足：{icir_10d:.4f} <= 0.5"

    halflife = ic_result.get("decay_halflife")
    if halflife is None or halflife <= 5:
        return False, f"衰减过快：半衰期={halflife} <= 5日"

    return True, "通过全部筛选条件"
