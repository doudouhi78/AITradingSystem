from ai_dev_os.idea_drafting import (
    draft_from_human_idea,
    draft_from_market_scan,
    draft_from_mature_edge,
    validate_human_idea_input,
)


def test_validate_human_idea_input():
    payload = validate_human_idea_input(
        {
            "input_id": "HUM-001",
            "title": "AI主线强势",
            "raw_idea": "AI相关股票最近持续走强",
            "market": "A股",
            "asset_scope": ["AI龙头", "算力ETF"],
            "timeframe_hint": "日线",
            "why_it_matters": "主线资金集中",
            "created_at": "2026-03-28T20:00:00+08:00",
        }
    )
    assert payload["market"] == "A股"


def test_draft_from_human_idea():
    draft = draft_from_human_idea(
        {
            "input_id": "HUM-001",
            "title": "AI主线强势",
            "raw_idea": "AI相关股票最近持续走强",
            "market": "A股",
            "asset_scope": ["AI龙头", "算力ETF"],
            "timeframe_hint": "日线",
            "why_it_matters": "主线资金集中",
            "created_at": "2026-03-28T20:00:00+08:00",
        },
        draft_id="DRAFT-001",
        loser_definition="追高后接盘的短线资金",
        profit_source_definition="主线趋势延续",
        entry_rule_prototype="创20日新高且成交放大时进入",
        exit_rule_prototype="跌破10日低点退出",
        stop_rule_prototype="跌破入场价3%止损",
        failure_conditions=["题材退潮", "成交量萎缩"],
        next_validation_focus="先看近5年主线趋势延续是否有统计优势",
    )
    assert draft["source_type"] == "human_idea"
    assert draft["target_assets"] == ["AI龙头", "算力ETF"]


def test_draft_from_market_scan():
    draft = draft_from_market_scan(
        {
            "input_id": "SCAN-001",
            "scan_source": "external_scanner",
            "theme_title": "能源链走强",
            "market_snapshot": "油价与能源股联动增强",
            "candidate_assets": ["原油ETF", "能源ETF"],
            "signal_summary": "近20日强于大盘",
            "timeframe_hint": "日线",
            "evidence_refs": ["news-1", "chart-1"],
            "created_at": "2026-03-28T20:00:00+08:00",
        },
        draft_id="DRAFT-002",
        loser_definition="忽视地缘风险再定价的资金",
        profit_source_definition="事件驱动后的趋势延续",
        target_market="全球商品/ETF",
        entry_rule_prototype="事件后3日内创20日新高时进入",
        exit_rule_prototype="跌破10日均线退出",
        stop_rule_prototype="ATR两倍止损",
        failure_conditions=["事件快速降温", "油价冲高回落"],
        next_validation_focus="验证事件后10日正漂移",
    )
    assert draft["source_type"] == "market_scan"
    assert draft["target_market"] == "全球商品/ETF"


def test_draft_from_mature_edge():
    draft = draft_from_mature_edge(
        {
            "input_id": "EDGE-001",
            "edge_name": "趋势跟随",
            "edge_family": "trend_following",
            "source_refs": ["classic-trend"],
            "edge_summary": "强者恒强，弱者恒弱",
            "common_market_conditions": "趋势性强的市场",
            "known_failure_modes": ["震荡市", "假突破"],
            "created_at": "2026-03-28T20:00:00+08:00",
        },
        draft_id="DRAFT-003",
        loser_definition="逆势交易与过早抄顶抄底的资金",
        profit_source_definition="趋势扩散与延续",
        target_market="宽基ETF",
        target_assets=["510300", "510500"],
        target_timeframe="日线",
        entry_rule_prototype="突破25日高点进入",
        exit_rule_prototype="跌破20日低点退出",
        stop_rule_prototype="固定3%止损",
        next_validation_focus="验证不同宽基ETF上的一致性",
    )
    assert draft["source_type"] == "mature_edge"
    assert draft["failure_conditions"] == ["震荡市", "假突破"]
