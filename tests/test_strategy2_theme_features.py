from __future__ import annotations

import pandas as pd

from src.strategy2.features.theme_features import (
    ThemeFeatureConfig,
    build_pool_b_mask,
    build_theme_feature_matrix_from_panels,
    load_theme_inputs,
    scan_feature_ic,
)


def test_load_theme_inputs_applies_min_members(tmp_path):
    theme_index = pd.DataFrame(
        {
            'ts_code': ['T1.TI', 'T2.TI'],
            'count': [12, 5],
        }
    )
    theme_member = pd.DataFrame(
        {
            'ts_code': ['T1.TI', 'T1.TI', 'T2.TI'],
            'con_code': ['000001.SZ', '000002.SZ', '000001.SZ'],
        }
    )
    index_path = tmp_path / 'ths_index.parquet'
    member_path = tmp_path / 'ths_member.parquet'
    theme_index.to_parquet(index_path)
    theme_member.to_parquet(member_path)

    filtered_index, filtered_member = load_theme_inputs(index_path, member_path, min_theme_members=10)

    assert filtered_index['ts_code'].tolist() == ['T1.TI']
    assert sorted(filtered_member['con_code'].tolist()) == ['000001.SZ', '000002.SZ']


def test_build_pool_b_mask_filters_new_listing_st_and_liquidity(tmp_path):
    trade_dates = pd.date_range('2024-01-01', periods=30, freq='B')
    close = pd.DataFrame(
        {
            '000001.SZ': range(30, 60),
            '000002.SZ': range(20, 50),
            '000003.SZ': range(10, 40),
        },
        index=trade_dates,
        dtype=float,
    )
    amount = pd.DataFrame(
        {
            '000001.SZ': [8e7] * 30,
            '000002.SZ': [1e7] * 30,
            '000003.SZ': [8e7] * 30,
        },
        index=trade_dates,
        dtype=float,
    )
    volume = pd.DataFrame(1.0, index=trade_dates, columns=close.columns)
    listed_dates = pd.Series(
        {
            '000001.SZ': pd.Timestamp('2020-01-01'),
            '000002.SZ': pd.Timestamp('2020-01-01'),
            '000003.SZ': pd.Timestamp('2020-01-01'),
        }
    )

    stock_basic = pd.DataFrame({'ts_code': ['000003.SZ'], 'name': ['*ST Test']})
    stock_basic.to_parquet(tmp_path / 'stock_basic.parquet')

    mask = build_pool_b_mask(
        close,
        amount,
        volume,
        listed_dates,
        data_dir=tmp_path,
        min_list_days=5,
        min_avg_amount=5e7,
    )

    assert mask['000001.SZ'].iloc[-1]
    assert not mask['000002.SZ'].iloc[-1]
    assert not mask['000003.SZ'].iloc[-1]


def test_build_theme_feature_matrix_and_ic_scan():
    trade_dates = pd.date_range('2024-01-01', periods=40, freq='B')
    close = pd.DataFrame(
        {
            '000001.SZ': 10 + pd.Series(range(40), index=trade_dates) * 0.2,
            '000002.SZ': 11 + pd.Series(range(40), index=trade_dates) * 0.18,
            '000003.SZ': 9 + pd.Series(range(40), index=trade_dates) * 0.05,
            '000004.SZ': 8 + pd.Series(range(40), index=trade_dates) * 0.04,
        }
    )
    amount = pd.DataFrame(
        {
            '000001.SZ': [9e7] * 40,
            '000002.SZ': [8e7] * 40,
            '000003.SZ': [7e7] * 40,
            '000004.SZ': [6e7] * 40,
        },
        index=trade_dates,
        dtype=float,
    )
    pool_mask = pd.DataFrame(True, index=trade_dates, columns=close.columns)
    theme_member = pd.DataFrame(
        {
            'ts_code': ['T1.TI', 'T1.TI', 'T2.TI', 'T2.TI'],
            'con_code': ['000001.SZ', '000002.SZ', '000003.SZ', '000004.SZ'],
        }
    )

    features = build_theme_feature_matrix_from_panels(
        close,
        amount,
        theme_member,
        pool_mask,
        config=ThemeFeatureConfig(windows=(5, 10, 20), rank_windows=(5, 20), heat_windows=(5, 20)),
    )

    expected = {
        'theme_avg_ret_5',
        'theme_avg_ret_10',
        'theme_avg_ret_20',
        'theme_rank_pct_5',
        'theme_rank_pct_20',
        'theme_heat_5',
        'theme_heat_20',
        'theme_member_count',
    }
    assert expected.issubset(features.columns)
    assert features.index.names == ['trade_date', 'ts_code']
    assert features['theme_member_count'].dropna().eq(1).all()

    ic_scan = scan_feature_ic(
        features,
        close,
        pool_mask,
        feature_names=['theme_avg_ret_5', 'theme_rank_pct_20', 'theme_heat_20'],
        horizons=(3, 5, 10),
    )

    assert list(ic_scan['feature_name'])
    assert {'ic_3d', 'ic_5d', 'ic_10d', 'best_icir'}.issubset(ic_scan.columns)

