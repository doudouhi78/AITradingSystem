from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_refresh_module():
    script_path = Path(__file__).resolve().parents[1] / 'scripts' / 'refresh_factor_registry.py'
    spec = importlib.util.spec_from_file_location('refresh_factor_registry', script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_refresh_registry_dry_run_detects_available_family(tmp_path) -> None:
    module = _load_refresh_module()
    (tmp_path / 'runtime' / 'fundamental_data').mkdir(parents=True)
    (tmp_path / 'runtime' / 'fundamental_data' / 'valuation_daily.parquet').write_bytes(b'test')

    result = module.refresh_registry(dry_run=True, root=tmp_path)

    assert result['mode'] == 'dry-run'
    assert result['selected_families'] == ['classic']
    assert 'run_classic_factors_ic_eval.py' in result['report']
    assert 'valuation_daily.parquet' in result['available_files']


def test_merge_registry_entries_is_idempotent_and_preserves_existing() -> None:
    module = _load_refresh_module()
    existing = [
        {'factor_name': 'alpha004', 'factor_id': 4, 'icir': 1.3},
        {'factor_name': 'momentum_12_1', 'factor_id': 'classic_momentum_12_1', 'icir': 0.055},
    ]
    incoming = [
        {'factor_name': 'momentum_12_1', 'factor_id': 'classic_momentum_12_1', 'icir': 0.060},
        {'factor_name': 'asset_turnover', 'factor_id': 'classic_asset_turnover', 'icir': 0.063},
    ]

    merged, added = module.merge_registry_entries(existing, incoming)
    merged_again, added_again = module.merge_registry_entries(merged, incoming)

    assert added == 1
    assert added_again == 0
    assert len(merged) == 3
    assert len(merged_again) == 3
    by_name = {item['factor_name']: item for item in merged_again}
    assert by_name['alpha004']['factor_id'] == 4
    assert by_name['momentum_12_1']['icir'] == 0.060
    assert by_name['asset_turnover']['factor_id'] == 'classic_asset_turnover'


def test_refresh_registry_run_appends_new_winners_without_duplicates(tmp_path, monkeypatch) -> None:
    module = _load_refresh_module()
    (tmp_path / 'runtime' / 'fundamental_data').mkdir(parents=True)
    (tmp_path / 'runtime' / 'fundamental_data' / 'valuation_daily.parquet').write_bytes(b'test')
    (tmp_path / 'runtime' / 'alpha_research').mkdir(parents=True)
    (tmp_path / 'runtime' / 'factor_registry').mkdir(parents=True)
    (tmp_path / 'runtime' / 'factor_registry' / 'factor_registry.json').write_text(
        json.dumps([
            {'factor_name': 'alpha004', 'factor_id': 4, 'icir': 1.3},
            {'factor_name': 'momentum_12_1', 'factor_id': 'classic_momentum_12_1', 'icir': 0.055},
        ], ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    (tmp_path / 'runtime' / 'alpha_research' / 'classic_factors_ic_summary.csv').write_text(
        'factor_name,category,ic_mean,ic_std,icir,status\n'
        'momentum_12_1,momentum,0.02,0.2,0.06,pass\n'
        'asset_turnover,quality,0.01,0.1,0.07,pass\n'
        'book_to_market,value,0.00,0.2,0.01,weak\n',
        encoding='utf-8',
    )

    monkeypatch.setattr(module, 'run_family', lambda spec, root: (True, 'ok'))

    result = module.refresh_registry(dry_run=False, root=tmp_path)
    payload = json.loads((tmp_path / 'runtime' / 'factor_registry' / 'factor_registry.json').read_text(encoding='utf-8'))

    assert result['new_factors'] == 1
    assert result['failed_families'] == 0
    assert result['skipped_families'] == 0
    assert len(payload) == 3
    names = [item['factor_name'] for item in payload]
    assert names.count('momentum_12_1') == 1
    assert 'asset_turnover' in names
