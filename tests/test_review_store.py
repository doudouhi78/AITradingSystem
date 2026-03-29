from pathlib import Path

from ai_dev_os import review_store
from ai_dev_os.project_objects import validate_formal_review_record
from tests.test_project_objects import sample_formal_review_record


def test_write_and_list_formal_review(tmp_path) -> None:
    review_store.REVIEWS_ROOT = tmp_path
    record = validate_formal_review_record(sample_formal_review_record())
    path = review_store.write_formal_review(record)

    assert Path(path).exists()
    loaded = review_store.read_formal_review('REV-TEST-001')
    assert loaded['experiment_id'] == 'exp-test-001'

    summaries = review_store.list_formal_reviews(limit=10, experiment_id='exp-test-001')
    assert len(summaries) == 1
    assert summaries[0]['review_id'] == 'REV-TEST-001'
