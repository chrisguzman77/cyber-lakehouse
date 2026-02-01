from src.utils.schema import QUALITY_RULES, REQUIRED_COLUMNS


def test_required_columns_not_empty():
    assert len(REQUIRED_COLUMNS) >= 5


def test_quality_rules_present():
    names = [r.name for r in QUALITY_RULES]
    assert "non_null_label" in names
