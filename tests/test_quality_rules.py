from src.utils.schema import QUALITY_RULES


def test_quality_rule_fields():
    for r in QUALITY_RULES:
        assert r.name
        assert r.description
