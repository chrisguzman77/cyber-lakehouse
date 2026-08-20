from src.utils.schema import CANONICAL_ATTACK_CATS, normalize_attack_cat_value


def test_null_and_empty_mean_normal():
    assert normalize_attack_cat_value(None) == "normal"
    assert normalize_attack_cat_value("") == "normal"
    assert normalize_attack_cat_value("   ") == "normal"


def test_trim_and_lower():
    assert normalize_attack_cat_value(" Fuzzers ") == "fuzzers"
    assert normalize_attack_cat_value("Reconnaissance") == "reconnaissance"
    assert normalize_attack_cat_value("DoS") == "dos"


def test_backdoors_variant_maps_to_backdoor():
    assert normalize_attack_cat_value("Backdoors") == "backdoor"
    assert normalize_attack_cat_value("Backdoor") == "backdoor"


def test_known_values_land_in_canonical_set():
    raw = [
        "Generic",
        "Exploits",
        " Fuzzers ",
        "DoS",
        "Reconnaissance",
        "Analysis",
        "Backdoors",
        " Shellcode ",
        "Worms",
        None,
    ]
    for v in raw:
        assert normalize_attack_cat_value(v) in CANONICAL_ATTACK_CATS
