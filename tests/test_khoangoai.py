from services.sinhdulieu import (
    get_fk_reference_data,
    apply_foreign_keys,
    validate_foreign_keys,
)


def test_fk_phai_ton_tai_trong_bang_cha(engine, setup_khoa_lop):
    foreign_keys = [
        {
            "columns": ["makhoa"],
            "referred_table": "khoa",
            "referred_columns": ["makhoa"],
        }
    ]

    ref_data = get_fk_reference_data(engine, foreign_keys)
    assert len(ref_data) == 1

    rows = [
        {"malop": 1, "tenlop": "CNTT1"},
        {"malop": 2, "tenlop": "CNTT2"},
    ]

    applied = apply_foreign_keys(rows, ref_data)
    assert all(r["makhoa"] in [1, 2] for r in applied)

    validated = validate_foreign_keys(applied, ref_data)
    assert len(validated) == 2


def test_fk_sai_bi_loai():
    fk_reference_data = [
        {
            "child_columns": ["makhoa"],
            "parent_columns": ["makhoa"],
            "parent_values": [{"makhoa": 1}, {"makhoa": 2}],
        }
    ]

    rows = [
        {"malop": 1, "tenlop": "A", "makhoa": 1},
        {"malop": 2, "tenlop": "B", "makhoa": 99},
    ]

    out = validate_foreign_keys(rows, fk_reference_data)
    assert len(out) == 1
    assert out[0]["makhoa"] == 1