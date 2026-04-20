from services.sinhdulieu import remove_invalid_rows


def test_gioitinh_hop_le_duoc_giu_lai():
    rows = [
        {"gioitinh": "Nam"},
        {"gioitinh": "Nữ"},
        {"gioitinh": "nu"},
    ]

    out = remove_invalid_rows(rows)

    assert len(out) == 3


def test_gioitinh_sai_bi_loai():
    rows = [
        {"gioitinh": "Nam"},
        {"gioitinh": "abc"},
        {"gioitinh": ""},
        {"gioitinh": None},
    ]

    out = remove_invalid_rows(rows)

    assert len(out) == 1
    assert out[0]["gioitinh"] == "Nam"