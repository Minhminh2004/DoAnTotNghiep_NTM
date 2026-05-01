from services.sinhdulieu import valid_cate


def test_gioitinh_hop_le_duoc_giu_lai():
    rows = [
        {"GioiTinh": "Nam"},
        {"GioiTinh": "Nữ"},
        {"GioiTinh": "nu"},
    ]

    out = [r for r in rows if valid_cate(r)]

    assert len(out) == 3


def test_gioitinh_sai_bi_loai():
    rows = [
        {"GioiTinh": "Nam"},
        {"GioiTinh": "abc"},
        {"GioiTinh": ""},
        {"GioiTinh": None},
    ]

    out = [r for r in rows if valid_cate(r)]

    assert len(out) == 1
    assert out[0]["GioiTinh"] == "Nam"