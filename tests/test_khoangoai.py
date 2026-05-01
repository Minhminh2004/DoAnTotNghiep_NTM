from services.sinhdulieu import (
    fk_data,
    apply_fk,
    valid_fk,
)


def test_fk_phai_ton_tai_trong_bang_cha(engine, setup_testcase_tables):
    foreign_keys = [
        {
            "columns": ["MaKhoa"],
            "referred_table": "KHOA_TEST",
            "referred_columns": ["MaKhoa"],
        },
        {
            "columns": ["MaLop"],
            "referred_table": "LOP_TEST",
            "referred_columns": ["MaLop"],
        }
    ]

    ref_data = fk_data(engine, foreign_keys)
    assert len(ref_data) == 2

    rows = [
        {
            "MaSV": 3,
            "HoTen": "Nguyen Van C",
            "Email": "c@gmail.com",
            "Tuoi": 20,
            "Diem": 8.0,
            "GioiTinh": "Nam",
            "NgaySinh": "2004-01-01",
            "SoDienThoai": "0911111111",
            "DiaChi": "Ha Noi",
        },
        {
            "MaSV": 4,
            "HoTen": "Tran Thi D",
            "Email": "d@gmail.com",
            "Tuoi": 21,
            "Diem": 9.0,
            "GioiTinh": "Nữ",
            "NgaySinh": "2003-01-01",
            "SoDienThoai": "0922222222",
            "DiaChi": "Hai Phong",
        },
    ]

    applied = apply_fk(rows, ref_data)

    assert all(r["MaKhoa"] in [1, 2] for r in applied)
    assert all(r["MaLop"] in [1, 2] for r in applied)

    validated = valid_fk(applied, ref_data)

    assert len(validated) == 2


def test_fk_sai_bi_loai():
    fk_reference_data = [
        {
            "child_columns": ["MaKhoa"],
            "parent_columns": ["MaKhoa"],
            "parent_values": [{"MaKhoa": 1}, {"MaKhoa": 2}],
        }
    ]

    rows = [
        {"MaSV": 3, "HoTen": "A", "MaKhoa": 1},
        {"MaSV": 4, "HoTen": "B", "MaKhoa": 99},
    ]

    out = valid_fk(rows, fk_reference_data)

    assert len(out) == 1
    assert out[0]["MaKhoa"] == 1