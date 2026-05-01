from services.sinhdulieu import (
    clean_rows,
    fix_pk,
    existing_pk,
)


def test_not_null_bi_loai_ko(engine, setup_testcase_tables):
    rows = [
        {
            "MaSV": 3,
            "HoTen": "Nguyen Van A",
            "Email": "a1@gmail.com",
            "Tuoi": 20,
            "Diem": 8.5,
            "NgaySinh": "2004-01-01",
            "GioiTinh": "Nam",
            "SoDienThoai": "0911111111",
            "DiaChi": "Ha Noi",
            "MaKhoa": 1,
            "MaLop": 1,
        },
        {
            "MaSV": 4,
            "HoTen": None,
            "Email": "a2@gmail.com",
            "Tuoi": 21,
            "Diem": 7.0,
            "NgaySinh": "2004-01-02",
            "GioiTinh": "Nam",
            "SoDienThoai": "0922222222",
            "DiaChi": "Hai Phong",
            "MaKhoa": 1,
            "MaLop": 1,
        },
    ]

    allowed_columns = [
        "MaSV", "HoTen", "Email", "Tuoi", "Diem",
        "NgaySinh", "GioiTinh", "SoDienThoai",
        "DiaChi", "MaKhoa", "MaLop"
    ]

    column_type_map = {
        "MaSV": "int",
        "HoTen": "nvarchar",
        "Email": "nvarchar",
        "Tuoi": "int",
        "Diem": "float",
        "NgaySinh": "date",
        "GioiTinh": "nvarchar",
        "SoDienThoai": "nvarchar",
        "DiaChi": "nvarchar",
        "MaKhoa": "int",
        "MaLop": "int",
    }

    required_columns = allowed_columns

    out = clean_rows(
        rows,
        allowed_columns,
        column_type_map,
        required_columns
    )

    assert len(out) == 1
    assert out[0]["HoTen"] == "Nguyen Van A"


def test_pk_trung_thi_tang_lai(engine, setup_testcase_tables):
    rows = [
        {
            "MaSV": 1,
            "HoTen": "A",
            "Email": "a3@gmail.com",
            "Tuoi": 20,
            "Diem": 9.0,
            "NgaySinh": "2004-02-01",
            "GioiTinh": "Nam",
            "SoDienThoai": "0933333333",
            "DiaChi": "Ha Noi",
            "MaKhoa": 1,
            "MaLop": 1,
        },
        {
            "MaSV": 1,
            "HoTen": "B",
            "Email": "a4@gmail.com",
            "Tuoi": 21,
            "Diem": 7.5,
            "NgaySinh": "2004-02-02",
            "GioiTinh": "Nữ",
            "SoDienThoai": "0944444444",
            "DiaChi": "Hai Phong",
            "MaKhoa": 2,
            "MaLop": 2,
        },
    ]

    pk_columns = ["MaSV"]
    existing = existing_pk(engine, "SINHVIEN_TEST", pk_columns)
    type_map = {"MaSV": "int"}

    out = fix_pk(rows, pk_columns, existing, type_map)

    assert out[0]["MaSV"] != 1
    assert out[1]["MaSV"] != 1
    assert out[0]["MaSV"] != out[1]["MaSV"]