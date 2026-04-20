from sqlalchemy import text
from services.sinhdulieu import (
    sanitize_rows,
    make_unique_primary_keys,
    get_existing_pk_values,
)

def test_not_null_bi_loai_ko(engine, setup_khoa_lop):
    rows = [
        {"masv": 1, "hoten": "Nguyen Van A", "diem": 8.5, "ngaysinh": "2004-01-01", "gioitinh": "Nam"},
        {"masv": 2, "hoten": None, "diem": 7.0, "ngaysinh": "2004-01-02", "gioitinh": "Nam"},
    ]

    allowed_columns = ["masv", "hoten", "diem", "ngaysinh", "gioitinh"]
    column_type_map = {
        "masv": "int",
        "hoten": "nvarchar",
        "diem": "float",
        "ngaysinh": "date",
        "gioitinh": "nvarchar",
    }
    required_columns = ["masv", "hoten", "diem", "ngaysinh", "gioitinh"]    

    out = sanitize_rows(rows, allowed_columns, column_type_map, required_columns)   

    assert len(out) == 1
    assert out[0]["hoten"] == "Nguyen Van A"


def test_pk_trung_thi_tang_lai(engine, setup_khoa_lop):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO diemthi (masv, hoten, diem, ngaysinh, gioitinh)
            VALUES (1, 'Cu', 8.0, '2004-01-01', 'Nam')
        """))

    rows = [
        {"masv": 1, "hoten": "A", "diem": 9.0, "ngaysinh": "2004-02-01", "gioitinh": "Nam"},
        {"masv": 1, "hoten": "B", "diem": 7.5, "ngaysinh": "2004-02-02", "gioitinh": "Nữ"},
    ]

    pk_columns = ["masv"]
    existing = get_existing_pk_values(engine, "diemthi", pk_columns)
    type_map = {"masv": "int"}

    out = make_unique_primary_keys(rows, pk_columns, existing, type_map)

    assert out[0]["masv"] != 1
    assert out[1]["masv"] != 1
    assert out[0]["masv"] != out[1]["masv"]