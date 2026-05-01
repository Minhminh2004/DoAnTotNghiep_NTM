import json
import time
from unittest.mock import patch
from sqlalchemy import text

from services.sinhdulieu import generate_and_insert_data


@patch("services.sinhdulieu.call_ollama")
def test_sinh_1000_dong_hop_le_va_insert_nhanh(
    mock_call_ollama,
    temp_db_url,
    engine,
    setup_testcase_tables
):
    data = []

    for i in range(1000):
        data.append({
            "MaSV": i + 100,
            "HoTen": f"Sinh vien {i}",
            "Email": f"sinhvien{i}@gmail.com",
            "Tuoi": 18 + (i % 30),
            "Diem": float(i % 11),
            "GioiTinh": "Nam" if i % 2 == 0 else "Nữ",
            "NgaySinh": "2004-01-01",
            "SoDienThoai": f"09{i:08d}"[:10],
            "DiaChi": "Ha Noi",
            "MaKhoa": 1 if i % 2 == 0 else 2,
            "MaLop": 1 if i % 2 == 0 else 2,
        })

    mock_call_ollama.return_value = json.dumps(data, ensure_ascii=False)

    start = time.time()

    result = generate_and_insert_data(
        db_url=temp_db_url,
        table_name="SINHVIEN_TEST",
        row_count=1000,
        model_name="fake-model",
        user_instruction="Sinh dữ liệu sinh viên hợp lệ"
    )

    elapsed = time.time() - start

    assert result["inserted_count"] == 1000
    assert elapsed < 10

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dbo.SINHVIEN_TEST")).scalar_one()

    assert count == 1002