import json
import time
from unittest.mock import patch
from sqlalchemy import text

from services.sinhdulieu import generate_and_insert_data

@patch("services.sinhdulieu.call_ollama")
def test_sinh_1000_dong_khong_loi(mock_call_ollama, temp_db_url, engine, setup_sanpham_table):
    data = []
    for i in range(1000):
        data.append({
            "masp": i + 100,
            "tensp": f"San pham {i}",
            "ngaynhap": "2024-01-01",
            "gia": float(1000 + i),
        })

    mock_call_ollama.return_value = json.dumps(data, ensure_ascii=False)

    start = time.time()
    result = generate_and_insert_data(
        db_url=temp_db_url,
        table_name="sanpham",
        row_count=1000,
        model_name="fake-model",
        user_instruction=""
    )
    elapsed = time.time() - start

    assert result["inserted_count"] == 1000
    assert elapsed < 10

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM sanpham")).scalar_one()
        assert count == 1002