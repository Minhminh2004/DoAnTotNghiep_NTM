from unittest.mock import patch


def test_api_test_connection_db(client):
    res = client.post("/api/test-connection", json={})
    assert res.status_code == 400
    assert res.json["success"] is False


@patch("app.test_connection")
@patch("app.get_table_names")
def test_api_test_connection_success(mock_tables, mock_conn, client):
    mock_conn.return_value = (True, "ok")
    mock_tables.return_value = ["sanpham", "sinhvien"]

    res = client.post("/api/test-connection", json={"db_url": "sqlite:///test.db"})

    assert res.status_code == 200
    assert res.json["success"] is True
    assert "sanpham" in res.json["tables"]


def test_api_generate_data_missing_table(client):
    res = client.post("/api/generate-data", json={
        "db_url": "sqlite:///test.db",
        "row_count": 2
    })
    assert res.status_code == 400
    assert res.json["success"] is False


@patch("app.generate_and_insert_data")
def test_api_generate_data_success(mock_generate, client):
    mock_generate.return_value = {
        "message": "Đã sinh thành công 2 dòng.",
        "inserted_count": 2,
        "preview": [{"masp": 3}, {"masp": 4}],
    }

    res = client.post("/api/generate-data", json={
        "db_url": "sqlite:///test.db",
        "table_name": "sanpham",
        "row_count": 2,
        "user_instruction": "sinh san pham chi la sach"
    })

    assert res.status_code == 200
    assert res.json["success"] is True
    assert res.json["inserted_count"] == 2