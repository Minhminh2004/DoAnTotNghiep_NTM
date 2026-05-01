from unittest.mock import patch


def test_api_test_connection_missing_db_url(client):
    res = client.post("/api/test-connection", json={})

    assert res.status_code == 400
    assert res.json["success"] is False
    assert "Vui lòng nhập link database" in res.json["message"]


@patch("app.test_connection")
@patch("app.get_table_names")
def test_api_test_connection_success(mock_tables, mock_conn, client):
    mock_conn.return_value = (True, "ok")
    mock_tables.return_value = ["SINHVIEN_TEST", "KHOA_TEST"]

    res = client.post("/api/test-connection", json={
        "db_url": "mssql+pyodbc://DESKTOP-33J7KC7/DATN_NTM_TEST?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    })

    assert res.status_code == 200
    assert res.json["success"] is True
    assert "SINHVIEN_TEST" in res.json["tables"]


def test_api_generate_data_missing_db_url(client):
    res = client.post("/api/generate-data", json={
        "table_name": "SINHVIEN_TEST",
        "row_count": 2
    })

    assert res.status_code == 400
    assert res.json["success"] is False
    assert "Thiếu link database" in res.json["message"]


def test_api_generate_data_missing_table(client):
    res = client.post("/api/generate-data", json={
        "db_url": "mssql+pyodbc://DESKTOP-33J7KC7/DATN_NTM_TEST?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
        "row_count": 2
    })

    assert res.status_code == 400
    assert res.json["success"] is False
    assert "Vui lòng chọn bảng" in res.json["message"]


def test_api_generate_data_invalid_row_count(client):
    res = client.post("/api/generate-data", json={
        "db_url": "mssql+pyodbc://DESKTOP-33J7KC7/DATN_NTM_TEST?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
        "table_name": "SINHVIEN_TEST",
        "row_count": 0
    })

    assert res.status_code == 400
    assert res.json["success"] is False
    assert "Số dòng cần sinh không hợp lệ" in res.json["message"]


@patch("app.generate_and_insert_data")
def test_api_generate_data_success_valid_case(mock_generate, client):
    mock_generate.return_value = {
        "message": "Đã sinh và insert thành công 2 dòng vào bảng 'SINHVIEN_TEST'.",
        "inserted_count": 2,
        "preview": [
            {
                "MaSV": 3,
                "HoTen": "Le Van C",
                "Email": "c@gmail.com",
                "Tuoi": 20,
                "Diem": 8.5,
                "GioiTinh": "Nam",
                "NgaySinh": "2004-01-01",
                "SoDienThoai": "0911111111",
                "DiaChi": "Ha Noi",
                "MaKhoa": 1,
                "MaLop": 1,
            },
            {
                "MaSV": 4,
                "HoTen": "Pham Thi D",
                "Email": "d@gmail.com",
                "Tuoi": 21,
                "Diem": 9.0,
                "GioiTinh": "Nữ",
                "NgaySinh": "2003-02-02",
                "SoDienThoai": "0922222222",
                "DiaChi": "Hai Phong",
                "MaKhoa": 2,
                "MaLop": 2,
            }
        ],
    }

    res = client.post("/api/generate-data", json={
        "db_url": "mssql+pyodbc://DESKTOP-33J7KC7/DATN_NTM_TEST?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
        "table_name": "SINHVIEN_TEST",
        "row_count": 2,
        "user_instruction": "Sinh dữ liệu sinh viên hợp lệ"
    })

    assert res.status_code == 200
    assert res.json["success"] is True
    assert res.json["inserted_count"] == 2


@patch("app.generate_and_insert_data")
def test_api_generate_data_not_null_error(mock_generate, client):
    mock_generate.side_effect = ValueError(
        "Dữ liệu không hợp lệ: cột HoTen không được null"
    )

    res = client.post("/api/generate-data", json={
        "db_url": "mssql+pyodbc://DESKTOP-33J7KC7/DATN_NTM_TEST?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
        "table_name": "SINHVIEN_TEST",
        "row_count": 1,
        "user_instruction": "Sinh case không hợp lệ: HoTen = null"
    })

    assert res.status_code == 400
    assert res.json["success"] is True


@patch("app.generate_and_insert_data")
def test_api_generate_data_unique_error(mock_generate, client):
    mock_generate.side_effect = Exception(
        "UNIQUE constraint failed: SINHVIEN_TEST.Email"
    )

    res = client.post("/api/generate-data", json={
        "db_url": "mssql+pyodbc://DESKTOP-33J7KC7/DATN_NTM_TEST?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
        "table_name": "SINHVIEN_TEST",
        "row_count": 1,
        "user_instruction": "Sinh case không hợp lệ: Email bị trùng"
    })

    assert res.status_code == 500
    assert res.json["success"] is True


@patch("app.generate_and_insert_data")
def test_api_generate_data_format_date_error(mock_generate, client):
    mock_generate.side_effect = Exception(
        "date and/or time conversion failed"
    )

    res = client.post("/api/generate-data", json={
        "db_url": "mssql+pyodbc://DESKTOP-33J7KC7/DATN_NTM_TEST?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
        "table_name": "SINHVIEN_TEST",
        "row_count": 1,
        "user_instruction": "Sinh case không hợp lệ: NgaySinh sai format"
    })

    assert res.status_code == 500
    assert res.json["success"] is False