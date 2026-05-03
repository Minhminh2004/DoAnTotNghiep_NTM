URL = "mssql+pyodbc://DESKTOP-33J7KC7/DATN_NTM_TEST?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"


def post(c, url, data):
    return c.post(url, json=data)


def test_kogui_linkdb(client):
    r = post(client, "/api/test-connection", {})
    assert r.status_code == 400
    assert r.json["success"] is False


def test_connect_that(client):
    r = post(client, "/api/test-connection", {"db_url": URL})
    assert r.status_code == 200
    assert r.json["success"] is True
    assert "SINHVIEN_TEST" in r.json["tables"]


def test_thieubang(client):
    r = post(client, "/api/generate-data", {"db_url": URL})
    assert r.status_code == 400
    assert r.json["success"] is False


def test_hang_kohople(client):
    r = post(client, "/api/generate-data", {
        "db_url": URL,
        "table_name": "SINHVIEN_TEST",
        "row_count": 0
    })
    assert r.status_code == 400
    assert r.json["success"] is False