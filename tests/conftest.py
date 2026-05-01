import gc
import time
from pathlib import Path
from datetime import datetime

import pytest
from sqlalchemy import create_engine, text

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

from app import app as flask_app
    

TEST_DB_URL = (
    "mssql+pyodbc://DESKTOP-33J7KC7/DATN_NTM_TEST"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)


@pytest.fixture
def app():
    flask_app.config["TESTING"] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def temp_db_url():
    return TEST_DB_URL


@pytest.fixture
def engine():
    engine = create_engine(TEST_DB_URL)
    yield engine
    engine.dispose()
    gc.collect()


@pytest.fixture
def setup_testcase_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            IF OBJECT_ID('dbo.SINHVIEN_TEST', 'U') IS NOT NULL DROP TABLE dbo.SINHVIEN_TEST;
            IF OBJECT_ID('dbo.LOP_TEST', 'U') IS NOT NULL DROP TABLE dbo.LOP_TEST;
            IF OBJECT_ID('dbo.KHOA_TEST', 'U') IS NOT NULL DROP TABLE dbo.KHOA_TEST;
        """))

        conn.execute(text("""
            CREATE TABLE dbo.KHOA_TEST (
                MaKhoa INT PRIMARY KEY,
                TenKhoa NVARCHAR(100) NOT NULL UNIQUE
            )
        """))

        conn.execute(text("""
            INSERT INTO dbo.KHOA_TEST (MaKhoa, TenKhoa) VALUES
            (1, N'CNTT'),
            (2, N'Kinh tế')
        """))

        conn.execute(text("""
            CREATE TABLE dbo.LOP_TEST (
                MaLop INT PRIMARY KEY,
                TenLop NVARCHAR(100) NOT NULL,
                MaKhoa INT NOT NULL,
                FOREIGN KEY (MaKhoa) REFERENCES dbo.KHOA_TEST(MaKhoa)
            )
        """))

        conn.execute(text("""
            INSERT INTO dbo.LOP_TEST (MaLop, TenLop, MaKhoa) VALUES
            (1, N'CNTT01', 1),
            (2, N'KT01', 2)
        """))

        conn.execute(text("""
            CREATE TABLE dbo.SINHVIEN_TEST (
                MaSV INT PRIMARY KEY,
                HoTen NVARCHAR(100) NOT NULL,
                Email NVARCHAR(100) NOT NULL UNIQUE CHECK (Email LIKE '%_@_%._%'),
                Tuoi INT NOT NULL CHECK (Tuoi >= 18 AND Tuoi <= 60),
                Diem FLOAT NOT NULL CHECK (Diem >= 0 AND Diem <= 10),
                GioiTinh NVARCHAR(10) NOT NULL CHECK (GioiTinh IN (N'Nam', N'Nữ', N'nu')),
                NgaySinh DATE NOT NULL,
                SoDienThoai VARCHAR(10) NOT NULL CHECK (LEN(SoDienThoai) = 10),
                DiaChi NVARCHAR(200) NOT NULL,
                MaKhoa INT NOT NULL,
                MaLop INT NOT NULL,
                FOREIGN KEY (MaKhoa) REFERENCES dbo.KHOA_TEST(MaKhoa),
                FOREIGN KEY (MaLop) REFERENCES dbo.LOP_TEST(MaLop)
            )
        """))

        conn.execute(text("""
            INSERT INTO dbo.SINHVIEN_TEST
            (MaSV, HoTen, Email, Tuoi, Diem, GioiTinh, NgaySinh, SoDienThoai, DiaChi, MaKhoa, MaLop)
            VALUES
            (1, N'Nguyen Van A', 'a@gmail.com', 20, 8.5, N'Nam', '2004-01-01', '0912345678', N'Hà Nội', 1, 1),
            (2, N'Tran Thi B', 'b@gmail.com', 21, 9.0, N'Nữ', '2003-02-02', '0987654321', N'Hải Phòng', 2, 2)
        """))

    return True


REPORT_DIR = Path("reports")
EXCEL_FILE = REPORT_DIR / f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

test_results = []


def pytest_runtest_setup(item):
    item.start_time = time.time()
    item.start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()

    if report.when == "call":
        duration = round(time.time() - item.start_time, 4)
        status = "PASS" if report.passed else "FAIL"

        test_results.append({
            "test_name": item.name,
            "status": status,
            "start_time": item.start_datetime,
            "duration_seconds": duration,
        })


def pytest_sessionfinish(session, exitstatus):
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    ws = wb.active
    ws.title = "Test Report"

    headers = [
        "Tên test",
        "Trạng thái",
        "Thời gian bắt đầu",
        "Thời gian chạy (giây)",
    ]

    ws.append(headers)

    for cell in ws[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="1F4E78")
        cell.alignment = Alignment(horizontal="center", vertical="center")

    for r in test_results:
        ws.append([
            r["test_name"],
            r["status"],
            r["start_time"],
            r["duration_seconds"],
        ])

    for row in range(2, ws.max_row + 1):
        status_cell = ws[f"B{row}"]

        if status_cell.value == "PASS":
            status_cell.fill = PatternFill("solid", fgColor="C6EFCE")
            status_cell.font = Font(color="006100", bold=True)

        elif status_cell.value == "FAIL":
            status_cell.fill = PatternFill("solid", fgColor="FFC7CE")
            status_cell.font = Font(color="9C0006", bold=True)

    for col in range(1, ws.max_column + 1):
        col_letter = get_column_letter(col)
        max_length = 0

        for cell in ws[col_letter]:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))

        ws.column_dimensions[col_letter].width = max_length + 5

    ws.freeze_panes = "A2"
    wb.save(EXCEL_FILE)