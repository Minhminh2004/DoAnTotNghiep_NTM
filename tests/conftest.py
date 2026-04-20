import os
import gc
import tempfile
import pytest
from sqlalchemy import create_engine, text

from app import app as flask_app


@pytest.fixture
def app():
    flask_app.config["TESTING"] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def temp_db_url():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db_url = f"sqlite:///{path}"
    yield db_url


@pytest.fixture
def engine(temp_db_url):
    engine = create_engine(temp_db_url)
    yield engine
    engine.dispose()

    # ép giải phóng connection/file handle
    gc.collect()

    # xóa file sqlite sau khi đã dispose
    path = temp_db_url.replace("sqlite:///", "")
    if os.path.exists(path):
        try:
            os.remove(path)
        except PermissionError:
            pass


@pytest.fixture
def setup_sanpham_table(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE sanpham (
                masp INTEGER PRIMARY KEY,
                tensp TEXT NOT NULL,
                ngaynhap DATE NOT NULL,
                gia FLOAT NOT NULL
            )
        """))
        conn.execute(text("""
            INSERT INTO sanpham (masp, tensp, ngaynhap, gia) VALUES
            (1, 'Laptop Dell', '2023-01-10', 15000000),
            (2, 'Chuot Logitech', '2023-02-15', 500000)
        """))
    return True


@pytest.fixture
def setup_khoa_lop(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE khoa (
                makhoa INTEGER PRIMARY KEY,
                tenkhoa TEXT NOT NULL
            )
        """))
        conn.execute(text("""
            INSERT INTO khoa (makhoa, tenkhoa) VALUES
            (1, 'CNTT'),
            (2, 'Kinh te')
        """))

        conn.execute(text("""
            CREATE TABLE lop (
                malop INTEGER PRIMARY KEY,
                tenlop TEXT NOT NULL,
                makhoa INTEGER NOT NULL,
                FOREIGN KEY (makhoa) REFERENCES khoa(makhoa)
            )
        """))

        conn.execute(text("""
            CREATE TABLE diemthi (
                masv INTEGER PRIMARY KEY,
                hoten TEXT NOT NULL,
                diem FLOAT NOT NULL,
                ngaysinh DATE NOT NULL,
                gioitinh TEXT NOT NULL
            )
        """))
    return True