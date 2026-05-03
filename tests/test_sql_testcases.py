from sqlalchemy import text


def run_sql_case(engine, name, sql, expected):
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
        actual = "SUCCESS"
        error = ""
    except Exception as e:
        actual = "FAIL"
        error = str(e)

    status = "PASS" if actual == expected else "FAIL"

    assert status == "PASS", f"""
CASE: {name}
EXPECTED: {expected}
ACTUAL: {actual}
ERROR: {error}
SQL:
{sql}
"""


def cleanup_test_data(engine):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM SINHVIEN_TEST WHERE MaSV >= 900000"))


def test_sql_valid_insert(engine):
    cleanup_test_data(engine)

    sql = """
    INSERT INTO SINHVIEN_TEST
    (MaSV, HoTen, Email, Tuoi, Diem, GioiTinh, NgaySinh, SoDienThoai, DiaChi, MaKhoa, MaLop)
    VALUES
    (900001, N'Nguyen Test Valid', 'valid900001@gmail.com', 22, 8.5, N'Nam',
     '2002-01-01', '0911111111', N'Ha Noi', 1, 1)
    """

    run_sql_case(engine, "VALID_INSERT", sql, "SUCCESS")
    cleanup_test_data(engine)


def test_sql_not_null_hoten_violation(engine):
    cleanup_test_data(engine)

    sql = """
    INSERT INTO SINHVIEN_TEST
    (MaSV, HoTen, Email, Tuoi, Diem, GioiTinh, NgaySinh, SoDienThoai, DiaChi, MaKhoa, MaLop)
    VALUES
    (900002, NULL, 'notnull900002@gmail.com', 22, 8.5, N'Nam',
     '2002-01-01', '0911111112', N'Ha Noi', 1, 1)
    """

    run_sql_case(engine, "NOT_NULL_HoTen", sql, "FAIL")
    cleanup_test_data(engine)


def test_sql_unique_email_violation(engine):
    cleanup_test_data(engine)

    sql = """
    INSERT INTO SINHVIEN_TEST
    (MaSV, HoTen, Email, Tuoi, Diem, GioiTinh, NgaySinh, SoDienThoai, DiaChi, MaKhoa, MaLop)
    VALUES
    (900003, N'Nguyen Unique Test', 'a@gmail.com', 22, 8.5, N'Nam',
     '2002-01-01', '0911111113', N'Ha Noi', 1, 1)
    """

    run_sql_case(engine, "UNIQUE_Email", sql, "FAIL")
    cleanup_test_data(engine)


def test_sql_min_tuoi_violation(engine):
    cleanup_test_data(engine)

    sql = """
    INSERT INTO SINHVIEN_TEST
    (MaSV, HoTen, Email, Tuoi, Diem, GioiTinh, NgaySinh, SoDienThoai, DiaChi, MaKhoa, MaLop)
    VALUES
    (900004, N'Nguyen Min Test', 'min900004@gmail.com', 17, 8.5, N'Nam',
     '2002-01-01', '0911111114', N'Ha Noi', 1, 1)
    """

    run_sql_case(engine, "MIN_Tuoi", sql, "FAIL")
    cleanup_test_data(engine)


def test_sql_max_tuoi_violation(engine):
    cleanup_test_data(engine)

    sql = """
    INSERT INTO SINHVIEN_TEST
    (MaSV, HoTen, Email, Tuoi, Diem, GioiTinh, NgaySinh, SoDienThoai, DiaChi, MaKhoa, MaLop)
    VALUES
    (900014, N'Nguyen Max Age Test', 'maxtuoi900014@gmail.com', 61, 8.5, N'Nam',
     '2002-01-01', '0911111124', N'Ha Noi', 1, 1)
    """

    run_sql_case(engine, "MAX_Tuoi", sql, "FAIL")
    cleanup_test_data(engine)


def test_sql_max_diem_violation(engine):
    cleanup_test_data(engine)

    sql = """
    INSERT INTO SINHVIEN_TEST
    (MaSV, HoTen, Email, Tuoi, Diem, GioiTinh, NgaySinh, SoDienThoai, DiaChi, MaKhoa, MaLop)
    VALUES
    (900005, N'Nguyen Max Test', 'max900005@gmail.com', 22, 11, N'Nam',
     '2002-01-01', '0911111115', N'Ha Noi', 1, 1)
    """

    run_sql_case(engine, "MAX_Diem", sql, "FAIL")
    cleanup_test_data(engine)


def test_sql_min_diem_violation(engine):
    cleanup_test_data(engine)

    sql = """
    INSERT INTO SINHVIEN_TEST
    (MaSV, HoTen, Email, Tuoi, Diem, GioiTinh, NgaySinh, SoDienThoai, DiaChi, MaKhoa, MaLop)
    VALUES
    (900015, N'Nguyen Min Diem Test', 'mindiem900015@gmail.com', 22, -1, N'Nam',
     '2002-01-01', '0911111125', N'Ha Noi', 1, 1)
    """

    run_sql_case(engine, "MIN_Diem", sql, "FAIL")
    cleanup_test_data(engine)


def test_sql_format_email_violation(engine):
    cleanup_test_data(engine)

    sql = """
    INSERT INTO SINHVIEN_TEST
    (MaSV, HoTen, Email, Tuoi, Diem, GioiTinh, NgaySinh, SoDienThoai, DiaChi, MaKhoa, MaLop)
    VALUES
    (900006, N'Nguyen Email Test', 'email_sai_format', 22, 8.5, N'Nam',
     '2002-01-01', '0911111116', N'Ha Noi', 1, 1)
    """

    run_sql_case(engine, "FORMAT_Email", sql, "FAIL")
    cleanup_test_data(engine)


def test_sql_length_phone_violation(engine):
    cleanup_test_data(engine)

    sql = """
    INSERT INTO SINHVIEN_TEST
    (MaSV, HoTen, Email, Tuoi, Diem, GioiTinh, NgaySinh, SoDienThoai, DiaChi, MaKhoa, MaLop)
    VALUES
    (900007, N'Nguyen Phone Test', 'phone900007@gmail.com', 22, 8.5, N'Nam',
     '2002-01-01', '123', N'Ha Noi', 1, 1)
    """

    run_sql_case(engine, "LENGTH_SoDienThoai", sql, "FAIL")
    cleanup_test_data(engine)


def test_sql_gender_rule_violation(engine):
    cleanup_test_data(engine)

    sql = """
    INSERT INTO SINHVIEN_TEST
    (MaSV, HoTen, Email, Tuoi, Diem, GioiTinh, NgaySinh, SoDienThoai, DiaChi, MaKhoa, MaLop)
    VALUES
    (900008, N'Nguyen Gender Test', 'gender900008@gmail.com', 22, 8.5, N'Khac',
     '2002-01-01', '0911111118', N'Ha Noi', 1, 1)
    """

    run_sql_case(engine, "CHECK_GioiTinh", sql, "FAIL")
    cleanup_test_data(engine)


def test_sql_foreign_key_khoa_violation(engine):
    cleanup_test_data(engine)

    sql = """
    INSERT INTO SINHVIEN_TEST
    (MaSV, HoTen, Email, Tuoi, Diem, GioiTinh, NgaySinh, SoDienThoai, DiaChi, MaKhoa, MaLop)
    VALUES
    (900009, N'Nguyen FK Khoa Test', 'fkkhoa900009@gmail.com', 22, 8.5, N'Nam',
     '2002-01-01', '0911111119', N'Ha Noi', 999, 1)
    """

    run_sql_case(engine, "FK_MaKhoa", sql, "FAIL")
    cleanup_test_data(engine)


def test_sql_foreign_key_lop_violation(engine):
    cleanup_test_data(engine)

    sql = """
    INSERT INTO SINHVIEN_TEST
    (MaSV, HoTen, Email, Tuoi, Diem, GioiTinh, NgaySinh, SoDienThoai, DiaChi, MaKhoa, MaLop)
    VALUES
    (900010, N'Nguyen FK Lop Test', 'fklop900010@gmail.com', 22, 8.5, N'Nam',
     '2002-01-01', '0911111120', N'Ha Noi', 1, 999)
    """

    run_sql_case(engine, "FK_MaLop", sql, "FAIL")
    cleanup_test_data(engine)