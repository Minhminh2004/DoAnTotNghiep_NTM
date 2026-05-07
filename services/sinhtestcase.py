from sqlalchemy import MetaData, Table, insert, select, func

from db.connection import create_db_engine
from db.laydulieu import get_table_schema_and_samples
from ai.generator import (
    build_testcase_prompt,
    call_ollama,
    parse_json_from_ollama
)


def normalize_kind(k):
    k = str(k or "").strip().upper()

    if k in ("HỢP_LỆ", "HOP_LE", "VALID", "HỢP LỆ", "HOP LE"):
        return "VALID"

    if k in ("KHÔNG_HỢP_LỆ", "KHONG_HOP_LE", "INVALID", "KHÔNG HỢP LỆ", "KHONG HOP LE"):
        return "INVALID"

    return k


def get_test_kind(testcase):
    return normalize_kind(
        testcase.get("loai_test")
        or testcase.get("test_kind")
    )


def get_input_data(testcase):
    return (
        testcase.get("du_lieu_test")
        or testcase.get("input_data")
        or {}
    )


def get_test_name(testcase):
    return (
        testcase.get("ten_testcase")
        or testcase.get("test_name")
        or "Chưa có tên test case"
    )


def get_test_rule(testcase):
    return (
        testcase.get("loai_kiem_thu")
        or testcase.get("rule_tested")
        or "Kiểm thử INSERT"
    )


def is_duplicate_pk_test(testcase):
    text = (
        str(get_test_name(testcase)) + " " +
        str(get_test_rule(testcase)) + " " +
        str(get_input_data(testcase))
    ).lower()

    return (
        "trùng khóa" in text
        or "trung khoa" in text
        or "primary key" in text
    )


def complete_insert_data(conn, tb, input_data, testcase):
    input_data = dict(input_data or {})

    sample = conn.execute(select(tb).limit(1)).mappings().first()
    sample = dict(sample) if sample else {}

    pk_cols = list(tb.primary_key.columns)

    if pk_cols:
        pk_col = pk_cols[0]
        pk_name = pk_col.name

        if not is_duplicate_pk_test(testcase):
            max_id = conn.execute(select(func.max(pk_col))).scalar()
            input_data[pk_name] = (max_id or 0) + 1
        elif input_data.get(pk_name) in (None, "") and sample:
            input_data[pk_name] = sample.get(pk_name)

    for col in tb.columns:
        col_name = col.name

        if col_name in input_data:
            continue

        if col.nullable:
            continue

        if col_name in sample and sample[col_name] is not None:
            input_data[col_name] = sample[col_name]
            continue

        col_type = str(col.type).lower()

        if "int" in col_type:
            input_data[col_name] = 1
        elif any(x in col_type for x in ("float", "real", "decimal", "numeric")):
            input_data[col_name] = 1
        elif "date" in col_type:
            input_data[col_name] = "2000-01-01"
        else:
            input_data[col_name] = "Gia tri hop le"

    text = (str(get_test_name(testcase)) + " " + str(get_test_rule(testcase))).lower()

    for key in list(input_data.keys()):
        if "email" in key.lower():
            if "email" not in text and "unique" not in text and "trùng" not in text:
                pk_val = input_data.get(pk_cols[0].name) if pk_cols else 999
                input_data[key] = f"test{pk_val}@example.com"

    return input_data


def short_sql_result(passed, actual_result):
    if passed:
        return "SQL Server INSERT thành công"

    msg = str(actual_result).split("\n")[0]

    if "Cannot insert the value NULL" in msg:
        return "SQL Server từ chối vì cột NOT NULL bị NULL"

    if "FOREIGN KEY" in msg:
        return "SQL Server từ chối vì vi phạm khóa ngoại"

    if "PRIMARY KEY" in msg:
        return "SQL Server từ chối vì trùng khóa chính"

    if "UNIQUE" in msg or "duplicate" in msg.lower():
        return "SQL Server từ chối vì trùng dữ liệu UNIQUE"

    if "CHECK constraint" in msg:
        return "SQL Server từ chối vì vi phạm CHECK constraint"

    if "String or binary data would be truncated" in msg:
        return "SQL Server từ chối vì dữ liệu vượt độ dài cột"

    if "Conversion failed" in msg or "Error converting" in msg:
        return "SQL Server từ chối vì sai kiểu dữ liệu"

    return msg[:180]


def run_one_insert_testcase(engine, table_name, testcase, schema="dbo"):
    tb = Table(table_name, MetaData(), schema=schema, autoload_with=engine)
    input_data = get_input_data(testcase)

    try:
        with engine.begin() as conn:
            final_input_data = complete_insert_data(conn, tb, input_data, testcase)
            conn.execute(insert(tb), final_input_data)

        return True, "SQL Server INSERT thành công", final_input_data

    except Exception as e:
        return False, str(e), final_input_data if "final_input_data" in locals() else input_data


def run_testcases_and_report(db_url, table_name, testcases):
    engine = create_db_engine(db_url)
    report = []

    for tc in testcases:
        test_kind = get_test_kind(tc)
        expected_should_fail = test_kind == "INVALID"

        passed, actual_result, final_input_data = run_one_insert_testcase(
            engine,
            table_name,
            tc
        )

        final_status = "PASS" if (
            (expected_should_fail and not passed)
            or ((not expected_should_fail) and passed)
        ) else "FAIL"

        expected_text = (
            "SQL Server INSERT thành công"
            if not expected_should_fail
            else "SQL Server phải từ chối dữ liệu sai ràng buộc"
        )

        report.append({
            "Tên test case": get_test_name(tc),
            "Loại kiểm thử": get_test_rule(tc),
            "Dữ liệu test": final_input_data,
            "Kết quả mong muốn": expected_text,
            "Kết quả thực tế": short_sql_result(passed, actual_result),
            "Trạng thái": final_status
        })

    passed_count = sum(1 for r in report if r["Trạng thái"] == "PASS")
    failed_count = len(report) - passed_count

    return {
        "Thông báo": f"Đã chạy {len(report)} test case: {passed_count} PASS, {failed_count} FAIL",
        "Tổng số test case": len(report),
        "Số lượng đạt": passed_count,
        "Số lượng không đạt": failed_count,
        "Báo cáo": report
    }


def generate_and_run_testcases(
    db_url,
    table,
    n,
    model="qwen2.5:3b",
    instr=""
):
    engine = create_db_engine(db_url)

    schema = get_table_schema_and_samples(
        engine,
        table,
        sample_limit=2
    )

    prompt = build_testcase_prompt(schema, n, instr)
    raw = call_ollama(model, prompt, timeout=600)

    testcases = parse_json_from_ollama(raw)
    testcases = testcases[:n]

    # Chỉ test INSERT
    for tc in testcases:
        tc["loai_thao_tac"] = "INSERT"

    report = run_testcases_and_report(
        db_url,
        table,
        testcases
    )

    return {
        "Tổng số test case yêu cầu": n,
        "Tổng số test case đã chạy": report["Tổng số test case"],
        "Số lượng đạt": report["Số lượng đạt"],
        "Số lượng không đạt": report["Số lượng không đạt"],
        "Báo cáo": report["Báo cáo"]
    }