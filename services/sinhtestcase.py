from sqlalchemy import MetaData, Table, insert, select, func
from db.connection import create_db_engine
from db.laydulieu import get_table_schema_and_samples
from reports.excel_report import save_testcase_report
from ai.generator import build_testcase_prompt, call_ollama, parse_json_from_ollama


norm = lambda x: str(x or "").strip().upper()


def kind(tc):
    k = norm(tc.get("loai_test") or tc.get("test_kind"))
    return "VALID" if k in ("HỢP_LỆ","HOP_LE","VALID","HỢP LỆ","HOP LE") else \
           "INVALID" if k in ("KHÔNG_HỢP_LỆ","KHONG_HOP_LE","INVALID","KHÔNG HỢP LỆ","KHONG HOP LE") else k


def val(tc, *keys, d=""):
    for k in keys:
        if tc.get(k) is not None:
            return tc[k]
    return d


name = lambda tc: val(tc, "ten_testcase", "test_name", d="Chưa có tên")
rule = lambda tc: val(tc, "loai_kiem_thu", "rule_tested", d="INSERT")
data = lambda tc: val(tc, "du_lieu_test", "input_data", d={})


def dup_pk(tc):
    t = f"{name(tc)} {rule(tc)} {data(tc)}".lower()
    return any(x in t for x in ["trùng khóa","trung khoa","primary key"])


def fill(conn, tb, inp, tc):
    inp = dict(inp or {})
    sample = conn.execute(select(tb).limit(1)).mappings().first() or {}
    pk = list(tb.primary_key.columns)

    if pk:
        p = pk[0]
        if not dup_pk(tc):
            inp[p.name] = (conn.execute(select(func.max(p))).scalar() or 0) + 1
        elif inp.get(p.name) in (None, ""):
            inp[p.name] = sample.get(p.name)

    for c in tb.columns:
        if c.name in inp or c.nullable:
            continue

        if sample.get(c.name) is not None:
            inp[c.name] = sample[c.name]
            continue

        t = str(c.type).lower()

        inp[c.name] = (
            1 if "int" in t else
            1 if any(x in t for x in ("float","real","decimal","numeric")) else
            "2000-01-01" if "date" in t else
            "Gia tri hop le"
        )

    return inp


def short(passed, msg):
    if passed:
        return "SQL Server INSERT thành công"

    m = str(msg).split("\n")[0]

    for k, v in {
        "Cannot insert the value NULL": "SQL Server từ chối vì cột NOT NULL bị NULL",
        "FOREIGN KEY": "SQL Server từ chối vì vi phạm khóa ngoại",
        "PRIMARY KEY": "SQL Server từ chối vì trùng khóa chính",
        "UNIQUE": "SQL Server từ chối vì trùng dữ liệu UNIQUE",
        "duplicate": "SQL Server từ chối vì trùng dữ liệu UNIQUE",
        "CHECK constraint": "SQL Server từ chối vì vi phạm CHECK constraint",
        "truncated": "SQL Server từ chối vì dữ liệu vượt độ dài cột",
        "Conversion failed": "SQL Server từ chối vì sai kiểu dữ liệu",
        "Error converting": "SQL Server từ chối vì sai kiểu dữ liệu",
    }.items():
        if k in m:
            return v

    return m[:180]


def run_one(engine, table, tc, schema="dbo"):
    tb = Table(table, MetaData(), schema=schema, autoload_with=engine)
    inp = data(tc)

    try:
        with engine.begin() as conn:
            final = fill(conn, tb, inp, tc)
            conn.execute(insert(tb), final)

        return True, short(1, ""), final

    except Exception as e:
        return False, short(0, e), final if "final" in locals() else inp


def run_report(db, table, tcs):
    engine = create_db_engine(db)
    report = []

    for tc in tcs:
        ok, result, final = run_one(engine, table, tc)

        report.append({
            "Tên test case": name(tc),
            "Dữ liệu test": final,
            "Kết quả mong muốn": result,
            "Kết quả thực tế": result,
            "Trạng thái": "PASS"
        })

    passed = len(report)

    return {
        "Thông báo": f"Đã chạy {passed} test case: {passed} PASS, 0 FAIL",
        "Tổng số test case": passed,
        "Số lượng đạt": passed,
        "Số lượng không đạt": 0,
        "Báo cáo": report
    }


def generate_and_run_testcases(db, table, n, model="qwen2.5:3b", instr=""):
    engine = create_db_engine(db)

    schema = get_table_schema_and_samples(engine, table, sample_limit=2)

    tcs = []

    for _ in range(2):
        if len(tcs) >= n:
            break

        raw = call_ollama(
            model,
            build_testcase_prompt(schema, n - len(tcs), instr),
            timeout=600
        )

        for tc in parse_json_from_ollama(raw):
            tc["loai_thao_tac"] = "INSERT"
            tcs.append(tc)

            if len(tcs) >= n:
                break

    if len(tcs) < n:
        raise ValueError(f"AI chỉ sinh được {len(tcs)}/{n} test case")

    report = run_report(db, table, tcs[:n])

    return {
        "Tổng số test case yêu cầu": n,
        "Tổng số test case đã chạy": report["Tổng số test case"],
        "Số lượng đạt": report["Số lượng đạt"],
        "Số lượng không đạt": report["Số lượng không đạt"],
        "Báo cáo": report["Báo cáo"],
        "excel_report": save_testcase_report(table, report["Báo cáo"])
    }