from flask import Flask, render_template, request, jsonify
from db.connection import test_connection, get_table_names
from services.sinhdulieu import generate_and_insert_data
from services.sinhtestcase import generate_and_run_testcases

app = Flask(__name__)
app.config["SECRET_KEY"] = "secret-key-demo"


@app.route("/")
def index():
    return render_template("index.html")


@app.post("/api/test-connection")
def test_conn():
    db = (request.get_json() or {}).get("db_url", "").strip()

    if not db:
        return jsonify(success=False, message="Thiếu link database."), 400

    ok, msg = test_connection(db)

    if not ok:
        return jsonify(success=False, message=msg), 400

    try:
        return jsonify(
            success=True,
            message="Kết nối thành công.",
            tables=get_table_names(db)
        )

    except Exception as e:
        return jsonify(success=False, message=str(e)), 500


@app.post("/api/generate-data")
def gen_data():
    d = request.get_json() or {}

    db = d.get("db_url", "").strip()
    tb = d.get("table_name", "").strip()
    instr = d.get("user_instruction", "").strip()

    if not db or not tb:
        return jsonify(success=False, message="Thiếu database hoặc bảng."), 400

    try:
        n = int(d.get("row_count", 0))

        if n <= 0:
            raise ValueError

        result = generate_and_insert_data(
            db,
            tb,
            n,
            "qwen2.5:3b",
            instr
        )

        return jsonify(success=True, **result)

    except Exception as e:
        msg = str(e)

        if "Ollama" in msg:
            msg = "Chưa bật Ollama"
        elif "UNIQUE" in msg or "unique" in msg:
            msg = "Trùng khóa chính hoặc UNIQUE"
        elif "FOREIGN KEY" in msg:
            msg = "Sai khóa ngoại"
        elif "CHECK constraint" in msg:
            msg = "Dữ liệu vi phạm CHECK constraint"
        elif "Cannot insert the value NULL" in msg:
            msg = "Thiếu dữ liệu ở cột NOT NULL"
        elif "AI chỉ sinh được" in msg:
            msg = msg
        elif not msg:
            msg = "Lỗi xử lý sinh dữ liệu"

        return jsonify(success=False, message=msg), 400


@app.post("/api/generate-testcases")
def gen_testcases():
    d = request.get_json() or {}

    db = d.get("db_url", "").strip()
    tb = d.get("table_name", "").strip()
    instr = d.get("testcase_instruction", "").strip()

    if not db or not tb:
        return jsonify(success=False, message="Thiếu database hoặc bảng."), 400

    try:
        n = int(d.get("testcase_count", 0))

        if n <= 0:
            raise ValueError

        result = generate_and_run_testcases(
            db,
            tb,
            n,
            "qwen2.5:3b",
            instr
        )

        return jsonify(success=True, **result)

    except Exception as e:
        msg = str(e)

        if "Ollama" in msg:
            msg = "Chưa bật Ollama"
        elif "JSON" in msg:
            msg = "AI trả về sai định dạng JSON"
        elif not msg:
            msg = "Lỗi xử lý sinh và chạy test case"

        return jsonify(success=False, message=msg), 400


if __name__ == "__main__":
    app.run(debug=True)