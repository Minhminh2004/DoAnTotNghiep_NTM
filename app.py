from flask import Flask, render_template, request, jsonify
from db.connection import test_connection, get_table_names
from services.sinhdulieu import generate_and_insert_data

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
        return jsonify(success=True, message="Kết nối thành công.", tables=get_table_names(db))
    except Exception as e:
        return jsonify(success=False, message=str(e)), 500


@app.post("/api/generate-data")
def gen_data():
    d = request.get_json() or {}
    db, tb = d.get("db_url","").strip(), d.get("table_name","").strip()
    instr = d.get("user_instruction","").strip()

    if not db or not tb:
        return jsonify(success=False, message="Thiếu database hoặc bảng."), 400

    try:
        n = int(d.get("row_count", 0))
        if n <= 0: raise ValueError

        return jsonify(success=True, **generate_and_insert_data(db, tb, n, "qwen2.5:3b", instr))

    except:
        msg = str(Exception())
        if "UNIQUE constraint failed" in msg:
            msg = "Trùng khóa chính / unique"
        elif "date" in msg:
            msg = "Sai định dạng ngày"
        elif "Ollama" in msg:
            msg = "Chưa bật Ollama"
        return jsonify(success=False, message=msg or "Lỗi xử lý"), 400


if __name__ == "__main__":
    app.run(debug=True)