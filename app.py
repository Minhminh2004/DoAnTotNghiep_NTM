from flask import Flask, render_template, request, jsonify
from db.connection import test_connection, get_table_names
from services.sinhdulieu import generate_and_insert_data

app = Flask(__name__)
app.config["SECRET_KEY"] = "secret-key-demo"


@app.route("/")
def index():
    return render_template("index.html")


@app.post("/api/test-connection")
def api_test_connection():
    db_url = (request.get_json() or {}).get("db_url", "").strip()
    if not db_url:
        return jsonify(success=False, message="Vui lòng nhập link database."), 400

    ok, msg = test_connection(db_url)
    if not ok:
        return jsonify(success=False, message=msg), 400

    try:
        return jsonify(
            success=True,
            message="Kết nối thành công.",
            tables=get_table_names(db_url)
        )
    except Exception as e:
        return jsonify(success=False, message=f"Không đọc được danh sách bảng: {e}"), 500


@app.post("/api/generate-data")
def api_generate_data():
    data = request.get_json() or {}
    db_url = data.get("db_url", "").strip()
    table_name = data.get("table_name", "").strip()
    user_instruction = data.get("user_instruction", "").strip()

    if not db_url:
        return jsonify(success=False, message="Thiếu link database."), 400
    if not table_name:
        return jsonify(success=False, message="Vui lòng chọn bảng."), 400

    try:
        row_count = int(data.get("row_count", 0))
        if row_count <= 0:
            raise ValueError
    except Exception:
        return jsonify(success=False, message="Số dòng cần sinh không hợp lệ."), 400

    try:
        result = generate_and_insert_data(
            db_url=db_url,
            table_name=table_name,
            row_count=row_count,
            model_name="qwen2.5:3b",
            user_instruction=user_instruction
        )
        return jsonify(
            success=True,
            message=result["message"],
            inserted_count=result["inserted_count"],
            preview=result.get("preview", [])
        )
    except ValueError as e:
        return jsonify(success=False, message=str(e)), 400
    except Exception as e:
        msg = str(e)
        if "UNIQUE constraint failed" in msg:
            msg = "Dữ liệu sinh ra bị trùng khóa chính hoặc cột duy nhất."
        elif "date and/or time" in msg:
            msg = "Dữ liệu ngày giờ sinh ra chưa đúng định dạng."
        elif "Không kết nối được Ollama" in msg:
            msg = "Chưa kết nối được Ollama. Hãy kiểm tra Ollama đang chạy."
        return jsonify(success=False, message=msg), 500


if __name__ == "__main__":
    app.run(debug=True)
