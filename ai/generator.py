import json, requests

BASE = "http://127.0.0.1:11434"
GEN = f"{BASE}/api/generate"
TAGS = f"{BASE}/api/tags"


def check_ollama_available():
    try:
        r = requests.get(TAGS, timeout=10); r.raise_for_status()
        return True, r.json()
    except Exception as e:
        return False, str(e)


def build_prompt(schema, n, user_instruction=""):
    extra = user_instruction.strip()

    priority = f"""
ƯU TIÊN YÊU CẦU:
- Làm theo yêu cầu thêm nếu có, không copy dữ liệu mẫu.
- Vẫn giữ đúng kiểu dữ liệu, ràng buộc.

YÊU CẦU THÊM:
{extra}
""".strip() if extra else "- Dữ liệu mẫu chỉ để tham khảo, không được copy."

    fk = "\nRÀNG BUỘC KHÓA NGOẠI:\n" + "\n".join(
        f"- {f.get('columns', [])} -> {f.get('referred_table')}({f.get('referred_columns', [])})"
        for f in schema.get("foreign_keys", [])
    ) if schema.get("foreign_keys") else ""

    return f"""
Sinh đúng {n} dòng dữ liệu hợp lệ cho bảng "{schema['table_name']}".
Chỉ trả về JSON array, không markdown, không giải thích.

Quy tắc:
- Dữ liệu phải INSERT được vào SQL Server.
- Không NULL ở cột NOT NULL.
- Đúng kiểu dữ liệu, đúng độ dài cột.
- Tuân thủ PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK.
- DATE/DATETIME dùng YYYY-MM-DD.
- FOREIGN KEY lấy từ parent_samples.
- Không copy nguyên dòng mẫu.

SCHEMA:
{json.dumps(schema, ensure_ascii=False, default=str)}

YÊU CẦU THÊM:
{extra}
""".strip()

def call_ollama(model, prompt, timeout=240):
    ok, _ = check_ollama_available()
    if not ok:
        raise RuntimeError("Chưa bật Ollama")

    r = requests.post(GEN, json={
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.3, "num_predict": 1000},
    }, timeout=timeout)

    if r.status_code >= 400:
        raise RuntimeError(f"Ollama lỗi {r.status_code}")

    txt = r.json().get("response", "").strip()
    if not txt:
        raise RuntimeError("Ollama trả về rỗng")
    return txt


def _extract_json(txt):
    txt = "\n".join(l.split("//")[0] for l in txt.strip()
                   .removeprefix("```json").removeprefix("```").removesuffix("```")
                   .splitlines()).strip()

    dec = json.JSONDecoder()
    for i, c in enumerate(txt):
        if c in "[{":
            try:
                _, end = dec.raw_decode(txt[i:])
                return txt[i:i+end]
            except:
                pass
    raise ValueError("Không đọc được JSON")


def parse_json_from_ollama(txt):
    data = json.loads(_extract_json(txt))
    if isinstance(data, dict): return [data]
    if not isinstance(data, list): raise ValueError("JSON không hợp lệ")
    return data

def build_testcase_prompt(schema, n, user_instruction=""):
    extra = user_instruction.strip() or "Sinh test case INSERT."

    required_cols = [
        c["name"] for c in schema["columns"]
        if not c.get("nullable", True)
        and str(c.get("autoincrement", "")).lower() != "true"
    ]

    return f"""
Sinh đúng {n} test case INSERT cho bảng "{schema['table_name']}".
Chỉ trả về JSON array, không markdown, không giải thích.
Nội dung mô tả dùng tiếng Việt.

Format:
[
  {{
    "id": 1,
    "ten_testcase": "...",
    "loai_thao_tac": "INSERT",
    "loai_test": "HỢP_LỆ",
    "loai_kiem_thu": "...",
    "du_lieu_test": {{}},
    "ket_qua_mong_muon": "..."
  }}
]

Quy tắc:
- Chỉ sinh INSERT.
- loai_test: HỢP_LỆ hoặc KHÔNG_HỢP_LỆ.
- du_lieu_test phải có đủ cột bắt buộc: {json.dumps(required_cols, ensure_ascii=False)}.
- HỢP_LỆ: dữ liệu thỏa mãn schema, UNIQUE, FOREIGN KEY, CHECK.
- KHÔNG_HỢP_LỆ: chỉ làm sai 1 ràng buộc, cột khác hợp lệ.
- FOREIGN KEY hợp lệ lấy từ parent_samples.
- Ưu tiên làm đúng yêu cầu thêm của người dùng.

SCHEMA:
{json.dumps(schema, ensure_ascii=False, default=str)}

YÊU CẦU THÊM:
{extra}
""".strip()