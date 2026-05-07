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
- Nếu là dữ liệu người Việt: sinh họ tên Việt Nam thực tế, có dấu, đa dạng.
- Không dùng tên mẫu kiểu Nguyen Van A, Nguyen Van B, Test, Demo.
- Không lặp tên trong cùng lần sinh.
- Email phải khớp tương đối với họ tên, không dùng test@example.com nếu không cần.


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
Mô tả dùng tiếng Việt.

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

Luật bắt buộc:
- Chỉ sinh INSERT.
- Phải sinh đúng {n} object trong JSON array.
- Nếu YÊU CẦU NGƯỜI DÙNG có nhiều dòng bắt đầu bằng "-", mỗi dòng là 1 test case riêng.
- Không được gộp 2 yêu cầu vào 1 test case.
- Chỉ dùng tiếng Việt Unicode UTF-8 chuẩn.
- du_lieu_test phải có đủ cột bắt buộc: {json.dumps(required_cols, ensure_ascii=False)}.
- Nếu yêu cầu "điểm 11" thì chỉ cột điểm sai, các cột khác hợp lệ.
- Nếu yêu cầu "tuổi 17" thì chỉ cột tuổi sai, các cột khác hợp lệ.
- HỢP_LỆ: dữ liệu thỏa mãn schema, CHECK, UNIQUE, FOREIGN KEY.
- KHÔNG_HỢP_LỆ: chỉ làm sai đúng 1 ràng buộc được yêu cầu.
- FOREIGN KEY hợp lệ lấy từ parent_samples.
- PRIMARY KEY hợp lệ phải là giá trị mới, không trùng dữ liệu mẫu.
- Nếu có cột HoTen/họ tên:
  + BẮT BUỘC dùng tên người Việt Nam thật.
  + Ví dụ hợp lệ:
    "Phạm Minh Tuấn"
    "Nguyễn Thị Thu Hà"
    "Trần Quốc Bảo"
    "Lê Hoàng Nam"
    "Đặng Gia Huy"
  + CẤM dùng:
    "Nguyễn Văn A"
    "Nguyễn Văn B"
    "Trần Thị B"
    "Test"
    "Demo"
    "User"
- Nếu có email, email nên khớp với họ tên và không trùng dữ liệu mẫu.
- ket_qua_mong_muon:
  + HỢP_LỆ: "SQL Server phải INSERT thành công"
  + KHÔNG_HỢP_LỆ: "SQL Server phải từ chối dữ liệu sai ràng buộc"

SCHEMA:
{json.dumps(schema, ensure_ascii=False, default=str)}

YÊU CẦU NGƯỜI DÙNG, PHẢI ƯU TIÊN TỪNG DÒNG:
{extra}
""".strip()