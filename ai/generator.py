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
    import json

    extra = user_instruction.strip()

    identity_cols = [
        c["name"]
        for c in schema.get("columns", [])
        if str(c.get("autoincrement", "")).lower() == "true"
    ]

    required_cols = [
        c["name"]
        for c in schema.get("columns", [])
        if not c.get("nullable", True)
        and c["name"] not in identity_cols
    ]

    fk = "\nRÀNG BUỘC KHÓA NGOẠI:\n" + "\n".join(
        f"- {f.get('columns', [])} -> {f.get('referred_table')}({f.get('referred_columns', [])})"
        for f in schema.get("foreign_keys", [])
    ) if schema.get("foreign_keys") else ""

    return f"""
Sinh đúng {n} dòng dữ liệu hợp lệ cho bảng "{schema['table_name']}".

Chỉ trả về JSON array.
Không markdown.
Không giải thích.

QUY TẮC:
- Dữ liệu phải INSERT được vào SQL Server.
- Đúng tên cột, đúng kiểu dữ liệu.
- Không NULL ở cột bắt buộc: {required_cols}
- Tuân thủ PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK.
- DATE/DATETIME dùng YYYY-MM-DD.
- FOREIGN KEY lấy từ parent_samples.
- Không copy nguyên dòng mẫu.
- Dữ liệu phải tự nhiên và hợp lý theo tên bảng/tên cột.
- Nếu là tên người thì dùng tên Việt Nam thật.
- Nếu là email thì email hợp lệ và không trùng.
- Không dùng dữ liệu kiểu Test, Demo, Sample.

{fk}

YÊU CẦU THÊM:
{extra if extra else "Không có"}

SCHEMA:
{json.dumps(schema, ensure_ascii=False, default=str)}
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