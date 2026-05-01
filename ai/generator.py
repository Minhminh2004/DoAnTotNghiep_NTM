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
Sinh đúng {n} dòng dữ liệu mới cho bảng "{schema['table_name']}", không copy mẫu.

YÊU CẦU:
- ONLY JSON array {n} object
- Không markdown / giải thích
- Không null cột NOT NULL
- DATE: YYYY-MM-DD
- Mỗi dòng khác mẫu ≥2 cột (không tính PK)

Schema:
{json.dumps(schema, ensure_ascii=False, indent=2, default=str)}
{fk}

{priority}
""".strip()


def call_ollama(model, prompt, timeout=240):
    ok, _ = check_ollama_available()
    if not ok:
        raise RuntimeError("Chưa bật Ollama")

    r = requests.post(GEN, json={
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.7, "num_predict": 1200},
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