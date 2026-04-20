import json
import requests

BASE = "http://127.0.0.1:11434"
GEN_URL = f"{BASE}/api/generate"
TAGS_URL = f"{BASE}/api/tags"


def check_ollama_available():
    try:
        r = requests.get(TAGS_URL, timeout=10)
        r.raise_for_status()
        return True, r.json()
    except Exception as e:
        return False, str(e)


def build_prompt(schema_info: dict, row_count: int, user_instruction: str = ""):
    extra = user_instruction.strip()

    priority_text = ""
    if extra:
        priority_text = f"""
ƯU TIÊN THEO YÊU CẦU NGƯỜI DÙNG:
- Phải ưu tiên làm theo yêu cầu thêm bên dưới.
- Dữ liệu mẫu chỉ dùng để học kiểu dữ liệu, cấu trúc cột và phong cách chung.
- Nếu yêu cầu thêm khác với dữ liệu mẫu, hãy ưu tiên yêu cầu thêm.
- Không được sao chép nguyên dữ liệu mẫu.
- Vẫn phải giữ đúng kiểu dữ liệu, ràng buộc khóa và format cột.

YÊU CẦU THÊM CỦA NGƯỜI DÙNG:
{extra}
""".strip()
    else:
        priority_text = """
- Dữ liệu mẫu chỉ dùng để học kiểu dữ liệu, cấu trúc cột và phong cách chung.
- Không được sao chép nguyên dữ liệu mẫu.
""".strip()

    fk_text = ""
    if schema_info.get("foreign_keys"):
        fk_text = "\nRÀNG BUỘC KHÓA NGOẠI:\n" + "\n".join(
            f"- {fk.get('columns', [])} tham chiếu {fk.get('referred_table')}({fk.get('referred_columns', [])})"
            for fk in schema_info["foreign_keys"]
        )

    return f"""
Bạn là hệ thống sinh dữ liệu cho database.

NHIỆM VỤ:
- Sinh đúng {row_count} dòng dữ liệu mới cho bảng "{schema_info['table_name']}".
- Phải sinh dữ liệu mới, hợp lệ, đúng schema.
- Không được copy dữ liệu mẫu.

NGUYÊN TẮC QUAN TRỌNG:
1. Ưu tiên yêu cầu người dùng nếu có.
2. Dữ liệu mẫu chỉ để tham khảo kiểu dữ liệu, cấu trúc và phong cách chung.
3. Nếu yêu cầu người dùng khác với nội dung mẫu, vẫn làm theo yêu cầu người dùng.
4. Không được sao chép nguyên bất kỳ dòng mẫu nào.
5. Không được chỉ đổi khóa chính rồi giữ nguyên các cột còn lại.

YÊU CẦU BẮT BUỘC:
1. Chỉ trả về JSON hợp lệ.
2. Không giải thích, không markdown, không comment.
3. Kết quả phải là JSON array có đúng {row_count} object.
4. Chỉ dùng các cột có trong bảng.
5. Không bỏ cột bắt buộc.
6. Không dùng null cho cột NOT NULL.
7. DATE phải theo format YYYY-MM-DD.
8. Mỗi dòng mới phải khác dữ liệu mẫu ít nhất 2 cột không phải khóa chính.
9. ONLY JSON.

Thông tin bảng:
{json.dumps(schema_info, ensure_ascii=False, indent=2, default=str)}
{fk_text}

{priority_text}
""".strip()


def call_ollama(model_name: str, prompt: str, timeout=240):
    ok, _ = check_ollama_available()
    if not ok:
        raise RuntimeError("Chưa bật Ollama. Hãy chạy 'ollama serve'.")

    r = requests.post(
        GEN_URL,
        json={
            "model": model_name,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.7, "num_predict": 1200},
        },
        timeout=timeout,
    )

    if r.status_code >= 400:
        raise RuntimeError(f"Ollama đang lỗi hoặc chưa phản hồi đúng. HTTP {r.status_code}")

    raw = r.json().get("response", "").strip()
    if not raw:
        raise RuntimeError("Ollama trả về rỗng.")
    return raw


def _extract_first_json_block(raw_text: str) -> str:
    text = raw_text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    text = "\n".join(line.split("//")[0].rstrip() for line in text.splitlines()).strip()
    decoder = json.JSONDecoder()

    for i, ch in enumerate(text):
        if ch in "[{":
            try:
                _, end = decoder.raw_decode(text[i:])
                return text[i:i + end]
            except Exception:
                pass

    raise ValueError("Không đọc được JSON hợp lệ từ Ollama.")


def parse_json_from_ollama(raw_text: str):
    data = json.loads(_extract_first_json_block(raw_text))
    if isinstance(data, dict):
        return [data]
    if not isinstance(data, list):
        raise ValueError("Kết quả từ Ollama không phải object/list hợp lệ.")
    return data