import json, requests

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


import json

def build_prompt(schema_info: dict, row_count: int, user_instruction: str = ""):
    extra = user_instruction.strip() or (
        "Chỉ dùng dữ liệu mẫu để học kiểu dữ liệu và phong cách dữ liệu. "
        "Phải sinh dữ liệu mới, khác rõ ràng với dữ liệu mẫu. "
        "Không được sao chép nguyên dòng mẫu. "
        "Không được chỉ thay khóa chính rồi giữ nguyên các cột còn lại."
    )

    return f"""
Bạn là hệ thống sinh dữ liệu cho database.

NHIỆM VỤ:
- Sinh đúng {row_count} dòng dữ liệu mới cho bảng "{schema_info['table_name']}".
- Chỉ dùng dữ liệu mẫu để hiểu kiểu dữ liệu và phong cách dữ liệu.
- Dữ liệu phải MỚI, không được copy lại dữ liệu mẫu.

YÊU CẦU BẮT BUỘC:
1. Chỉ trả về JSON hợp lệ.
2. Không giải thích.
3. Không markdown.
4. Không comment.
5. Không thêm chữ nào ngoài JSON.
6. Kết quả phải là JSON array có đúng {row_count} object.
7. Mỗi object là 1 dòng dữ liệu.
8. Chỉ dùng các cột có trong bảng.
9. Không bỏ cột bắt buộc.
10. Không dùng null cho cột NOT NULL.
11. Với khóa chính kiểu số: sinh giá trị mới, không trùng.
12. Không được sao chép nguyên bất kỳ dòng mẫu nào.
13. Không được chỉ đổi khóa chính rồi giữ nguyên toàn bộ các cột còn lại.
14. Mỗi dòng mới phải khác dữ liệu mẫu ở ít nhất 2 cột không phải khóa chính.
15. Với cột tên/text: sinh giá trị mới, không trùng dữ liệu mẫu.
16. Với cột ngày: sinh ngày mới, không trùng dữ liệu mẫu.
17. Với cột số: sinh giá trị mới, không trùng dữ liệu mẫu.
18. Nếu bảng có rất ít dữ liệu mẫu, vẫn phải tạo biến thể mới hợp lý.
19. DATE phải theo format YYYY-MM-DD.
20. ONLY JSON.

VÍ DỤ SAI:
- Chỉ đổi MaSP từ 1 thành 3 nhưng TenSP, NgayNhap, Gia giữ nguyên.
- Copy lại nguyên dòng mẫu.

VÍ DỤ ĐÚNG:
- MaSP mới, TenSP mới, NgayNhap mới, Gia mới nhưng vẫn cùng loại dữ liệu và cùng phong cách.

Thông tin bảng:
{json.dumps(schema_info, ensure_ascii=False, indent=2, default=str)}

Yêu cầu thêm:
{extra}
""".strip()

def call_ollama(model_name: str, prompt: str):
    ok, _ = check_ollama_available()
    if not ok:
        raise RuntimeError("Chưa bật Ollama. Hãy chạy 'ollama serve'.")

    r = requests.post(GEN_URL, json={"model": model_name, "prompt": prompt, "stream": False}, timeout=180)
    if r.status_code >= 400:
        raise RuntimeError("Ollama đang lỗi hoặc chưa phản hồi đúng.")

    raw = r.json().get("response", "").strip()
    if not raw:
        raise RuntimeError("Ollama trả về rỗng.")
    return raw


def _remove_inline_comments(text: str) -> str:
    return "\n".join(line.split("//")[0].rstrip() for line in text.splitlines()).strip()


def _extract_first_json_block(raw_text: str) -> str:
    raw_text = _remove_inline_comments(raw_text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip())
    decoder = json.JSONDecoder()

    for i, ch in enumerate(raw_text):
        if ch in "[{":
            try:
                _, end = decoder.raw_decode(raw_text[i:])
                return raw_text[i:i + end]
            except:
                pass

    raise ValueError("Không đọc được JSON hợp lệ từ Ollama.")


def parse_json_from_ollama(raw_text: str):
    parsed = json.loads(_extract_first_json_block(raw_text))
    if isinstance(parsed, dict):
        return [parsed]
    if not isinstance(parsed, list):
        raise ValueError("Kết quả từ Ollama không phải object/list hợp lệ.")
    return parsed
