from datetime import datetime, date
import re

from sqlalchemy import MetaData, Table, insert, select
from db.connection import create_db_engine
from db.laydulieu import get_table_schema_and_samples
from ai.generator import build_prompt, call_ollama, parse_json_from_ollama

norm = lambda x: str(x).strip().lower()
norm_text = lambda x: "" if x is None else str(x).strip().lower()


def get_column_type_map(schema_info):
    return {c["name"]: str(c.get("type", "")).lower() for c in schema_info["columns"]}


def parse_date_value(value):
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(value).strip(), fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Không parse được date: {value}")


def cast_value_by_sql_type(value, sql_type):
    if value in (None, ""):
        return None
    sql_type = (sql_type or "").lower()
    if "date" in sql_type:
        return parse_date_value(value)
    if "int" in sql_type:
        return int(str(value).strip())
    if any(x in sql_type for x in ["float", "real", "decimal", "numeric"]):
        return float(str(value).strip())
    return str(value).strip()


def sanitize_rows(rows, allowed_columns, column_type_map):
    allowed_map = {norm(c): c for c in allowed_columns}
    type_map = {norm(k): v for k, v in column_type_map.items()}
    result = []

    for row in rows:
        if not isinstance(row, dict):
            continue

        clean = {}
        try:
            for k, v in row.items():
                k2 = norm(k)
                if k2 in allowed_map:
                    clean[allowed_map[k2]] = cast_value_by_sql_type(v, type_map.get(k2, ""))
        except Exception:
            continue

        # Bắt buộc mọi cột đều phải có dữ liệu, không cho NULL
        if clean and all(clean.get(col) not in (None, "", []) for col in allowed_columns):
            result.append(clean)

    return result


def deduplicate_rows(rows):
    seen, unique = set(), []
    for row in rows:
        key = tuple(sorted((k.lower(), str(v)) for k, v in row.items()))
        if key not in seen:
            seen.add(key)
            unique.append(row)
    return unique


def get_existing_pk_values(engine, table_name, pk_columns):
    if not pk_columns:
        return {}
    table = Table(table_name, MetaData(), autoload_with=engine)
    existing = {norm(pk): set() for pk in pk_columns}
    with engine.connect() as conn:
        for row in conn.execute(select(table)).mappings():
            row = {norm(k): v for k, v in row.items()}
            for pk in pk_columns:
                if row.get(norm(pk)) is not None:
                    existing[norm(pk)].add(row[norm(pk)])
    return existing


def make_unique_primary_keys(rows, pk_columns, existing_pk_values, column_type_map):
    pk_list = [norm(pk) for pk in pk_columns]
    type_map = {norm(k): v for k, v in column_type_map.items()}
    result = []

    # Chỉ xử lý mạnh cho PK kiểu int
    next_values = {}

    for pk in pk_list:
        if "int" in type_map.get(pk, ""):
            used = existing_pk_values.setdefault(pk, set())
            next_values[pk] = (max(used) + 1) if used else 1

    for row in rows:
        row = dict(row)
        real_keys = {norm(k): k for k in row}

        for pk in pk_list:
            if pk not in real_keys:
                continue
            if "int" not in type_map.get(pk, ""):
                continue

            key = real_keys[pk]
            used = existing_pk_values.setdefault(pk, set())

            # LUÔN gán lại PK mới, không tin giá trị AI sinh
            new_value = next_values[pk]
            while new_value in used:
                new_value += 1

            row[key] = new_value
            used.add(new_value)
            next_values[pk] = new_value + 1

        result.append(row)

    return result


def is_too_similar_to_sample(row, sample_rows, pk_columns=None):
    if not sample_rows:
        return False

    pk_set = {norm(pk) for pk in (pk_columns or [])}

    for sample in sample_rows:
        common_keys = [
            k for k in row
            if k in sample and norm(k) not in pk_set
        ]

        if not common_keys:
            continue

        matched = sum(
            1 for k in common_keys
            if norm_text(row[k]) == norm_text(sample[k])
        )

        if common_keys and matched / len(common_keys) >= 0.5:
            return True

    return False


def is_invalid_categorical_value(col_name, value):
    if value in (None, "", []):
        return True

    col = norm(col_name)
    text = str(value).strip()

    if "gioitinh" in col:
        return text not in ("Nam", "Nữ")

    if any(x in col for x in ["diachi", "address", "khoa", "faculty", "department"]):
        return bool(re.search(r"\s+\d+$", text))

    if any(x in col for x in ["lop", "class"]):
        return bool(re.search(r"\s+\d+$", text))

    return False


def remove_invalid_rows(rows):
    filtered = []
    for row in rows:
        bad = False
        for col, val in row.items():
            if is_invalid_categorical_value(col, val):
                bad = True
                break
        if not bad:
            filtered.append(row)
    return filtered


def generate_and_insert_data(db_url, table_name, row_count, model_name="qwen2.5:3b", user_instruction=""):
    engine = create_db_engine(db_url)
    schema = get_table_schema_and_samples(engine=engine, table_name=table_name, sample_limit=10)

    allowed_columns = [c["name"] for c in schema["columns"]]
    pk_columns = schema.get("primary_keys", [])
    column_type_map = get_column_type_map(schema)
    existing_pk_values = get_existing_pk_values(engine, table_name, pk_columns)
    sample_rows = schema.get("sample_rows", [])

    all_rows = []
    for _ in range(10):
        need = row_count - len(all_rows)
        if need <= 0:
            break

        rows = parse_json_from_ollama(call_ollama(model_name, build_prompt(schema, need, user_instruction)))
        rows = sanitize_rows(rows, allowed_columns, column_type_map)
        rows = make_unique_primary_keys(rows, pk_columns, existing_pk_values, column_type_map)
        rows = remove_invalid_rows(rows)
        rows = [r for r in rows if not is_too_similar_to_sample(r, sample_rows, pk_columns)]

        all_rows = deduplicate_rows(all_rows + rows)
        if len(all_rows) >= row_count:
            break

    if len(all_rows) < row_count:
        raise ValueError(f"AI chỉ sinh được {len(all_rows)}/{row_count} dòng hợp lệ. Hãy bấm lại hoặc giảm số dòng.")

    rows_to_insert = all_rows[:row_count]
    table = Table(table_name, MetaData(), autoload_with=engine)

    with engine.begin() as conn:
        conn.execute(insert(table), rows_to_insert)

    return {
        "message": f"Đã sinh và insert thành công {len(rows_to_insert)} dòng vào bảng '{table_name}'.",
        "inserted_count": len(rows_to_insert),
        "preview": rows_to_insert[:3]
    }
