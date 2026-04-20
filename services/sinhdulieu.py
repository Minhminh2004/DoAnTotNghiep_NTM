from datetime import datetime, date
import random

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


def get_required_non_fk_columns(schema):
    fk_cols = {col for fk in schema.get("foreign_keys", []) for col in fk.get("columns", [])}
    return [
        c["name"]
        for c in schema["columns"]
        if c.get("nullable", True) is False
        and str(c.get("autoincrement", "")).lower() != "true"
        and c["name"] not in fk_cols
    ]


def sanitize_rows(rows, allowed_columns, column_type_map, required_columns):
    allowed_map = {norm(c): c for c in allowed_columns}
    type_map = {norm(k): v for k, v in column_type_map.items()}
    result = []

    for row in rows:
        if not isinstance(row, dict):
            continue

        clean = {col: None for col in allowed_columns}
        try:
            for k, v in row.items():
                key = norm(k)
                if key in allowed_map:
                    clean[allowed_map[key]] = cast_value_by_sql_type(v, type_map.get(key, ""))
        except Exception:
            continue

        if all(clean.get(col) not in (None, "", []) for col in required_columns):
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


def get_lay_pk_values(engine, table_name, pk_columns):
    if not pk_columns:
        return {}

    table = Table(table_name, MetaData(), autoload_with=engine)
    existing = {norm(pk): set() for pk in pk_columns}

    with engine.connect() as conn:
        for row in conn.execute(select(table)).mappings():
            row = {norm(k): v for k, v in row.items()}
            for pk in pk_columns:
                value = row.get(norm(pk))
                if value is not None:
                    existing[norm(pk)].add(value)

    return existing


def make_unique_primary_keys(rows, pk_columns, existing_pk_values, column_type_map):
    pk_list = [norm(pk) for pk in pk_columns]
    type_map = {norm(k): v for k, v in column_type_map.items()}
    next_values = {}

    for pk in pk_list:
        if "int" in type_map.get(pk, ""):
            used = existing_pk_values.setdefault(pk, set())
            next_values[pk] = (max(used) + 1) if used else 1

    out = []
    for row in rows:
        row = dict(row)
        real_keys = {norm(k): k for k in row}

        for pk in pk_list:
            if pk in real_keys and "int" in type_map.get(pk, ""):
                key = real_keys[pk]
                used = existing_pk_values.setdefault(pk, set())

                value = next_values[pk]
                while value in used:
                    value += 1

                row[key] = value
                used.add(value)
                next_values[pk] = value + 1

        out.append(row)

    return out


def is_invalid_categorical_value(col_name, value):
    if value in (None, "", []):
        return True
    if "gioitinh" in norm(col_name):
        return str(value).strip().lower() not in ("nam", "nữ", "nu")
    return False


def remove_invalid_rows(rows):
    return [
        row for row in rows
        if not any(is_invalid_categorical_value(col, val) for col, val in row.items())
    ]


def get_fk_reference_data(engine, foreign_keys):
    ref_data = []

    with engine.connect() as conn:
        for fk in foreign_keys or []:
            child_cols = fk.get("columns", [])
            parent_table = fk.get("referred_table")
            parent_cols = fk.get("referred_columns", [])

            if not child_cols or not parent_table or not parent_cols:
                continue

            table = Table(parent_table, MetaData(), autoload_with=engine)
            rows = conn.execute(select(table)).mappings().all()

            parent_values = []
            for row in rows:
                item = {col: row.get(col) for col in parent_cols}
                if all(v is not None for v in item.values()):
                    parent_values.append(item)

            if parent_values:
                ref_data.append({
                    "child_columns": child_cols,
                    "parent_columns": parent_cols,
                    "parent_values": parent_values,
                })

    return ref_data


def apply_foreign_keys(rows, fk_reference_data):
    if not fk_reference_data:
        return rows

    out = []
    for row in rows:
        row = dict(row)
        for fk in fk_reference_data:
            picked = random.choice(fk["parent_values"])
            for child_col, parent_col in zip(fk["child_columns"], fk["parent_columns"]):
                row[child_col] = picked[parent_col]
        out.append(row)
    return out


def validate_foreign_keys(rows, fk_reference_data):
    if not fk_reference_data:
        return rows

    valid_rows = []
    for row in rows:
        ok = True
        for fk in fk_reference_data:
            allowed = {
                tuple(ref[col] for col in fk["parent_columns"])
                for ref in fk["parent_values"]
            }
            row_key = tuple(row.get(col) for col in fk["child_columns"])
            if row_key not in allowed:
                ok = False
                break
        if ok:
            valid_rows.append(row)

    return valid_rows


def is_too_similar_to_sample(row, sample_rows, pk_columns=None):
    if not sample_rows:
        return False

    pk_set = {norm(pk) for pk in (pk_columns or [])}
    for sample in sample_rows:
        common_keys = [k for k in row if k in sample and norm(k) not in pk_set]
        if not common_keys:
            continue

        same_count = sum(1 for k in common_keys if norm_text(row[k]) == norm_text(sample[k]))
        if same_count == len(common_keys):
            return True
        if len(common_keys) >= 2 and (len(common_keys) - same_count) < 2:
            return True

    return False


def build_schema_for_prompt(schema, fk_reference_data):
    prompt_schema = dict(schema)
    prompt_schema["foreign_key_reference_samples"] = [
        {
            "child_columns": fk["child_columns"],
            "parent_columns": fk["parent_columns"],
            "allowed_examples": fk["parent_values"][:8],
        }
        for fk in fk_reference_data
    ]
    return prompt_schema


def generate_and_insert_data(db_url, table_name, row_count, model_name="qwen2.5:3b", user_instruction=""):
    engine = create_db_engine(db_url)
    schema = get_table_schema_and_samples(engine=engine, table_name=table_name, sample_limit=5)

    allowed_columns = [c["name"] for c in schema["columns"]]
    pk_columns = schema.get("primary_keys", [])
    fk_list = schema.get("foreign_keys", [])
    column_type_map = get_column_type_map(schema)
    existing_pk_values = get_existing_pk_values(engine, table_name, pk_columns)
    sample_rows = schema.get("sample_rows", [])
    required_columns = get_required_non_fk_columns(schema)

    fk_reference_data = get_fk_reference_data(engine, fk_list)
    prompt_schema = build_schema_for_prompt(schema, fk_reference_data)

    all_rows = []
    for _ in range(5):
        need = row_count - len(all_rows)
        if need <= 0:
            break

        raw_text = call_ollama(model_name, build_prompt(prompt_schema, need, user_instruction), timeout=240)

        try:
            parsed_rows = parse_json_from_ollama(raw_text)
        except Exception:
            continue

        rows = sanitize_rows(parsed_rows, allowed_columns, column_type_map, required_columns)
        rows = apply_foreign_keys(rows, fk_reference_data)
        rows = make_unique_primary_keys(rows, pk_columns, existing_pk_values, column_type_map)
        rows = remove_invalid_rows(rows)
        rows = validate_foreign_keys(rows, fk_reference_data)
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
        "preview": rows_to_insert[:2],
    }