from datetime import datetime, date
from decimal import Decimal
import random
import uuid
import re
from sqlalchemy import MetaData, Table, insert, select
from db.connection import create_db_engine
from db.laydulieu import get_table_schema_and_samples
from ai.generator import build_prompt, call_ollama, parse_json_from_ollama
from reports.excel_report import save_generated_data_report


def norm(x):
    return str(x).strip().lower()


def txt(x):
    return "" if x is None else str(x).strip().lower()


def split_table_name(table_name, default_schema="dbo"):
    """
    Nhận cả: SinhVien, dbo.SinhVien, [dbo].[SinhVien]
    Trả về: schema, table
    """
    raw = str(table_name or "").strip().replace("[", "").replace("]", "")
    if "." in raw:
        schema, table = raw.split(".", 1)
        return schema.strip() or default_schema, table.strip()
    return default_schema, raw


def is_identity_col(col):
    v = str(col.get("autoincrement", "")).lower()
    default = str(col.get("default", "")).lower()
    return v in ("true", "auto", "1") or "identity" in default


def type_map(schema):
    return {c["name"]: str(c.get("type", "")).lower() for c in schema["columns"]}


def parse_date(v):
    if v in (None, ""):
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    for f in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, f).date()
        except Exception:
            pass
    return date(2000, 1, 1)


def cast_value(v, sql_type):
    if v in (None, ""):
        return None
    t = (sql_type or "").lower()
    if "date" in t or "time" in t:
        return parse_date(v)
    if "uniqueidentifier" in t:
        try:
            return str(uuid.UUID(str(v)))
        except Exception:
            return str(uuid.uuid4())
    if "bit" in t or "bool" in t:
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in ("1", "true", "yes", "y", "có", "co")
    if "int" in t:
        return int(float(str(v).strip()))
    if any(x in t for x in ("float", "real", "decimal", "numeric", "money")):
        return float(str(v).strip().replace(",", "."))
    return str(v).strip()


def insertable_columns(schema):
    # Không insert cột IDENTITY/computed; SQL Server tự sinh.
    return [c["name"] for c in schema["columns"] if not is_identity_col(c)]


def required_cols(schema):
    return [
        c["name"] for c in schema["columns"]
        if not c.get("nullable", True)
        and not is_identity_col(c)
        and c.get("default") is None
    ]


def clean_rows(rows, allowed_cols, types, required):
    amap = {norm(c): c for c in allowed_cols}
    tmap = {norm(k): v for k, v in types.items()}
    out = []

    for r in rows or []:
        if not isinstance(r, dict):
            continue
        row = {}
        try:
            for k, v in r.items():
                nk = norm(k)
                if nk in amap:
                    real_col = amap[nk]
                    row[real_col] = cast_value(v, tmap.get(nk, ""))

            # Chỉ giữ cột insert được, không ép tất cả cột thành None.
            row = {c: row.get(c) for c in allowed_cols if c in row}

            if all(row.get(c) not in (None, "", []) for c in required):
                out.append(row)
        except Exception:
            continue

    return out


def existing_pk(engine, table, pk_cols, schema="dbo"):
    if not pk_cols:
        return {}
    tb = Table(table, MetaData(), schema=schema, autoload_with=engine)
    data = {norm(p): set() for p in pk_cols}
    with engine.connect() as conn:
        for r in conn.execute(select(tb)).mappings():
            rr = {norm(k): v for k, v in r.items()}
            for p in pk_cols:
                if rr.get(norm(p)) is not None:
                    data[norm(p)].add(rr[norm(p)])
    return data


def fix_pk(rows, pk_cols, used, types, schema):
    # Chỉ tự sửa PK không phải identity và kiểu int.
    identity = {norm(c["name"]) for c in schema["columns"] if is_identity_col(c)}
    tmap = {norm(k): v for k, v in types.items()}

    next_value = {}
    for p in pk_cols:
        np = norm(p)
        if np in identity:
            continue
        if "int" in tmap.get(np, ""):
            vals = [int(x) for x in used.setdefault(np, set()) if isinstance(x, int)] or [0]
            next_value[np] = max(vals) + 1

    for r in rows:
        keys = {norm(k): k for k in r.keys()}
        for np, val in next_value.items():
            if np not in keys:
                continue
            while val in used[np]:
                val += 1
            r[keys[np]] = val
            used[np].add(val)
            next_value[np] = val + 1
    return rows


def existing_values(engine, table, columns, schema="dbo"):
    if not columns:
        return {}
    tb = Table(table, MetaData(), schema=schema, autoload_with=engine)
    data = {tuple(norm(c) for c in cols): set() for cols in columns}

    with engine.connect() as conn:
        for r in conn.execute(select(tb)).mappings():
            rr = {norm(k): v for k, v in r.items()}
            for cols in columns:
                key = tuple(norm(c) for c in cols)
                val = tuple(txt(rr.get(norm(c))) for c in cols)
                if all(x != "" for x in val):
                    data[key].add(val)
    return data


def unique_sets(schema):
    sets = []
    for u in schema.get("unique_constraints", []) or []:
        cols = [c for c in u.get("columns", []) if c]
        if cols:
            sets.append(cols)

    # Dự phòng cho DB introspect thiếu UNIQUE email.
    for c in schema.get("columns", []):
        if "email" in norm(c["name"]):
            sets.append([c["name"]])

    # Loại trùng, giữ composite unique đúng nghĩa.
    seen, out = set(), []
    for cols in sets:
        key = tuple(norm(c) for c in cols)
        if key not in seen:
            seen.add(key)
            out.append(cols)
    return out


def make_unique_value(col, old=None):
    n = norm(col)
    suffix = random.randint(100000, 999999)
    if "email" in n:
        prefix = re.sub(r"[^a-z0-9]+", "", txt(old)) or "user"
        return f"{prefix}_{suffix}@example.com"
    if any(x in n for x in ("phone", "sdt", "dienthoai", "dien_thoai")):
        return "09" + str(random.randint(10000000, 99999999))
    return f"{old or col}_{suffix}"


def fix_unique(rows, schema, engine, table, db_schema):
    sets = unique_sets(schema)
    if not sets:
        return rows

    used = existing_values(engine, table, sets, db_schema)

    for r in rows:
        for cols in sets:
            real_cols = []
            for col in cols:
                real = next((k for k in r if norm(k) == norm(col)), None)
                if real:
                    real_cols.append(real)
            if len(real_cols) != len(cols):
                continue

            key = tuple(norm(c) for c in cols)
            val = tuple(txt(r.get(c)) for c in real_cols)
            if any(x == "" for x in val):
                continue

            if val in used.setdefault(key, set()):
                # Sửa cột cuối trong nhóm unique để không phá các cột khác.
                last = real_cols[-1]
                r[last] = make_unique_value(last, r.get(last))
                val = tuple(txt(r.get(c)) for c in real_cols)
            used[key].add(val)
    return rows


def fk_data(engine, fks, default_schema="dbo"):
    out = []
    with engine.connect() as conn:
        for f in fks or []:
            child = f.get("columns", [])
            parent_table = f.get("referred_table")
            parent_cols = f.get("referred_columns", [])
            parent_schema = f.get("referred_schema") or default_schema
            if not child or not parent_table or not parent_cols:
                continue

            pt = Table(parent_table, MetaData(), schema=parent_schema, autoload_with=engine)
            vals = []
            for r in conn.execute(select(pt)).mappings():
                item = {c: r.get(c) for c in parent_cols}
                if all(v is not None for v in item.values()):
                    vals.append(item)

            if vals:
                out.append({
                    "child_columns": child,
                    "parent_columns": parent_cols,
                    "parent_values": vals,
                })
    return out


def apply_fk(rows, fks):
    for r in rows:
        for f in fks:
            parent = random.choice(f["parent_values"])
            for child_col, parent_col in zip(f["child_columns"], f["parent_columns"]):
                r[child_col] = parent[parent_col]
    return rows


def valid_fk(rows, fks):
    if not fks:
        return rows
    out = []
    for r in rows:
        ok = True
        for f in fks:
            allowed = {tuple(v[c] for c in f["parent_columns"]) for v in f["parent_values"]}
            current = tuple(r.get(c) for c in f["child_columns"])
            if current not in allowed:
                ok = False
                break
        if ok:
            out.append(r)
    return out


def valid_basic(row, required):
    for c in required:
        if row.get(c) in (None, "", []):
            return False
    for c, v in row.items():
        if "gioitinh" in norm(c) and v not in (None, "", []):
            if str(v).strip().lower() not in ("nam", "nữ", "nu", "female", "male"):
                return False
    return True


def too_similar(row, samples, pk_cols):
    pk = {norm(x) for x in pk_cols}
    for sample in samples or []:
        ks = [k for k in row if k in sample and norm(k) not in pk]
        if not ks:
            continue
        same = sum(txt(row[k]) == txt(sample[k]) for k in ks)
        if same == len(ks) or (len(ks) >= 2 and len(ks) - same < 2):
            return True
    return False


def unique_rows(rows):
    seen, out = set(), []
    for r in rows:
        key = tuple(sorted((norm(k), txt(v)) for k, v in r.items()))
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


def prompt_schema(schema, fks):
    x = dict(schema)
    x["foreign_key_reference_samples"] = [
        {
            "child_columns": f["child_columns"],
            "parent_columns": f["parent_columns"],
            "allowed_examples": f["parent_values"][:10],
        }
        for f in fks
    ]
    return x


def insert_one_by_one(engine, table_obj, rows):
    inserted, errors = [], []
    for row in rows:
        try:
            with engine.begin() as conn:
                conn.execute(insert(table_obj), row)
            inserted.append(row)
        except Exception as e:
            errors.append(str(e))
    return inserted, errors


def generate_and_insert_data(db_url, table, n, model="qwen2.5:3b", instr=""):
    engine = create_db_engine(db_url)
    db_schema, table_name = split_table_name(table)

    schema = get_table_schema_and_samples(engine, table_name, schema=db_schema, sample_limit=5, fk_limit=10)

    allowed = insertable_columns(schema)
    pk = schema.get("primary_keys", [])
    types = type_map(schema)
    fks = fk_data(engine, schema.get("foreign_keys", []), default_schema=db_schema)
    used_pk = existing_pk(engine, table_name, pk, schema=db_schema)
    samples = schema.get("sample_rows", [])
    required = required_cols(schema)
    ps = prompt_schema(schema, fks)
    table_obj = Table(table_name, MetaData(), schema=db_schema, autoload_with=engine)

    inserted_rows = []
    last_errors = []

    # Sinh theo lô, insert từng dòng để dòng lỗi không làm hỏng cả lô.
    for _ in range(8):
        need = n - len(inserted_rows)
        if need <= 0:
            break

        try:
            raw = call_ollama(model, build_prompt(ps, max(need * 2, need), instr), timeout=240)
            rows = parse_json_from_ollama(raw)
        except Exception as e:
            last_errors.append(str(e))
            continue

        rows = clean_rows(rows, allowed, types, required)
        rows = apply_fk(rows, fks)
        rows = fix_pk(rows, pk, used_pk, types, schema)
        rows = fix_unique(rows, schema, engine, table_name, db_schema)
        rows = [r for r in rows if valid_basic(r, required) and not too_similar(r, samples, pk)]
        rows = valid_fk(rows, fks)
        rows = unique_rows(rows)

        if not rows:
            continue

        ok_rows, errors = insert_one_by_one(engine, table_obj, rows[:need])
        inserted_rows.extend(ok_rows)
        last_errors.extend(errors[-3:])

    if len(inserted_rows) < n:
        detail = "; ".join(last_errors[-2:]) if last_errors else "Không đủ dữ liệu hợp lệ từ AI"
        raise ValueError(f"AI chỉ sinh và insert được {len(inserted_rows)}/{n}. {detail}")

    report_file = save_generated_data_report(table_name, inserted_rows[:n])

    return {
        "message": f"Đã insert {len(inserted_rows[:n])} dòng vào '{db_schema}.{table_name}'",
        "inserted_count": len(inserted_rows[:n]),
        "preview": inserted_rows[:2],
        "excel_report": report_file,
    }
