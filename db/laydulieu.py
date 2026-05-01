from datetime import date, datetime, time
from decimal import Decimal
from sqlalchemy import inspect, MetaData, Table, select


def json_val(v):
    if v is None: return None
    if isinstance(v, (datetime, date, time)): return str(v)
    if isinstance(v, Decimal): return float(v)
    if isinstance(v, bytes):
        try: return v.decode("utf-8")
        except: return str(v)
    return v


def get_existing_pk_values(engine, table, schema="dbo"):
    pk = (inspect(engine).get_pk_constraint(table, schema=schema) or {}).get("constrained_columns", [])
    if not pk: return []

    tb = Table(table, MetaData(), schema=schema, autoload_with=engine)
    with engine.connect() as c:
        rows = c.execute(select(tb)).mappings().all()

    return [json_val(r[pk[0]]) if len(pk)==1 else {k: json_val(r[k]) for k in pk} for r in rows]


def get_table_schema_and_samples(engine, table, schema="dbo", sample_limit=4, fk_limit=4):
    ins = inspect(engine)
    cols = ins.get_columns(table, schema=schema)
    pk = (ins.get_pk_constraint(table, schema=schema) or {}).get("constrained_columns", [])
    fks = ins.get_foreign_keys(table, schema=schema) or []

    tb = Table(table, MetaData(), schema=schema, autoload_with=engine)
    FK, parents = [], []

    with engine.connect() as c:
        samples = c.execute(select(tb).limit(sample_limit)).mappings().all()

        for f in fks:
            child, ref_tb, ref_cols = f.get("constrained_columns", []), f.get("referred_table"), f.get("referred_columns", [])
            ref_schema = f.get("referred_schema") or schema
            if not ref_tb or not ref_cols: continue

            FK.append({"columns": child, "referred_schema": ref_schema, "referred_table": ref_tb, "referred_columns": ref_cols})

            pt = Table(ref_tb, MetaData(), schema=ref_schema, autoload_with=engine)
            rows = c.execute(select(pt).limit(fk_limit)).mappings().all()

            parents.append({
                "table": ref_tb,
                "schema": ref_schema,
                "child_columns": child,
                "referred_columns": ref_cols,
                "existing_pk_values": get_existing_pk_values(engine, ref_tb, ref_schema),
                "sample_rows": [{k: json_val(v) for k, v in dict(r).items()} for r in rows],
            })

    return {
        "table_name": table,
        "schema": schema,
        "columns": [{
            "name": c["name"],
            "type": str(c["type"]),
            "nullable": c.get("nullable", True),
            "default": str(c.get("default")) if c.get("default") is not None else None,
            "primary_key": c["name"] in pk,
            "autoincrement": str(c.get("autoincrement", False)),
        } for c in cols],
        "primary_keys": pk,
        "foreign_keys": FK,
        "parent_samples": parents,
        "sample_rows": [{k: json_val(v) for k, v in dict(r).items()} for r in samples],
    }