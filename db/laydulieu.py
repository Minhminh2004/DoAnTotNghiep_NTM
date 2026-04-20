from datetime import date, datetime, time
from decimal import Decimal
from sqlalchemy import inspect, MetaData, Table, select


def convert_value_for_json(value):
    if value is None:
        return None
    if isinstance(value, (datetime, date, time)):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return str(value)
    return value


def get_table_schema_and_samples(engine, table_name: str, sample_limit: int = 4, fk_sample_limit: int = 4):
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    primary_keys = (inspector.get_pk_constraint(table_name) or {}).get("constrained_columns", [])
    foreign_keys = [
        {
            "columns": fk.get("constrained_columns", []),
            "referred_table": fk.get("referred_table"),
            "referred_columns": fk.get("referred_columns", []),
        }
        for fk in (inspector.get_foreign_keys(table_name) or [])
    ]

    table = Table(table_name, MetaData(), autoload_with=engine)

    with engine.connect() as conn:
        sample_rows = conn.execute(select(table).limit(sample_limit)).mappings().all()

        parent_samples = []
        for fk in foreign_keys:
            ref_table = fk.get("referred_table")
            ref_cols = fk.get("referred_columns", [])
            if not ref_table or not ref_cols:
                continue

            parent_table = Table(ref_table, MetaData(), autoload_with=engine)
            parent_rows = conn.execute(select(parent_table).limit(fk_sample_limit)).mappings().all()

            parent_samples.append({
                "table": ref_table,
                "columns": ref_cols,
                "sample_rows": [
                    {k: convert_value_for_json(v) for k, v in dict(row).items()}
                    for row in parent_rows
                ],
            })

    return {
        "table_name": table_name,
        "columns": [
            {
                "name": col["name"],
                "type": str(col["type"]),
                "nullable": col.get("nullable", True),
                "default": str(col.get("default")) if col.get("default") is not None else None,
                "primary_key": col["name"] in primary_keys,
                "autoincrement": str(col.get("autoincrement", False)),
            }
            for col in columns
        ],
        "primary_keys": primary_keys,
        "foreign_keys": foreign_keys,
        "parent_samples": parent_samples,
        "sample_rows": [
            {k: convert_value_for_json(v) for k, v in dict(row).items()}
            for row in sample_rows
        ],
    }