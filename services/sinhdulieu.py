from datetime import datetime, date
import random
from sqlalchemy import MetaData, Table, insert, select
from db.connection import create_db_engine
from db.laydulieu import get_table_schema_and_samples
from ai.generator import build_prompt, call_ollama, parse_json_from_ollama
from reports.excel_report import save_generated_data_report

norm = lambda x: str(x).strip().lower()
txt = lambda x: "" if x is None else str(x).strip().lower()

type_map = lambda s: {c["name"]: str(c.get("type","")).lower() for c in s["columns"]}


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
        except:
            pass

    # fallback để tránh lỗi sai định dạng ngày
    return date(2000, 1, 1)


def cast(v,t):
    if v in (None,""): return None
    t=(t or "").lower()
    if "date" in t: return parse_date(v)
    if "int" in t: return int(str(v).strip())
    if any(x in t for x in ("float","real","decimal","numeric")): return float(str(v).strip())
    return str(v).strip()


def required_cols(s):
    fk={c for f in s.get("foreign_keys",[]) for c in f.get("columns",[])}
    return [c["name"] for c in s["columns"]
            if not c.get("nullable",True)
            and str(c.get("autoincrement","")).lower()!="true"
            and c["name"] not in fk]


def clean_rows(rows,allowed,types,req):
    amap,tmap={norm(c):c for c in allowed},{norm(k):v for k,v in types.items()}
    out=[]
    for r in rows:
        if not isinstance(r,dict): continue
        row={c:None for c in allowed}
        try:
            for k,v in r.items():
                if norm(k) in amap:
                    row[amap[norm(k)]]=cast(v,tmap.get(norm(k),""))
            if all(row.get(c) not in (None,"",[]) for c in req):
                out.append(row)
        except: pass
    return out


def unique(rows):
    seen,out=set(),[]
    for r in rows:
        k=tuple(sorted((k.lower(),str(v)) for k,v in r.items()))
        if k not in seen: seen.add(k); out.append(r)
    return out


def existing_pk(engine,table,pk,schema="dbo"):
    if not pk: return {}
    tb=Table(table,MetaData(),schema=schema,autoload_with=engine)
    data={norm(p):set() for p in pk}
    with engine.connect() as c:
        for r in c.execute(select(tb)).mappings():
            r={norm(k):v for k,v in r.items()}
            for p in pk:
                if r.get(norm(p)) is not None:
                    data[norm(p)].add(r[norm(p)])
    return data


def fix_pk(rows,pk,used,types):
    tmap={norm(k):v for k,v in types.items()}
    nxt={norm(p):max(used.setdefault(norm(p),{0}))+1
         for p in pk if "int" in tmap.get(norm(p),"")}
    for r in rows:
        keys={norm(k):k for k in r}
        for p in nxt:
            if p in keys:
                while nxt[p] in used[p]: nxt[p]+=1
                r[keys[p]]=nxt[p]; used[p].add(nxt[p]); nxt[p]+=1
    return rows

def existing_values(engine, table, columns, schema="dbo"):
    tb = Table(table, MetaData(), schema=schema, autoload_with=engine)
    data = {norm(c): set() for c in columns}

    with engine.connect() as c:
        for r in c.execute(select(tb)).mappings():
            rr = {norm(k): v for k, v in r.items()}
            for col in columns:
                if rr.get(norm(col)) is not None:
                    data[norm(col)].add(str(rr[norm(col)]).lower())

    return data


def fix_unique(rows, schema, engine, table):
    unique_cols = []

    for u in schema.get("unique_constraints", []):
        for c in u.get("columns", []):
            unique_cols.append(c)

    # bắt thêm các cột hay unique như Email nếu DB khai báo không lấy được
    for c in schema.get("columns", []):
        name = c["name"]
        if "email" in norm(name):
            unique_cols.append(name)

    unique_cols = list(dict.fromkeys(unique_cols))

    if not unique_cols:
        return rows

    used = existing_values(engine, table, unique_cols)

    for i, r in enumerate(rows, start=1):
        for col in unique_cols:
            real_col = next((k for k in r if norm(k) == norm(col)), None)
            if not real_col:
                continue

            val = r.get(real_col)

            if val is None or str(val).lower() in used.get(norm(col), set()):
                if "email" in norm(real_col):
                    new_val = f"test_{random.randint(100000, 999999)}@example.com"
                else:
                    new_val = f"{real_col}_{random.randint(100000, 999999)}"

                r[real_col] = new_val
                used.setdefault(norm(col), set()).add(str(new_val).lower())
            else:
                used.setdefault(norm(col), set()).add(str(val).lower())

    return rows

def fk_data(engine,fks):
    out=[]
    with engine.connect() as c:
        for f in fks or []:
            child,table,cols=f.get("columns",[]),f.get("referred_table"),f.get("referred_columns",[])
            schema=f.get("referred_schema") or "dbo"
            if not child or not table or not cols: continue
            tb=Table(table,MetaData(),schema=schema,autoload_with=engine)
            vals=[{c:r.get(c) for c in cols} for r in c.execute(select(tb)).mappings()]
            vals=[v for v in vals if all(x is not None for x in v.values())]
            if vals: out.append({"child_columns":child,"parent_columns":cols,"parent_values":vals})
    return out


def apply_fk(rows,fks):
    for r in rows:
        for f in fks:
            p=random.choice(f["parent_values"])
            for c,pcol in zip(f["child_columns"],f["parent_columns"]):
                r[c]=p[pcol]
    return rows


def valid_fk(rows,fks):
    if not fks: return rows
    out=[]
    for r in rows:
        ok=True
        for f in fks:
            allowed={tuple(v[c] for c in f["parent_columns"]) for v in f["parent_values"]}
            if tuple(r.get(c) for c in f["child_columns"]) not in allowed:
                ok=False; break
        if ok: out.append(r)
    return out


def valid_cate(r, req=None):
    req = req or []

    for c in req:
        if r.get(c) in (None, "", []):
            return False

    for c, v in r.items():
        if "gioitinh" in norm(c) and v not in (None, "", []):
            if str(v).lower() not in ("nam", "nữ", "nu"):
                return False

    return True


def too_similar(r,samples,pk):
    pk={norm(x) for x in pk}
    for s in samples or []:
        ks=[k for k in r if k in s and norm(k) not in pk]
        if ks:
            same=sum(txt(r[k])==txt(s[k]) for k in ks)
            if same==len(ks) or (len(ks)>=2 and len(ks)-same<2): return True
    return False


def prompt_schema(s,fks):
    x=dict(s)
    x["foreign_key_reference_samples"]=[{
        "child_columns":f["child_columns"],
        "parent_columns":f["parent_columns"],
        "allowed_examples":f["parent_values"][:8],
    } for f in fks]
    return x


def generate_and_insert_data(db_url,table,n,model="qwen2.5:3b",instr=""):
    engine=create_db_engine(db_url)
    schema=get_table_schema_and_samples(engine,table,sample_limit=5)

    allowed=[c["name"] for c in schema["columns"]]
    pk=schema.get("primary_keys",[])
    types=type_map(schema)
    fks=fk_data(engine,schema.get("foreign_keys",[]))
    used=existing_pk(engine,table,pk)
    samples=schema.get("sample_rows",[])
    req=required_cols(schema)
    ps=prompt_schema(schema,fks)

    all_rows=[]
    for _ in range(5):
        need=n-len(all_rows)
        if need<=0: break
        try:
            raw=call_ollama(model,build_prompt(ps,need,instr),timeout=240)
            rows=parse_json_from_ollama(raw)
        except: continue

        rows=clean_rows(rows,allowed,types,req)
        rows=apply_fk(rows,fks)
        rows=fix_pk(rows,pk,used,types)
        rows=fix_unique(rows, schema, engine, table)
        rows=[r for r in rows if valid_cate(r, req) and not too_similar(r,samples,pk)]
        rows=valid_fk(rows,fks)
        all_rows=unique(all_rows+rows)

    if len(all_rows)<n:
        raise ValueError(f"AI chỉ sinh được {len(all_rows)}/{n}")

    rows=all_rows[:n]
    tb=Table(table,MetaData(),schema="dbo",autoload_with=engine)

    with engine.begin() as c:
        c.execute(insert(tb),rows)

        report_file = save_generated_data_report(table, rows)

    return {
        "message": f"Đã insert {len(rows)} dòng vào '{table}'",
        "inserted_count": len(rows),
        "preview": rows[:2],
        "excel_report": report_file
    }