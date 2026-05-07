import json
import pandas as pd
from config import report_path


def save_generated_data_report(table, rows):
    path = report_path("du_lieu_sinh", table)

    df = pd.DataFrame(rows)
    df.to_excel(path, index=False)

    return path


def save_testcase_report(table, report):
    path = report_path("testcase", table)

    rows = []
    for r in report:
        row = {
            "Tên test case": r.get("Tên test case"),
            "Kết quả mong muốn": r.get("Kết quả mong muốn"),
            "Kết quả thực tế": r.get("Kết quả thực tế"),
            "Trạng thái": r.get("Trạng thái"),
        }

        data = r.get("Dữ liệu test") or {}
        for k, v in data.items():
            row[k] = v

        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_excel(path, index=False)

    return path