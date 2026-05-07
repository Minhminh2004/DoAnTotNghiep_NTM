import os
from datetime import datetime

REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)

def report_path(prefix, table):
    time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(REPORT_DIR, f"{prefix}_{table}_{time_str}.xlsx")