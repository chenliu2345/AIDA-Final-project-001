import hashlib
from pathlib import Path
import re
import uuid

import pandas as pd
import pyodbc

from settings import CAT_FEATURES, DATABASE, resource_dir


def connect(server=".", database=DATABASE, autocommit=False):
    """通过 Windows 身份验证连接本机或指定 SQL Server，不在代码中保存密码。
    Connect using Windows authentication without storing passwords in code."""
    drivers = pyodbc.drivers()
    driver = next((name for name in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"] if name in drivers), None)
    if not driver:
        raise RuntimeError("训练需要 Microsoft ODBC Driver 17 或 18 for SQL Server。")
    escaped_server = str(server).replace("}", "}}")
    return pyodbc.connect(f"DRIVER={{{driver}}};SERVER={{{escaped_server}}};DATABASE={database};"
                          "Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes;",
                          timeout=10, autocommit=autocommit)


def initialize_database(server="."):
    """执行幂等 SQL 建库脚本，只创建本项目数据库和缺失对象，不删除已有数据库。
    Create only missing database objects with an idempotent schema; preserve existing databases."""
    sql = (resource_dir() / "schema.sql").read_text(encoding="utf-8-sig")
    connection = connect(server, "master", autocommit=True)
    try:
        for batch in re.split(r"(?im)^\s*GO\s*$", sql):
            if batch.strip():
                connection.cursor().execute(batch)
    finally:
        connection.close()


def import_batch(csv_path, clean, audit, server="."):
    """在单个事务内写入原始行、拒绝原因、类别维度和清洗记录；失败时整体回滚。
    Write raw rows, rejection reasons, dimensions and cleaned rows in one transaction, rolling back on failure."""
    batch_id = str(uuid.uuid4())
    file_hash = hashlib.sha256(Path(csv_path).read_bytes()).hexdigest()
    connection = connect(server)
    try:
        cursor = connection.cursor()
        cursor.execute("INSERT INTO dbo.Import_Batches(Batch_ID, Source_Name, Source_SHA256, Raw_Count, Accepted_Count) VALUES (?,?,?,?,?)",
                       batch_id, Path(csv_path).name, file_hash, len(audit), len(clean))
        cursor.fast_executemany = True
        cursor.executemany("INSERT INTO dbo.Raw_Rows(Batch_ID, Source_Row, Raw_JSON, Reject_Reason) VALUES (?,?,?,?)",
                           [(batch_id, x["Source_Row"], x["Raw_JSON"], x["Reject_Reason"] or None) for x in audit])
        mappings = {}
        for column in CAT_FEATURES:
            table = "Dim_" + column
            existing = {row[1]: row[0] for row in cursor.execute(f"SELECT ID, Value FROM dbo.[{table}]").fetchall()}
            for value in sorted(set(clean[column]) - set(existing)):
                cursor.execute(f"INSERT INTO dbo.[{table}](Value) OUTPUT INSERTED.ID VALUES (?)", value)
                existing[value] = cursor.fetchone()[0]
            mappings[column] = existing
        columns = ["Batch_ID", "Source_Row", "Listing_Key", "Group_Key", "Year", "Kilometres", "Price_CAD", "Status", "Scrape_Date", "Sold_Date", "Listing_Title"] + [c + "_ID" for c in CAT_FEATURES]
        rows = []
        for item in clean.to_dict("records"):
            rows.append(tuple([batch_id, item["Source_Row"], item["Listing_Key"], item["Group_Key"], item["Year"], item["Kilometres"], item["Price_CAD"], item["Status"],
                               item["Scrape_Date"] if pd.notna(item["Scrape_Date"]) else None,
                               item["Sold_Date"] if pd.notna(item["Sold_Date"]) else None, item["Listing_Title"]]
                              + [mappings[c][item[c]] for c in CAT_FEATURES]))
        cursor.executemany("INSERT INTO dbo.Listings_Final_ETL (" + ",".join(f"[{c}]" for c in columns) + ") VALUES (" + ",".join("?" for _ in columns) + ")", rows)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return batch_id


def read_training_batch(batch_id, server="."):
    """与最终 Notebook 一致只读取当前批次 SOLD 记录，避免活动或重复上架记录进入价格模型。
    Read only current-batch SOLD records, excluding active and relisted records from training."""
    connection = connect(server)
    try:
        cursor = connection.cursor().execute("SELECT * FROM dbo.V_Training WHERE Batch_ID = ? AND Status = 'SOLD'", batch_id)
        return pd.DataFrame.from_records([tuple(row) for row in cursor.fetchall()], columns=[c[0] for c in cursor.description])
    finally:
        connection.close()


def save_distances(distances, server="."):
    """将真实 ORS 距离写入城市维度，两项距离与原版 tbl_Locations 含义一致。
    Save real ORS distances to the city dimension with the original tbl_Locations semantics."""
    connection = connect(server)
    try:
        cursor = connection.cursor()
        for city, (edmonton, calgary) in distances.items():
            cursor.execute("UPDATE dbo.Dim_City_Name SET Distance_from_Edmonton_KM=?, Distance_from_Calgary_KM=? WHERE Value=?", edmonton, calgary, city)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
