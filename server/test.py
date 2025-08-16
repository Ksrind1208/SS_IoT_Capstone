import sqlite3
import pandas as pd

DB_FILE = "sensor_data.db"

# Kết nối DB
conn = sqlite3.connect(DB_FILE)

# Lấy danh sách bảng
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
tables = [t[0] for t in tables]

print("📋 Các bảng trong database:", tables)

# Đọc dữ liệu từng bảng
for table in tables:
    print(f"\n=== Nội dung bảng {table} ===")
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    print(df.to_string(index=False))

conn.close()
