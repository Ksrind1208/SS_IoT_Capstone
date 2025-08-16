import sqlite3

# Kết nối tới file database
db_path = "sensor_data.db"  # thay bằng đường dẫn file DB của bạn
conn = sqlite3.connect(db_path)

# Để kết quả truy vấn trả về dạng dict
conn.row_factory = sqlite3.Row

# Tạo con trỏ và đọc dữ liệu
cursor = conn.cursor()
cursor.execute("SELECT * FROM events")

rows = cursor.fetchall()

# In ra toàn bộ dữ liệu
for row in rows:
    print(dict(row))

# Đóng kết nối
conn.close()
