import json
import sqlite3
from datetime import datetime
from dateutil.parser import isoparse
import paho.mqtt.client as mqtt

# Cấu hình
DB_FILE = "sensor_data.db"
TEMP_LIMIT = 4.0
MQTT_BROKER = "127.0.0.1"
MQTT_TELEMETRY_TOPIC = "coldchain/fridge1/telemetry"
MQTT_CONTROL_TOPIC = "coldchain/fridge1/control"

# Kết nối DB
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()
current_event = None

# Kiểm tra event đã tồn tại chưa
def event_exists(device_id, start_time):
    cursor.execute(
        "SELECT 1 FROM events WHERE device_id=? AND started_at=?",
        (device_id, start_time.isoformat())
    )
    return cursor.fetchone() is not None

# Hàm xử lý violations từ fridge_readings
def process_violations(minutes):
    print(f"[subscribe.py] Processing violations for {minutes} minutes...")
    
    conn_local = sqlite3.connect(DB_FILE)
    conn_local.row_factory = sqlite3.Row
    cursor_local = conn_local.cursor()
    
    readings = cursor_local.execute("SELECT * FROM fridge_readings ORDER BY ts").fetchall()
    
    count = 0
    violation_periods = []  # Lưu tất cả violation periods
    current_violation = None
    
    # Bước 1: Tìm tất cả violation periods
    for i, reading in enumerate(readings):
        t_c = reading["t_c"]
        ts = isoparse(reading["ts"])
        current_device = reading["device_id"]
        
        if t_c > TEMP_LIMIT:
            # Bắt đầu violation period mới
            if current_violation is None:
                current_violation = {
                    "device_id": current_device,
                    "start": ts,
                    "end": ts
                }
            else:
                # Cập nhật end time của violation hiện tại
                current_violation["end"] = ts
        else:
            # Nhiệt độ bình thường - kết thúc violation period (nếu có)
            if current_violation is not None:
                violation_periods.append(current_violation)
                current_violation = None
    
    # Nếu violation kéo dài đến cuối data
    if current_violation is not None:
        violation_periods.append(current_violation)
    
    print(f"[subscribe.py] Found {len(violation_periods)} violation periods")
    
    # Bước 2: Xử lý từng violation period
    for violation in violation_periods:
        duration = (violation["end"] - violation["start"]).total_seconds() / 60
        
        # Chỉ tạo event nếu duration >= minutes yêu cầu
        if duration >= minutes:
            device_id = violation["device_id"]
            start_time = violation["start"].isoformat()
            end_time = violation["end"].isoformat()
            
            # Kiểm tra xem event này đã tồn tại chưa (cùng start time và device)
            existing = cursor_local.execute("""
                SELECT 1 FROM events 
                WHERE device_id=? AND type='temp_violation' 
                AND started_at=?
            """, (device_id, start_time)).fetchone()
            
            if not existing:
                cursor_local.execute("""
                    INSERT INTO events (device_id, type, started_at, ended_at, duration_min)
                    VALUES (?, ?, ?, ?, ?)
                """, (device_id, "temp_violation", start_time, end_time, round(duration, 2)))
                conn_local.commit()
                count += 1
                print(f"[subscribe.py] Added violation: {start_time} to {end_time}, duration: {duration:.2f} min")
            else:
                # Nếu đã tồn tại, cập nhật end_time và duration nếu cần
                cursor_local.execute("""
                    UPDATE events 
                    SET ended_at=?, duration_min=?
                    WHERE device_id=? AND type='temp_violation' AND started_at=?
                """, (end_time, round(duration, 2), device_id, start_time))
                conn_local.commit()
                print(f"[subscribe.py] Updated existing violation: {start_time}, new duration: {duration:.2f} min")
        else:
            print(f"[subscribe.py] Skipped violation (duration {duration:.2f} min < {minutes} min): {violation['start']} to {violation['end']}")
    
    conn_local.close()
    print(f"[subscribe.py] Completed processing: {count} new events added")
    return count

# Xử lý message MQTT
def on_message(client, userdata, msg):
    global current_event
    topic = msg.topic

    if topic == MQTT_TELEMETRY_TOPIC:
        payload = json.loads(msg.payload.decode())
        device_id = payload["device_id"]
        t_c = payload["t_c"]
        ts = payload["ts"]

        # Lưu dữ liệu sensor vào DB
        cursor.execute(
            "INSERT INTO fridge_readings (device_id, t_c, ts) VALUES (?, ?, ?)",
            (device_id, t_c, ts)
        )
        conn.commit()

        # Tạo event realtime nếu nhiệt độ vượt TEMP_LIMIT
        if t_c > TEMP_LIMIT:
            if not current_event:
                current_event = {"device_id": device_id, "start": datetime.fromisoformat(ts)}
            else:
                duration = (datetime.fromisoformat(ts) - current_event["start"]).total_seconds() / 60
                if duration >= 15 and not event_exists(device_id, current_event["start"]):
                    cursor.execute("""
                        INSERT INTO events (device_id, type, started_at, ended_at, duration_min)
                        VALUES (?, ?, ?, ?, ?)
                    """, (device_id, "temp_violation",
                          current_event["start"].isoformat(),
                          ts, duration))
                    conn.commit()
                    current_event = None
        else:
            current_event = None

    elif topic == MQTT_CONTROL_TOPIC:
        # Nhận lệnh từ app.py để process violations
        minutes = int(msg.payload.decode())
        print(f"[subscribe.py] Received control command: process {minutes} minutes")
        process_violations(minutes)

# Kết nối MQTT
client = mqtt.Client()
client.on_message = on_message
client.connect(MQTT_BROKER, 1883, 60)

# Subscribe 2 topic
client.subscribe(MQTT_TELEMETRY_TOPIC)
client.subscribe(MQTT_CONTROL_TOPIC)

print(f"[subscribe.py] Subscribed to {MQTT_TELEMETRY_TOPIC} and {MQTT_CONTROL_TOPIC}")
client.loop_forever()