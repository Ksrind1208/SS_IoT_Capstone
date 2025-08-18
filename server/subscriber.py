import json
import sqlite3
from datetime import datetime
from dateutil.parser import isoparse
import paho.mqtt.client as mqtt
import time

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

def normalize_datetime(dt):
    """Convert datetime to string in consistent format without timezone"""
    if isinstance(dt, str):
        dt = isoparse(dt)
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt.isoformat()

def parse_datetime_safely(datetime_str):
    """Parse datetime string and return timezone-naive datetime"""
    if not datetime_str:
        return None
    try:
        dt = isoparse(datetime_str)
        #Chuyển đổi 
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except:
        try:
            return datetime.fromisoformat(datetime_str.replace('Z', '').replace('+00:00', ''))
        except:
            return None

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

# Tạo bảng nếu chưa tồn tại
def init_database():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fridge_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            t_c REAL NOT NULL,
            ts TEXT NOT NULL
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            type TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            duration_min REAL
        )
    """)
    
    conn.commit()
    print("[subscriber.py] Database tables initialized")

# Kiểm tra event đã tồn tại chưa
def event_exists(device_id, start_time, event_type="temp_violation"):
    cursor.execute("""
        SELECT 1 FROM events 
        WHERE device_id=? AND started_at=? AND type=?
    """, (device_id, start_time.isoformat(), event_type))
    return cursor.fetchone() is not None

# Hàm xử lý violations từ fridge_readings
def process_violations(minutes):
    print(f"[subscriber.py] Processing violations for {minutes} minutes...")
    start_time = time.time()
    
    conn_local = sqlite3.connect(DB_FILE)
    conn_local.row_factory = sqlite3.Row
    cursor_local = conn_local.cursor()
    
    try:
        # Lấy tất cả readings theo thứ tự thời gian
        readings = cursor_local.execute("""
            SELECT * FROM fridge_readings 
            ORDER BY ts
        """).fetchall()
        
        if not readings:
            print("[subscriber.py] No readings found")
            conn_local.close()
            return 0
        
        print(f"[subscriber.py] Analyzing {len(readings)} readings...")
        
        violation_periods = []
        current_violation = None
        
        # Bước 1: Tìm tất cả violation periods
        for reading in readings:
            t_c = reading["t_c"]
            ts_raw = reading["ts"]
            device_id = reading["device_id"]
            
            ts = parse_datetime_safely(ts_raw)
            if not ts:
                print(f"[subscriber.py] Warning: Could not parse timestamp {ts_raw}")
                continue
            
            if t_c > TEMP_LIMIT:
                # Bắt đầu hoặc tiếp tục violation period
                if current_violation is None:
                    current_violation = {
                        "device_id": device_id,
                        "start": ts,
                        "end": ts
                    }
                else:
                    # Cập nhật end time
                    current_violation["end"] = ts
            else:
                # Nhiệt độ bình thường - kết thúc violation period (nếu có)
                if current_violation is not None:
                    violation_periods.append(current_violation)
                    current_violation = None
        
        # Nếu violation kéo dài đến cuối data
        if current_violation is not None:
            violation_periods.append(current_violation)
        
        print(f"[subscriber.py] Found {len(violation_periods)} violation periods")
        
        # Bước 2: Xử lý từng violation period
        new_events = 0
        updated_events = 0
        
        for i, violation in enumerate(violation_periods):
            duration = (violation["end"] - violation["start"]).total_seconds() / 60
            
            print(f"[subscriber.py] Violation {i+1}: {duration:.2f} minutes "
                  f"({violation['start']} to {violation['end']})")
            
            # Chỉ tạo event nếu duration >= minutes yêu cầu
            if duration >= minutes:
                device_id = violation["device_id"]
                start_time = normalize_datetime(violation["start"])
                end_time = normalize_datetime(violation["end"])
                
                # Kiểm tra event đã tồn tại chưa
                existing = cursor_local.execute("""
                    SELECT id, ended_at, duration_min FROM events 
                    WHERE device_id=? AND type='temp_violation' 
                    AND started_at=?
                """, (device_id, start_time)).fetchone()
                
                if not existing:
                    # Tạo event mới
                    cursor_local.execute("""
                        INSERT INTO events (device_id, type, started_at, ended_at, duration_min)
                        VALUES (?, ?, ?, ?, ?)
                    """, (device_id, "temp_violation", start_time, end_time, round(duration, 2)))
                    new_events += 1
                    print(f"[subscriber.py] ✓ Added new violation: duration {duration:.2f} min")
                else:
                    # Cập nhật event hiện có nếu cần
                    if (existing["ended_at"] != end_time or 
                        abs((existing["duration_min"] or 0) - duration) > 0.1):
                        cursor_local.execute("""
                            UPDATE events 
                            SET ended_at=?, duration_min=?
                            WHERE device_id=? AND type='temp_violation' AND started_at=?
                        """, (end_time, round(duration, 2), device_id, start_time))
                        updated_events += 1
                        print(f"[subscriber.py] ↻ Updated existing violation: duration {duration:.2f} min")
                    else:
                        print(f"[subscriber.py] - No change needed for existing violation")
            else:
                print(f"[subscriber.py] ✗ Skipped (duration {duration:.2f} < {minutes} min)")
        
        # Commit tất cả changes
        conn_local.commit()
        
        processing_time = time.time() - start_time
        print(f"[subscriber.py] Processing completed in {processing_time:.2f}s: "
              f"{new_events} new events, {updated_events} updated events")
        
        return new_events + updated_events
        
    except Exception as e:
        print(f"[subscriber.py] Error processing violations: {e}")
        conn_local.rollback()
        return 0
        
    finally:
        conn_local.close()

# Xử lý message MQTT
def on_message(client, userdata, msg):
    global current_event
    
    try:
        topic = msg.topic
        print(f"[subscriber.py] Received message on {topic}")

        if topic == MQTT_TELEMETRY_TOPIC:
            payload = json.loads(msg.payload.decode())
            device_id = payload["device_id"]
            t_c = payload["t_c"]
            ts = payload["ts"]

            print(f"[subscriber.py] Telemetry: {device_id} = {t_c}°C at {ts}")

            # Lưu dữ liệu sensor vào DB
            cursor.execute("""
                INSERT INTO fridge_readings (device_id, t_c, ts) 
                VALUES (?, ?, ?)
            """, (device_id, t_c, ts))
            conn.commit()

            current_time = parse_datetime_safely(ts)
            if not current_time:
                print(f"[subscriber.py] Warning: Could not parse timestamp {ts}")
                return
            
            if t_c > TEMP_LIMIT:
                if current_event is None:
                    # Bắt đầu violation mới
                    current_event = {
                        "device_id": device_id,
                        "start": current_time,
                        "last_violation": current_time
                    }
                    print(f"[subscriber.py] Started tracking violation at {ts}")
                else:
                    # Cập nhật thời gian violation gần nhất
                    current_event["last_violation"] = current_time
                    
                    # Kiểm tra nếu violation đã kéo dài >= 1 phút
                    duration = (current_time - current_event["start"]).total_seconds() / 60
                    if duration >= 1.0:
                        start_time_str = normalize_datetime(current_event["start"])
                        end_time_str = normalize_datetime(current_time)
                        
                        # Kiểm tra xem đã tạo event cho violation này chưa
                        existing = cursor.execute("""
                            SELECT id FROM events 
                            WHERE device_id=? AND type='temp_violation' 
                            AND started_at=?
                        """, (device_id, start_time_str)).fetchone()
                        
                        if not existing:
                            # Tạo event mới
                            cursor.execute("""
                                INSERT INTO events (device_id, type, started_at, ended_at, duration_min)
                                VALUES (?, ?, ?, ?, ?)
                            """, (device_id, "temp_violation", 
                                 start_time_str, end_time_str, round(duration, 2)))
                            conn.commit()
                            print(f"[subscriber.py] Created realtime event: {duration:.2f} minutes")
                        else:
                            # Cập nhật event hiện có
                            cursor.execute("""
                                UPDATE events 
                                SET ended_at=?, duration_min=?
                                WHERE device_id=? AND type='temp_violation' AND started_at=?
                            """, (end_time_str, round(duration, 2), device_id, start_time_str))
                            conn.commit()
                            print(f"[subscriber.py] Updated realtime event: {duration:.2f} minutes")
            else:
                # Nhiệt độ bình thường
                if current_event is not None:
                    # Kết thúc violation - cập nhật event cuối cùng nếu có
                    duration = (current_event["last_violation"] - current_event["start"]).total_seconds() / 60
                    if duration >= 1.0:
                        start_time_str = normalize_datetime(current_event["start"])
                        end_time_str = normalize_datetime(current_event["last_violation"])
                        
                        cursor.execute("""
                            UPDATE events 
                            SET ended_at=?, duration_min=?
                            WHERE device_id=? AND type='temp_violation' AND started_at=?
                        """, (end_time_str, round(duration, 2), device_id, start_time_str))
                        conn.commit()
                        print(f"[subscriber.py] Finished violation: {duration:.2f} minutes")
                    
                    current_event = None
            
        elif topic == MQTT_CONTROL_TOPIC:
            # Nhận lệnh từ Flask app để process violations
            minutes = int(msg.payload.decode())
            print(f"[subscriber.py] Control command: analyze {minutes} minutes")
            
            result = process_violations(minutes)
            print(f"[subscriber.py] Analysis result: {result} events processed")
            
    except Exception as e:
        print(f"[subscriber.py] Error handling message: {e}")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[subscriber.py] Connected to MQTT broker successfully")
        # Subscribe to topics
        client.subscribe(MQTT_TELEMETRY_TOPIC)
        client.subscribe(MQTT_CONTROL_TOPIC)
        print(f"[subscriber.py] Subscribed to {MQTT_TELEMETRY_TOPIC} and {MQTT_CONTROL_TOPIC}")
    else:
        print(f"[subscriber.py] Failed to connect to MQTT broker: {rc}")

def on_disconnect(client, userdata, rc):
    print(f"[subscriber.py] Disconnected from MQTT broker: {rc}")

if __name__ == "__main__":
    # Khởi tạo
    init_database()
    
    # Setup MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    
    try:
        print(f"[subscriber.py] Connecting to MQTT broker at {MQTT_BROKER}:1883...")
        client.connect(MQTT_BROKER, 1883, 60)
        
        print("[subscriber.py] Starting MQTT loop...")
        client.loop_forever()
        
    except KeyboardInterrupt:
        print("\n[subscriber.py] Shutting down...")
        client.disconnect()
        conn.close()
    except Exception as e:
        print(f"[subscriber.py] Fatal error: {e}")
        conn.close()