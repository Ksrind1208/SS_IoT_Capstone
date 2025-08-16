import json
import sqlite3
from datetime import datetime
import paho.mqtt.client as mqtt

DB_FILE = "sensor_data.db"
MQTT_BROKER = "127.0.0.1"
MQTT_TOPIC = "coldchain/fridge1/telemetry"
TEMP_LIMIT = 4.0
TIME_LIMIT_MIN = 15

conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()
current_event = None

def event_exists(device_id, start_time):
    cursor.execute("SELECT 1 FROM events WHERE device_id=? AND started_at=?", (device_id, start_time.isoformat()))
    return cursor.fetchone() is not None

def on_message(client, userdata, msg):
    global current_event
    payload = json.loads(msg.payload.decode())
    device_id = payload["device_id"]
    t_c = payload["t_c"]
    ts = payload["ts"]

    cursor.execute("INSERT INTO fridge_readings (device_id, t_c, ts) VALUES (?, ?, ?)",
                   (device_id, t_c, ts))
    conn.commit()

    if t_c > TEMP_LIMIT:
        if not current_event:
            current_event = {
                "device_id": device_id,
                "start": datetime.fromisoformat(ts)
            }
        else:
            duration = (datetime.fromisoformat(ts) - current_event["start"]).total_seconds() / 60
            if duration >= TIME_LIMIT_MIN and not event_exists(device_id, current_event["start"]):
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

client = mqtt.Client()
client.on_message = on_message
client.connect(MQTT_BROKER, 1883, 60)
client.subscribe(MQTT_TOPIC)
print(f"Subscribed to {MQTT_TOPIC}")
client.loop_forever()
