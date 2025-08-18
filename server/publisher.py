import paho.mqtt.client as mqtt
import json
import random
from datetime import datetime, timezone, timedelta
import time

# ==== MQTT config ====
MQTT_BROKER = "192.168.1.44"
MQTT_PORT = 1883
MQTT_TOPIC = "coldchain/fridge1/telemetry"

# Hàm lấy timestamp ISO8601 UTC+7
def get_iso8601_time():
    tz = timezone(timedelta(hours=7))  # UTC+7
    now = datetime.now(tz)
    return now.strftime("%Y-%m-%dT%H:%M:%S+07:00")

# Hàm publish dữ liệu
def publish_loop():
    client = mqtt.Client()
    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    while True:
        # random nhiệt độ từ 0.0 đến 10.0
        tempC = round(random.uniform(5.0, 10.0), 1)

        payload = {
            "device_id": "fridge1",
            "t_c": tempC,
            "ts": get_iso8601_time()
        }

        payload_str = json.dumps(payload)
        print("Gửi MQTT:", payload_str)

        client.publish(MQTT_TOPIC, payload_str)
        time.sleep(2)  # gửi mỗi 2 giây

if __name__ == "__main__":
    publish_loop()
