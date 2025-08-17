from flask import Flask, render_template, jsonify, request, send_file
import sqlite3
import csv
import io
import paho.mqtt.publish as publish
from datetime import datetime, timedelta
import time
from dateutil.parser import isoparse

DB_FILE = "sensor_data.db"
MQTT_BROKER = "127.0.0.1"
MQTT_CONTROL_TOPIC = "coldchain/fridge1/control"
TEMP_LIMIT = 4.0

app = Flask(__name__)

def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def parse_datetime_safely(datetime_str):
    """Parse datetime string and return timezone-naive datetime"""
    if not datetime_str:
        return None
    try:
        # Parse with dateutil to handle various formats
        dt = isoparse(datetime_str)
        # Convert to naive datetime (remove timezone info)
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except:
        try:
            # Fallback to standard parsing
            return datetime.fromisoformat(datetime_str.replace('Z', '').replace('+00:00', ''))
        except:
            return None

def get_current_violation_status(threshold_minutes=15):
    """
    Kiểm tra xem hiện tại có violation đang diễn ra và kéo dài >= threshold không
    """
    conn = get_db()
    
    # Lấy 20 readings gần nhất để phân tích
    recent_readings = conn.execute("""
        SELECT * FROM fridge_readings 
        ORDER BY ts DESC 
        LIMIT 20
    """).fetchall()
    
    conn.close()
    
    if not recent_readings:
        return False
    
    # Sắp xếp theo thời gian tăng dần
    recent_readings = list(reversed(recent_readings))
    
    # Tìm violation period hiện tại (nếu có)
    violation_start = None
    for reading in reversed(recent_readings):  # Duyệt ngược từ mới nhất
        if reading["t_c"] > TEMP_LIMIT:
            if violation_start is None:
                violation_start = parse_datetime_safely(reading["ts"])
        else:
            # Nhiệt độ bình thường, không có violation hiện tại
            return False
    
    # Nếu có violation đang diễn ra
    if violation_start:
        now = datetime.now()
        violation_duration = (now - violation_start).total_seconds() / 60
        return violation_duration >= threshold_minutes
    
    return False

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/data")
def data():
    conn = get_db()
    readings = conn.execute("SELECT * FROM fridge_readings ORDER BY ts").fetchall()
    events = conn.execute("SELECT * FROM events ORDER BY started_at DESC").fetchall()
    conn.close()

    # Tính status dựa trên readings và events gần nhất
    status = "Compliant"
    
    try:
        # Kiểm tra violations trong events có kéo dài >= 1 phút không
        if events:
            recent_violations = [e for e in events if e["type"] == "temp_violation" and (e["duration_min"] or 0) >= 1]
            if recent_violations:
                # Kiểm tra có violation nào đang diễn ra không
                now = datetime.now()
                for event in recent_violations:
                    event_start = parse_datetime_safely(event["started_at"])
                    if not event_start:
                        continue
                        
                    # Nếu event chưa có end time hoặc mới kết thúc gần đây
                    if not event["ended_at"]:
                        status = "Warning"
                        break
                    else:
                        event_end = parse_datetime_safely(event["ended_at"])
                        if event_end:
                            # Nếu event kết thúc trong 5 phút gần đây, có thể vẫn đang có vấn đề
                            time_since_end = (now - event_end).total_seconds() / 60
                            if time_since_end <= 5:
                                status = "Warning"
                                break
        
        # Kiểm tra readings gần nhất
        if readings and status == "Compliant":
            recent_readings = readings[-10:]  # 10 readings gần nhất
            violation_count = sum(1 for r in recent_readings if r["t_c"] > TEMP_LIMIT)
            if violation_count >= 3:  # Nếu có 3+ readings vượt ngưỡng
                status = "Warning"
                
    except Exception as e:
        print(f"Error calculating status: {e}")
        status = "Compliant"  # Default fallback
    
    return jsonify({
        "readings": [dict(r) for r in readings],
        "events": [dict(e) for e in events],
        "status": status
    })

@app.route("/check_violations", methods=["GET"])
def check_violations():
    minutes = int(request.args.get("minutes", 15))
    
    try:
        # Gửi lệnh xử lý tới subscriber qua MQTT
        publish.single(MQTT_CONTROL_TOPIC, str(minutes), hostname=MQTT_BROKER)
        
        # Đợi một chút để subscriber xử lý
        time.sleep(2)
        
    except Exception as e:
        return jsonify({"status": "error", "message": f"MQTT error: {str(e)}"})

    # Lấy events từ DB để trả về cho UI
    try:
        conn = get_db()
        events = conn.execute("""
            SELECT * FROM events 
            WHERE duration_min >= ? AND type = 'temp_violation'
            ORDER BY started_at DESC
        """, (minutes,)).fetchall()
        conn.close()

        return jsonify({
            "status": "ok", 
            "events": [dict(e) for e in events],
            "threshold": minutes
        })
        
    except Exception as e:
        return jsonify({"status": "error", "message": f"Database error: {str(e)}"})

@app.route("/export_csv")
def export_csv():
    conn = get_db()
    events = conn.execute("SELECT * FROM events ORDER BY started_at DESC").fetchall()
    conn.close()

    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(["ID", "Device", "Type", "Start", "End", "Duration (min)"])
    for e in events:
        cw.writerow([
            e["id"], 
            e["device_id"], 
            e["type"], 
            e["started_at"], 
            e["ended_at"], 
            e["duration_min"]
        ])

    output = io.BytesIO()
    output.write(si.getvalue().encode("utf-8"))
    output.seek(0)

    return send_file(
        output, 
        mimetype="text/csv", 
        as_attachment=True, 
        download_name="coldwatch_events.csv"
    )

@app.route("/current_status")
def current_status():
    """
    API endpoint để kiểm tra status hiện tại với threshold cụ thể
    """
    threshold = int(request.args.get("threshold", 15))
    is_violation = get_current_violation_status(threshold)
    
    return jsonify({
        "status": "Warning" if is_violation else "Compliant",
        "threshold": threshold
    })

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)