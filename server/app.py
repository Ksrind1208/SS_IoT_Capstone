from flask import Flask, render_template, jsonify, request, send_file
import sqlite3
import csv
import io
import paho.mqtt.publish as publish

DB_FILE = "sensor_data.db"
MQTT_BROKER = "127.0.0.1"
MQTT_CONTROL_TOPIC = "coldchain/fridge1/control"
TEMP_LIMIT = 4.0

app = Flask(__name__)

def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/data")
def data():
    conn = get_db()
    readings = conn.execute("SELECT * FROM fridge_readings ORDER BY ts").fetchall()
    events = conn.execute("SELECT * FROM events ORDER BY started_at DESC").fetchall()
    conn.close()

    status = "Compliant"
    if readings and readings[-1]["t_c"] > TEMP_LIMIT:
        status = "Warning"

    return jsonify({
        "readings": [dict(r) for r in readings],
        "events": [dict(e) for e in events],
        "status": status
    })

@app.route("/check_violations", methods=["GET"])
def check_violations():
    minutes = int(request.args.get("minutes", 15))
    try:
        print(str(minutes))
        # Publish số phút tới subscribe.py qua MQTT
        publish.single(MQTT_CONTROL_TOPIC, str(minutes), hostname=MQTT_BROKER)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

    # Fetch events từ DB để UI hiển thị
    conn = get_db()
    events = conn.execute("SELECT * FROM events WHERE duration_min >= ? ORDER BY started_at DESC", (minutes,)).fetchall()
    conn.close()

    return jsonify({"status": "ok", "events": [dict(e) for e in events]})

@app.route("/export_csv")
def export_csv():
    conn = get_db()
    events = conn.execute("SELECT * FROM events ORDER BY started_at DESC").fetchall()
    conn.close()

    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(["ID", "Device", "Type", "Start", "End", "Duration (min)"])
    for e in events:
        cw.writerow([e["id"], e["device_id"], e["type"], e["started_at"], e["ended_at"], e["duration_min"]])

    output = io.BytesIO()
    output.write(si.getvalue().encode("utf-8"))
    output.seek(0)

    return send_file(output, mimetype="text/csv", as_attachment=True, download_name="events.csv")

if __name__ == "__main__":
    app.run(debug=True)
