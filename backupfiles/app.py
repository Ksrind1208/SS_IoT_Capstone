from flask import Flask, render_template, jsonify, send_file, request
import sqlite3
import csv
import io
from datetime import datetime
from dateutil.parser import isoparse

TEMP_LIMIT = 4.0

app = Flask(__name__)
DB_FILE = "sensor_data.db"
TEMP_LIMIT = 4.0

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

@app.route("/check_violations")
def check_violations():
    minutes = int(request.args.get("minutes", 15))
    conn = get_db()

    # Xoá các event cũ có duration < minutes
    conn.execute("DELETE FROM events WHERE duration_min < ?", (minutes,))
    conn.commit()

    readings = conn.execute("SELECT * FROM fridge_readings ORDER BY ts").fetchall()

    count = 0
    start = None
    device_id = None

    for r in readings:
        t_c = r["t_c"]
        ts = isoparse(r["ts"])

        if t_c > TEMP_LIMIT:
            if start is None:
                start = ts
                device_id = r["device_id"]
            duration = (ts - start).total_seconds() / 60
            if duration >= minutes:
                exists = conn.execute(
                    "SELECT 1 FROM events WHERE device_id=? AND started_at=?",
                    (device_id, start.isoformat())
                ).fetchone()
                if not exists:
                    conn.execute("""
                        INSERT INTO events (device_id, type, started_at, ended_at, duration_min)
                        VALUES (?, ?, ?, ?, ?)
                    """, (device_id, "temp_violation",
                          start.isoformat(), ts.isoformat(), duration))
                    conn.commit()
                    count += 1
                start = None
        else:
            start = None

    # Chỉ trả về log >= minutes
    events = conn.execute("SELECT * FROM events WHERE duration_min >= ? ORDER BY started_at DESC", (minutes,)).fetchall()
    conn.close()

    return jsonify({
        "count": count,
        "events": [dict(e) for e in events]
    })

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
