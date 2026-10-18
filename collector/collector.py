"""Subscribe MQTT -> validate -> ghi InfluxDB (bucket iot_raw).

Schema:  measurement = sensor_raw
         tags   = device_id
         fields = temperature, humidity, distance_cm, rssi, sequence, uptime_s,
                  gap (so goi mat ngay truoc do), latency_ms (recv - ts thiet bi), write_ms
         time   = ts cua thiet bi (NTP, ms)  -> ban tin gui lai cung ts se GHI DE (idempotent, khong trung)
"""
import json, logging, sys, time, threading
from collections import deque
from pathlib import Path

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("collector")

REQUIRED = ["device_id", "ts", "sequence", "temperature", "humidity", "distance_cm"]
last_seq: dict[str, int] = {}
stats = {"received": 0, "stored": 0, "rejected": 0, "lost": 0, "reboots": 0}
retry_buf: deque = deque(maxlen=5000)   # dem cuc bo khi InfluxDB tam thoi loi
lock = threading.Lock()

influx = InfluxDBClient(url=C.INFLUX_URL, token=C.INFLUX_TOKEN, org=C.INFLUX_ORG)
write_api = influx.write_api(write_options=SYNCHRONOUS)


def validate(msg: dict) -> str | None:
    """Tra ve None neu hop le, nguoc lai tra ve ly do loai."""
    for k in REQUIRED:
        if k not in msg:
            return f"thieu khoa {k}"
    for k, (lo, hi) in C.VALID_RANGE.items():
        v = msg[k]
        if not isinstance(v, (int, float)) or isinstance(v, bool) or v != v:
            return f"{k} khong phai so"
        if not lo <= v <= hi:
            return f"{k}={v} ngoai mien [{lo},{hi}]"
    if not isinstance(msg["ts"], (int, float)) or msg["ts"] < 1_600_000_000_000:
        return "ts khong hop le"
    return None


def flush():
    """Ghi cac point trong retry_buf; neu loi thi giu lai de thu lai sau."""
    while retry_buf:
        p = retry_buf[0]
        try:
            write_api.write(bucket=C.BUCKET_RAW, record=p)
            retry_buf.popleft()
            stats["stored"] += 1
        except Exception as e:  # noqa: BLE001
            log.warning("Ghi InfluxDB loi (%s) - con %d point cho", e, len(retry_buf))
            return


def on_message(client, userdata, m):
    recv_ms = int(time.time() * 1000)
    stats["received"] += 1
    try:
        msg = json.loads(m.payload.decode())
    except Exception:  # noqa: BLE001
        stats["rejected"] += 1
        log.warning("JSON hong: %r", m.payload[:80])
        return
    reason = validate(msg)
    if reason:
        stats["rejected"] += 1
        log.warning("LOAI ban tin (%s): %s", reason, msg)
        return

    dev, seq = str(msg["device_id"]), int(msg["sequence"])
    prev = last_seq.get(dev)
    gap = 0
    if prev is not None:
        if seq == prev:
            log.info("Ban tin trung seq=%d - bo qua", seq)
            return
        if seq > prev + 1:
            gap = seq - prev - 1
            stats["lost"] += gap
            log.warning("MAT %d goi (seq %d -> %d)", gap, prev, seq)
        elif seq < prev:
            stats["reboots"] += 1
            log.warning("Thiet bi khoi dong lai (seq %d -> %d)", prev, seq)
    last_seq[dev] = seq

    t0 = time.perf_counter()
    p = (Point(C.MEAS_RAW).tag("device_id", dev)
         .field("temperature", float(msg["temperature"]))
         .field("humidity", float(msg["humidity"]))
         .field("distance_cm", float(msg["distance_cm"]))
         .field("rssi", int(msg.get("rssi", 0)))
         .field("sequence", seq)
         .field("uptime_s", int(msg.get("uptime_s", 0)))
         .field("gap", gap)
         .field("latency_ms", float(recv_ms - msg["ts"]))
         .time(int(msg["ts"]), WritePrecision.MS))
    with lock:
        retry_buf.append(p)
        flush()
    # write_ms ghi nhan o log (do thoi gian ghi DB)
    log.info("seq=%d T=%.1f H=%.1f D=%.1f | e2e=%d ms | db=%.1f ms",
             seq, msg["temperature"], msg["humidity"], msg["distance_cm"],
             recv_ms - msg["ts"], (time.perf_counter() - t0) * 1000)


def on_connect(client, userdata, flags, rc, props=None):
    log.info("MQTT connected rc=%s, subscribe %s", rc, C.MQTT_TOPIC)
    client.subscribe(C.MQTT_TOPIC, qos=1)


def stats_loop():
    while True:
        time.sleep(30)
        log.info("STATS %s", stats)


if __name__ == "__main__":
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="lab2-collector-b23dccn435")
    c.on_connect, c.on_message = on_connect, on_message
    c.reconnect_delay_set(1, 30)           # tu dong noi lai
    c.connect(C.MQTT_HOST, C.MQTT_PORT, keepalive=30)
    threading.Thread(target=stats_loop, daemon=True).start()
    try:
        c.loop_forever()
    except KeyboardInterrupt:
        log.info("Dung. STATS %s", stats)
