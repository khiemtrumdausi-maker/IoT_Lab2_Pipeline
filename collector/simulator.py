"""Gia lap ESP32 de test pipeline khi chua co Wokwi, va sinh du lieu 'ban' (spike, mat goi)
de bieu dien tien xu ly.   python collector/simulator.py --n 600 --interval 1
"""
import argparse, json, math, random, sys, time
from pathlib import Path
import paho.mqtt.client as mqtt

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C

ap = argparse.ArgumentParser()
ap.add_argument("--n", type=int, default=600)
ap.add_argument("--interval", type=float, default=1.0, help="giay giua 2 ban tin")
ap.add_argument("--host", default=C.MQTT_HOST)
ap.add_argument("--port", type=int, default=C.MQTT_PORT)
ap.add_argument("--topic", default="iot/b23dccn435/lab2/esp32-sim/telemetry")
a = ap.parse_args()

c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="lab2-sim")
c.connect(a.host, a.port)
c.loop_start()
t0 = time.time()
seq = 0
for i in range(a.n):
    seq += 1
    if random.random() < 0.05:        # mo phong mat 1-3 goi (khong publish)
        seq += random.randint(1, 3)
    temp = 28 + 3 * math.sin(i / 40) + random.gauss(0, 0.2)
    hum = 65 + 8 * math.cos(i / 55) + random.gauss(0, 0.8)
    dist = 100 + 30 * math.sin(i / 25) + random.gauss(0, 0.5)
    if random.random() < 0.03:        # spike (van nam trong mien hop le -> toi buoc tien xu ly moi bat duoc)
        temp += random.choice([-1, 1]) * random.uniform(15, 25)
    if random.random() < 0.03:
        dist += random.choice([-1, 1]) * random.uniform(80, 150)
    msg = {"device_id": "esp32-sim", "ts": int(time.time() * 1000), "sequence": seq,
           "uptime_s": int(time.time() - t0), "temperature": round(temp, 2),
           "humidity": round(min(max(hum, 0), 100), 2), "distance_cm": round(min(max(dist, 2), 400), 2),
           "rssi": random.randint(-90, -60)}
    c.publish(a.topic, json.dumps(msg), qos=1)
    time.sleep(a.interval)
c.loop_stop()
print("done, last seq", seq)
