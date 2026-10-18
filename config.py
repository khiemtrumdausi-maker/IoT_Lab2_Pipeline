"""Cau hinh chung - doc tu bien moi truong, co gia tri mac dinh khop docker-compose.yml."""
import os

MQTT_HOST = os.getenv("MQTT_HOST", "broker.hivemq.com")   # phai TRUNG voi firmware
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "iot/b23dccn435/lab2/+/telemetry")

INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = "nOn5FzYNaEDleDeb0Pdtp9p2UH87C_cqvyJTafOJxo9KEgjAvLHO9blF4r25vSrWNnYJrGHelEGj5jhzyfWqgA=="
INFLUX_ORG = os.getenv("INFLUX_ORG", "ptit")
BUCKET_RAW = os.getenv("BUCKET_RAW", "iot_raw")
BUCKET_PROCESSED = os.getenv("BUCKET_PROCESSED", "iot_processed")

MEAS_RAW = "sensor_raw"
MEAS_CLEAN = "sensor_clean"
SENSOR_COLS = ["temperature", "humidity", "distance_cm"]

# Mien gia tri hop le (kiem tra ngay luc thu thap)
VALID_RANGE = {
    "temperature": (-40.0, 80.0),    # DHT22
    "humidity": (0.0, 100.0),
    "distance_cm": (2.0, 400.0),     # HC-SR04
}
