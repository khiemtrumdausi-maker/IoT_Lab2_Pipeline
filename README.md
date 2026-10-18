# Bài thực hành 2 – Thu thập, lưu trữ, tiền xử lý dữ liệu IoT (B23DCCN435)

Kiến trúc: ESP32 (Wokwi) → MQTT broker công cộng (HiveMQ) → collector.py → InfluxDB (iot_raw) → preprocess.py → InfluxDB (iot_processed) → Grafana / Streamlit

## Chạy
1. `docker compose up -d influxdb influx-init grafana`   (InfluxDB :8086, Grafana :3000; token `lab2-dev-token`)
2. `pip install -r requirements.txt`
3. Wokwi: tạo project ESP32, dán `firmware/` (sketch.ino, diagram.json, libraries.txt), đổi DEVICE_ID/topic theo MSSV, bấm Run.
4. `python collector/collector.py`          (không có Wokwi: `python collector/simulator.py` ở terminal khác)
5. Đợi ≥10 phút dữ liệu, rồi: `python preprocess/preprocess.py --start -1h --window 1min --method iqr --device <device_id>`
6. `streamlit run app/app.py`
7. Grafana: add data source InfluxDB (Flux, org ptit, token lab2-dev-token), tạo panel từ bucket iot_raw / iot_processed.

Lưu ý: Wokwi không truy cập được broker localhost → dùng broker công cộng và topic có MSSV để tránh trùng người khác.
