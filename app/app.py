"""App giam sat (Streamlit): du lieu real-time, du lieu da xu ly, do tre, mat goi.
   streamlit run app/app.py
"""
import sys
from pathlib import Path
import pandas as pd
import streamlit as st
from influxdb_client import InfluxDBClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C

st.set_page_config(page_title="IoT Lab 2 - B23DCCN435", layout="wide")
st.title("Giám sát dữ liệu IoT – Bài thực hành 2")
rng = st.sidebar.selectbox("Khoảng thời gian", ["-5m", "-15m", "-1h", "-6h", "-24h"], index=1)
device = st.sidebar.text_input("device_id (để trống = tất cả)", "")


@st.cache_resource
def client():
    return InfluxDBClient(url=C.INFLUX_URL, token=C.INFLUX_TOKEN, org=C.INFLUX_ORG)


def query(bucket, meas, fields, start):
    f = "[" + ",".join(f'"{x}"' for x in fields) + "]"
    dev = f'|> filter(fn:(r) => r.device_id == "{device}")' if device else ""
    q = f'''from(bucket:"{bucket}") |> range(start:{start})
      |> filter(fn:(r) => r._measurement == "{meas}") {dev}
      |> filter(fn:(r) => contains(value: r._field, set: {f}))
      |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")'''
    df = client().query_api().query_data_frame(q)
    if isinstance(df, list):
        df = pd.concat(df) if df else pd.DataFrame()
    if df.empty:
        return df
    return df.set_index("_time")[[c for c in fields if c in df.columns]]


@st.fragment(run_every=5)
def live():
    raw = query(C.BUCKET_RAW, C.MEAS_RAW,
                C.SENSOR_COLS + ["latency_ms", "gap", "sequence", "rssi"], rng)
    if raw.empty:
        st.info("Chưa có dữ liệu – hãy chạy collector và Wokwi/simulator.")
        return
    last = raw.iloc[-1]
    c = st.columns(5)
    c[0].metric("Nhiệt độ (°C)", f"{last.temperature:.1f}")
    c[1].metric("Độ ẩm (%RH)", f"{last.humidity:.1f}")
    c[2].metric("Khoảng cách (cm)", f"{last.distance_cm:.1f}")
    c[3].metric("Độ trễ gần nhất (ms)", f"{last.latency_ms:.0f}")
    c[4].metric("Gói mất (cửa sổ)", f"{int(raw['gap'].sum())}")
    st.subheader("Dữ liệu thô (real-time)")
    st.line_chart(raw[C.SENSOR_COLS])
    st.subheader("Độ trễ end-to-end (ms)")
    st.line_chart(raw["latency_ms"])
    lat = raw["latency_ms"]
    st.caption(f"mean={lat.mean():.0f} ms · p50={lat.quantile(.5):.0f} · p95={lat.quantile(.95):.0f} · max={lat.max():.0f}")


live()

st.subheader("Dữ liệu đã tiền xử lý (resample + nội suy + loại outlier)")
proc = query(C.BUCKET_PROCESSED, C.MEAS_CLEAN,
             C.SENSOR_COLS + [f"{c}_rollmean" for c in C.SENSOR_COLS], rng)
if proc.empty:
    st.info("Chưa có dữ liệu đã xử lý – chạy preprocess/preprocess.py trước.")
else:
    st.line_chart(proc[C.SENSOR_COLS])
    st.line_chart(proc[[f"{c}_rollmean" for c in C.SENSOR_COLS]])
