"""Tien xu ly: doc iot_raw -> lam sach -> phat hien outlier -> resample -> dac trung -> chuan hoa -> ghi iot_processed.

  python preprocess/preprocess.py --start -1h --window 1min --method iqr
  python preprocess/preprocess.py --csv data.csv      # che do offline de test, khong can InfluxDB
"""
import argparse, sys
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config as C


# ---------- cac buoc xu ly (thuan pandas, de kiem thu) ----------
def dedup_sort(df: pd.DataFrame) -> pd.DataFrame:
    """Bo ban ghi trung chi muc thoi gian, sap xep tang dan."""
    return df[~df.index.duplicated(keep="last")].sort_index()


def detect_outliers(s: pd.Series, method="iqr", k=1.5, z=3.0) -> pd.Series:
    """Tra ve mask True tai diem bat thuong."""
    if method == "zscore":
        sd = s.std()
        return pd.Series(False, index=s.index) if not sd else ((s - s.mean()).abs() / sd > z)
    q1, q3 = s.quantile(0.25), s.quantile(0.75)
    iqr = q3 - q1
    return (s < q1 - k * iqr) | (s > q3 + k * iqr)


def clean(df: pd.DataFrame, window="1min", method="iqr", roll=5):
    """df: index la DatetimeIndex (UTC), cot gom C.SENSOR_COLS. Tra ve (df_clean, bao_cao)."""
    df = dedup_sort(df)
    report = {"rows_in": len(df)}

    # 1) outlier -> NaN (khong xoa hang de giu truc thoi gian)
    out = df[C.SENSOR_COLS].copy()
    for col in C.SENSOR_COLS:
        mask = detect_outliers(out[col], method)
        report[f"outliers_{col}"] = int(mask.sum())
        out.loc[mask, col] = np.nan

    # 2) resample theo cua so thoi gian (trung binh) -> o trong = missing
    rs = out.resample(window).mean()
    report["missing_bins_before"] = int(rs[C.SENSOR_COLS[0]].isna().sum())

    # 3) xu ly missing: noi suy theo thoi gian, toi da 5 o lien tiep; con lai ffill/bfill gioi han
    rs = rs.interpolate(method="time", limit=5, limit_direction="both")
    report["missing_bins_after"] = int(rs[C.SENSOR_COLS[0]].isna().sum())

    # 4) dac trung
    for col in C.SENSOR_COLS:
        rs[f"{col}_rollmean"] = rs[col].rolling(roll, min_periods=1).mean()
        rs[f"{col}_delta"] = rs[col].diff().fillna(0.0)

    # 5) chuan hoa (min-max va z-score) tren cot da lam sach
    for col in C.SENSOR_COLS:
        mn, mx, mu, sd = rs[col].min(), rs[col].max(), rs[col].mean(), rs[col].std()
        rs[f"{col}_minmax"] = (rs[col] - mn) / (mx - mn) if mx > mn else 0.0
        rs[f"{col}_z"] = (rs[col] - mu) / sd if sd else 0.0

    report["rows_out"] = len(rs)
    return rs.dropna(subset=C.SENSOR_COLS), report


# ---------- vao/ra InfluxDB ----------
def read_influx(start: str, device: str | None) -> pd.DataFrame:
    from influxdb_client import InfluxDBClient
    fields = "[" + ",".join(f'"{c}"' for c in C.SENSOR_COLS) + "]"
    dev_filter = f'|> filter(fn:(r) => r.device_id == "{device}")' if device else ""
    q = f'''from(bucket:"{C.BUCKET_RAW}") |> range(start: {start})
      |> filter(fn:(r) => r._measurement == "{C.MEAS_RAW}")
      |> filter(fn:(r) => contains(value: r._field, set: {fields}))
      {dev_filter}
      |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")'''
    with InfluxDBClient(url=C.INFLUX_URL, token=C.INFLUX_TOKEN, org=C.INFLUX_ORG) as cl:
        df = cl.query_api().query_data_frame(q)
    if isinstance(df, list):
        df = pd.concat(df) if df else pd.DataFrame()
    if df.empty:
        return df
    df["_time"] = pd.to_datetime(df["_time"], utc=True)
    return df.set_index("_time")[C.SENSOR_COLS]


def write_influx(df: pd.DataFrame, device: str):
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
    pts = []
    for ts, row in df.iterrows():
        p = Point(C.MEAS_CLEAN).tag("device_id", device).time(ts.to_pydatetime(), WritePrecision.S)
        for k, v in row.items():
            if pd.notna(v):
                p.field(k, float(v))
        pts.append(p)
    with InfluxDBClient(url=C.INFLUX_URL, token=C.INFLUX_TOKEN, org=C.INFLUX_ORG) as cl:
        cl.write_api(write_options=SYNCHRONOUS).write(bucket=C.BUCKET_PROCESSED, record=pts)
    print(f"Da ghi {len(pts)} diem vao {C.BUCKET_PROCESSED}/{C.MEAS_CLEAN}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="-1h")
    ap.add_argument("--window", default="1min")
    ap.add_argument("--method", choices=["iqr", "zscore"], default="iqr")
    ap.add_argument("--device", default=None)
    ap.add_argument("--csv", default=None, help="doc CSV (cot _time + 3 cot cam bien) thay vi InfluxDB")
    a = ap.parse_args()

    if a.csv:
        raw = pd.read_csv(a.csv, parse_dates=["_time"]).set_index("_time")
        raw.index = pd.to_datetime(raw.index, utc=True)
    else:
        raw = read_influx(a.start, a.device)
    if raw.empty:
        sys.exit("Khong co du lieu trong khoang thoi gian da chon.")
    res, rep = clean(raw, a.window, a.method)
    print("BAO CAO:", rep)
    print(res.head())
    if not a.csv:
        write_influx(res, a.device or "all")
    else:
        res.to_csv("processed_out.csv")
        print("Da luu processed_out.csv")
