import os
import numpy as np
import pandas as pd

# -----------------------------
# CONFIG
# -----------------------------
N_STATIONS = 150
START_DATE = "2024-01-01"
END_DATE = "2024-12-31"   # 12 months
OUTPUT_DIR = "data/synthetic"

np.random.seed(42)

# -----------------------------
# HELPER DATA
# -----------------------------
regions = [
    "North East", "North West", "Yorkshire & Humber", "Midlands",
    "East of England", "South East", "South West", "Wales", "Scotland"
]

rivers = [
    "River Thames", "River Trent", "River Severn", "River Ouse",
    "River Tyne", "River Avon", "River Dee", "River Clyde",
    "River Wear", "River Don", "River Wye", "River Eden"
]

catchments = [
    "Upper Thames", "Lower Thames", "Trent Valley", "Severn Estuary",
    "Humber Estuary", "Mersey Basin", "Clyde Basin", "Tyne Catchment"
]

towns = [
    "London", "Oxford", "Reading", "Birmingham", "Manchester", "Leeds",
    "Newcastle", "Cardiff", "Glasgow", "Bristol", "Norwich", "Sheffield",
    "Nottingham", "Leicester", "Plymouth", "Exeter"
]


def random_coord_in_uk():
    """Rough bounding box around the UK."""
    lat = np.random.uniform(50.0, 57.5)    # south–north
    lon = np.random.uniform(-5.5, 1.5)     # west–east
    return lat, lon


# -----------------------------
# 1. CREATE STATION BASELINE
# -----------------------------
stations = []
for i in range(N_STATIONS):
    station_ref = f"UK{(i+1):03d}"
    region = np.random.choice(regions)
    river = np.random.choice(rivers)
    catchment = np.random.choice(catchments)
    town = np.random.choice(towns)

    # Typical range: low between 0.2–1.5m, range width between 0.8–2.0m
    typical_low = np.round(np.random.uniform(0.2, 1.5), 2)
    typical_high = np.round(typical_low + np.random.uniform(0.8, 2.0), 2)

    lat, lon = random_coord_in_uk()

    stations.append({
        "stationReference": station_ref,
        "riverName": river,
        "catchmentName": catchment,
        "town": town,
        "region": region,
        "latitude": round(lat, 5),
        "longitude": round(lon, 5),
        "typical_range_low": typical_low,
        "typical_range_high": typical_high
    })

df_stations = pd.DataFrame(stations)

# -----------------------------
# 2. CREATE DAILY LEVELS (1 YEAR)
# -----------------------------
dates = pd.date_range(START_DATE, END_DATE, freq="D")
rows = []

# assign a phase per region so their seasonality is not identical
region_phase = {
    r: np.random.uniform(0, 2 * np.pi) for r in regions
}

for _, s in df_stations.iterrows():
    station_ref = s["stationReference"]
    region = s["region"]
    river = s["riverName"]
    catchment = s["catchmentName"]
    town = s["town"]
    low = s["typical_range_low"]
    high = s["typical_range_high"]

    # base level around middle of typical range
    base_mid = (low + high) / 2

    # For each station, random number of flood events
    n_events = np.random.randint(2, 6)  # 2–5 events per year
    event_days = np.random.choice(len(dates), size=n_events, replace=False)

    for idx, d in enumerate(dates):
        day_of_year = d.timetuple().tm_yday

        # Seasonal factor: wetter winter / early spring, drier summer
        phase = region_phase[region]
        seasonal = 0.3 * np.sin(2 * np.pi * day_of_year / 365 + phase)

        # Random noise
        noise = np.random.normal(loc=0.0, scale=0.15)

        level = base_mid + seasonal * (high - low) + noise

        # Occasionally create flood spikes
        if idx in event_days:
            spike = np.random.uniform(0.6, 1.8) * (high - low)
            level += spike

        # Clip to non-negative
        level = max(0.0, level)

        # Flood risk category
        if level <= high:
            risk = "Low"
        elif level <= high + 0.3:
            risk = "Medium"
        elif level <= high + 0.7:
            risk = "High"
        else:
            risk = "Severe"

        # Useful derived metrics
        level_minus_low = level - low
        level_minus_high = level - high
        pct_of_range = (level - low) / (high - low) * 100

        rows.append({
            "stationReference": station_ref,
            "date": d.date().isoformat(),
            "level": round(level, 3),
            "typical_range_low": low,
            "typical_range_high": high,
            "level_minus_low": round(level_minus_low, 3),
            "level_minus_high": round(level_minus_high, 3),
            "pct_of_range": round(pct_of_range, 1),
            "flood_risk": risk,
            "riverName": river,
            "catchmentName": catchment,
            "town": town,
            "region": region
        })

df_daily = pd.DataFrame(rows)

# -----------------------------
# 3. WRITE TO CSV
# -----------------------------
os.makedirs(OUTPUT_DIR, exist_ok=True)

stations_path = os.path.join(OUTPUT_DIR, "synthetic_stations.csv")
daily_path = os.path.join(OUTPUT_DIR, "synthetic_station_daily.csv")

df_stations.to_csv(stations_path, index=False)
df_daily.to_csv(daily_path, index=False)

print(f"Written station baseline -> {stations_path}")
print(f"Written daily station levels -> {daily_path}")
print("Example station:")
print(df_stations.head(3))
print("Example daily rows:")
print(df_daily.head(5))

