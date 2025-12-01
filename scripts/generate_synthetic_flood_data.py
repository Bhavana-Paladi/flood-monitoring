import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

"""
Synthetic flood data generator for UK stations.

Outputs:
- data/synthetic/synthetic_stations.csv
- data/synthetic/synthetic_station_daily.csv

Design goals:
- 150 stations across UK with different rivers/catchments/towns
- 12 months of daily readings
- Mostly normal levels, but clear High / Very high events
- Columns compatible with your existing BigQuery setup
"""

np.random.seed(42)


def make_station_metadata(n_stations: int = 150) -> pd.DataFrame:
    station_ids = [f"UK{idx:03d}" for idx in range(1, n_stations + 1)]

    rivers = [
        "River Thames", "River Severn", "River Trent", "River Mersey",
        "River Tyne", "River Clyde", "River Avon", "River Ouse"
    ]
    catchments = [
        "Thames Basin", "Severn Basin", "Trent Basin", "Mersey Basin",
        "North East Coast", "North West Coast", "South East Coast"
    ]
    towns = [
        "London", "Manchester", "Liverpool", "Birmingham",
        "Leeds", "Sheffield", "Newcastle", "Bristol",
        "Cardiff", "Glasgow", "Edinburgh", "Belfast"
    ]

    # Rough bounding box around the UK
    lat_min, lat_max = 50.0, 57.5
    lon_min, lon_max = -5.5, 1.5

    records = []
    for sid in station_ids:
        river = np.random.choice(rivers)
        catchment = np.random.choice(catchments)
        town = np.random.choice(towns)

        # Risk profile controls how often this station floods
        risk_profile = np.random.choice(
            ["Low", "Medium", "High", "Very high"],
            p=[0.5, 0.3, 0.15, 0.05]
        )

        # Base level (m) and variability
        base_level = np.random.uniform(0.4, 1.6)
        base_std = np.random.uniform(0.05, 0.25)

        # Typical range and alert thresholds
        typical_low = max(0.05, base_level - np.random.uniform(0.1, 0.4))
        typical_high = base_level + np.random.uniform(0.2, 0.6)

        alert_high = typical_high
        alert_very_high = typical_high + base_std * np.random.uniform(1.2, 2.5)

        lat = np.random.uniform(lat_min, lat_max)
        lon = np.random.uniform(lon_min, lon_max)

        records.append(
            dict(
                stationReference=sid,
                stationName=f"{town} {river} Gauge",
                riverName=river,
                catchmentName=catchment,
                town=town,
                lat=round(lat, 5),
                long=round(lon, 5),
                typical_range_low=round(typical_low, 3),
                typical_range_high=round(typical_high, 3),
                status="active",
                alert_threshold_high=round(alert_high, 3),
                alert_threshold_very_high=round(alert_very_high, 3),
                base_level=base_level,
                base_std=base_std,
                risk_profile=risk_profile,
            )
        )

    return pd.DataFrame(records)


def generate_daily_readings(
    stations: pd.DataFrame,
    start_date: str = "2024-11-01",
    n_days: int = 365,
) -> pd.DataFrame:
    dates = pd.date_range(start=start_date, periods=n_days, freq="D")
    all_rows = []

    for _, row in stations.iterrows():
        sid = row["stationReference"]
        base_level = row["base_level"]
        base_std = row["base_std"]
        typical_low = row["typical_range_low"]
        typical_high = row["typical_range_high"]
        alert_high = row["alert_threshold_high"]
        alert_very_high = row["alert_threshold_very_high"]
        risk_profile = row["risk_profile"]

        # Seasonal amplitude: wetter winter, slightly higher levels
        seasonal_amp = np.random.uniform(0.05, 0.3)

        # Number of storm windows depends on risk profile
        profile_to_storms = {
            "Low": (1, 2),
            "Medium": (2, 3),
            "High": (3, 4),
            "Very high": (4, 5),
        }
        min_storms, max_storms = profile_to_storms[risk_profile]
        n_storms = np.random.randint(min_storms, max_storms + 1)

        storm_days = set()
        for _ in range(n_storms):
            start_idx = np.random.randint(0, n_days - 7)
            length = np.random.randint(2, 5)
            for d_idx in range(start_idx, start_idx + length):
                storm_days.add(d_idx)

        # How strong storms are relative to base_std
        profile_to_mult = {
            "Low": 1.5,
            "Medium": 2.0,
            "High": 2.5,
            "Very high": 3.0,
        }
        storm_mult = profile_to_mult[risk_profile]

        for idx, d in enumerate(dates):
            day_of_year = d.timetuple().tm_yday
            season = seasonal_amp * np.sin(2 * np.pi * day_of_year / 365.0)

            noise = np.random.normal(0, base_std)
            level = base_level + season + noise

            # Storm boost
            if idx in storm_days:
                level += storm_mult * base_std

            level = max(0.05, level)

            # Classify risk for this reading
            if level >= alert_very_high:
                flood_risk = "Very high"
            elif level >= alert_high:
                flood_risk = "High"
            elif level >= typical_high:
                flood_risk = "Medium"
            else:
                flood_risk = "Low"

            all_rows.append(
                dict(
                    stationReference=sid,
                    date=d.date().isoformat(),
                    level=round(float(level), 3),
                    typical_range_low=typical_low,
                    typical_range_high=typical_high,
                    flood_risk=flood_risk,
                )
            )

    daily = pd.DataFrame(all_rows)
    return daily


def summarize_stations(daily: pd.DataFrame, stations_meta: pd.DataFrame) -> pd.DataFrame:
    # Aggregate daily readings into station-level stats for station_baseline
    agg = (
        daily.groupby("stationReference")["level"]
        .agg(
            n_readings_total="count",
            mean_level_total="mean",
            std_level_total="std",
            p05_level=lambda s: s.quantile(0.05),
            median_level="median",
            p95_level=lambda s: s.quantile(0.95),
        )
        .reset_index()
    )

    # Most common flood risk label per station
    risk = (
        daily.groupby(["stationReference", "flood_risk"])
        .size()
        .reset_index(name="n")
    )
    risk = risk.sort_values(["stationReference", "n"], ascending=[True, False])
    risk = risk.drop_duplicates("stationReference")[["stationReference", "flood_risk"]]
    risk = risk.rename(columns={"flood_risk": "label"})

    # Merge metadata + stats + label
    baseline = (
        stations_meta.merge(agg, on="stationReference")
        .merge(risk, on="stationReference")
    )

    # Round some numeric columns for nicer tables
    for col in [
        "mean_level_total",
        "std_level_total",
        "p05_level",
        "median_level",
        "p95_level",
    ]:
        baseline[col] = baseline[col].round(3)

    # Drop internal columns
    baseline = baseline.drop(columns=["base_level", "base_std", "risk_profile"])

    return baseline


def main():
    base_dir = Path(__file__).resolve().parents[1]
    out_dir = base_dir / "data" / "synthetic"
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Generating station metadata...")
    stations = make_station_metadata()
    print(f"  -> {len(stations)} stations")

    print("Generating daily readings (12 months)...")
    daily = generate_daily_readings(stations, start_date="2024-11-01", n_days=365)
    print(f"  -> {len(daily)} daily rows")

    print("Summarizing station baselines...")
    baseline = summarize_stations(daily, stations)

    stations_path = out_dir / "synthetic_stations.csv"
    daily_path = out_dir / "synthetic_station_daily.csv"

    print(f"Writing station baseline -> {stations_path}")
    baseline.to_csv(stations_path, index=False)

    print(f"Writing daily readings  -> {daily_path}")
    daily.to_csv(daily_path, index=False)

    # Quick sanity check summary
    print("\nExample baseline rows:")
    print(baseline.head())

    print("\nOverall daily level distribution:")
    print(daily["level"].describe())

    print("\nFlood risk distribution:")
    print(daily["flood_risk"].value_counts(normalize=True).round(3))


if __name__ == "__main__":
    main()
