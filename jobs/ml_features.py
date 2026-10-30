from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame

REQUIRED_FEATURES = [
    "lag_1h",
    "lag_2h",
    "lag_24h",
    "lag_168h",
    "rolling_6h_mean",
    "rolling_6h_std",
    "rolling_24h_mean",
    "rolling_24h_std",
    "hour_of_day_sin",
    "hour_of_day_cos",
    "day_of_week_sin",
    "day_of_week_cos",
    "is_weekend",
]

LABEL_MAP = {0: "Low", 1: "Normal", 2: "High"}

# =====================================================================
# THE SOURCE OF TRUTH BUSINESS METRIC CONTRACT
# =====================================================================
# Format: COST_MATRIX[actual_class, predicted_class]
# Underprediction = tier gap * 15 | Overprediction = tier gap * 10
COST_MATRIX = np.array(
    [
        [0.0, 10.0, 20.0],  # Actual Low (0)
        [15.0, 0.0, 10.0],  # Actual Normal (1)
        [30.0, 15.0, 0.0],  # Actual High (2)
    ],
    dtype=float,
)


@dataclass(frozen=True)
class DemandThresholds:
    q25: float
    q75: float


def select_highest_volume_station(spark_df: SparkDataFrame,training_year=None) -> str:
    from pyspark.sql import functions as F
    rc = "ride_count"
    if training_year is not None:
        spark_df=spark_df.where(F.year(F.from_utc_timestamp('station_hour_ts','America/New_York'))==training_year)
    bounds=spark_df.agg(F.min('station_hour_ts').alias('lo'),F.max('station_hour_ts').alias('hi')).first()
    if not bounds or bounds['lo'] is None:raise ValueError('No observation history')
    cutoff=bounds['hi'] if training_year is not None else bounds['lo']+(bounds['hi']-bounds['lo'])*.7
    spark_df=spark_df.where(F.col('station_hour_ts')<=F.lit(cutoff))
    row = (
        spark_df.groupBy("station_id")
        .agg(F.sum(F.col(rc)).alias("total_rides"))
        .orderBy(F.col("total_rides").desc())
        .first()
    )
    if row is None:
        raise ValueError("Gold ML observation table is empty.")
    return str(row["station_id"])


def load_station_history(
    spark_df: SparkDataFrame,
    station_id: str,
    end_ts: str | datetime | None = None,
) -> pd.DataFrame:
    """Load one start station's dense hourly history from Iceberg into pandas."""
    from pyspark.sql import functions as F
    rc = "ride_count"
    df = spark_df.filter(F.col("station_id") == str(station_id))
    if end_ts is not None:
        end_ts = pd.Timestamp(end_ts).to_pydatetime()
        df = df.filter(F.col("station_hour_ts") <= F.lit(end_ts))

    pdf = df.select(
        "target_year",
        "target_month",
        "station_id",
        "station_hour_ts",
        F.col(rc).alias("ride_count"),
    ).toPandas()

    if pdf.empty:
        raise ValueError(f"No rows found for station_id={station_id}")

    pdf["station_hour_ts"] = pd.to_datetime(pdf["station_hour_ts"])
    pdf = pdf.sort_values("station_hour_ts").reset_index(drop=True)
    return pdf


def add_calendar_features(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.copy()
    pdf["station_hour_ts"] = pd.to_datetime(pdf["station_hour_ts"])
    local = pd.to_datetime(pdf['station_hour_ts'],utc=True).dt.tz_convert('America/New_York')
    pdf["hour_of_day"] = local.dt.hour.astype(int)
    pdf["day_of_week"] = local.dt.dayofweek.astype(int)
    pdf["is_weekend"] = pdf["day_of_week"].isin([5, 6]).astype(int)
    return pdf


def add_lag_rolling_features(pdf: pd.DataFrame) -> pd.DataFrame:
    """Create leakage-safe features for each row at timestamp t.

    Every rolling feature is shifted so the current hour's ride_count is not used.
    """
    pdf = pdf.sort_values("station_hour_ts").copy()
    s = pdf["ride_count"]

    pdf["lag_1h"] = s.shift(1)
    pdf["lag_2h"] = s.shift(2)
    pdf["lag_24h"] = s.shift(24)
    pdf["lag_168h"] = s.shift(168)

    shifted = s.shift(1)
    pdf["rolling_6h_mean"] = shifted.rolling(window=6, min_periods=6).mean()
    pdf["rolling_6h_std"] = shifted.rolling(window=6, min_periods=6).std()
    pdf["rolling_24h_mean"] = shifted.rolling(window=24, min_periods=24).mean()
    pdf["rolling_24h_std"] = shifted.rolling(window=24, min_periods=24).std()
    return pdf


def add_cyclical_features(pdf: pd.DataFrame) -> pd.DataFrame:
    """Apply cyclical encoding using fixed periods."""
    pdf = pdf.copy()
    for column,period in [('hour_of_day',24),('day_of_week',7)]:
        pdf[column+'_sin']=np.sin(2*np.pi*pdf[column]/period)
        pdf[column+'_cos']=np.cos(2*np.pi*pdf[column]/period)
    return pdf.drop(columns=['hour_of_day','day_of_week'])



def build_training_features(pdf: pd.DataFrame) -> pd.DataFrame:
    if 'station_id' in pdf and pdf.station_id.astype(str).nunique()!=1:raise ValueError('Build features for one station at a time')
    pdf=pdf.sort_values('station_hour_ts').copy()
    if pdf.station_hour_ts.duplicated().any():raise ValueError('Duplicate station hours')
    # Never interpret a missing month as a one-hour step. Each gap restarts warmup.
    segments=pd.to_datetime(pdf.station_hour_ts).diff().ne(pd.Timedelta(hours=1)).cumsum()
    if segments.nunique()>1:
        return pd.concat([build_training_features(part) for _,part in pdf.groupby(segments)],ignore_index=True)
    pdf = add_calendar_features(pdf)
    pdf = add_lag_rolling_features(pdf)
    pdf = add_cyclical_features(pdf)
    pdf = pdf.dropna(subset=REQUIRED_FEATURES).reset_index(drop=True)
    return pdf


def chronological_split(
    pdf: pd.DataFrame, train_frac: float = 0.70, val_frac: float = 0.15
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    pdf = pdf.sort_values("station_hour_ts").reset_index(drop=True)
    n = len(pdf)
    if n < 10:
        raise ValueError("Not enough rows after feature engineering to split data.")

    train_end = int(n * train_frac)
    val_end = int(n * (train_frac + val_frac))
    return (
        pdf.iloc[:train_end].copy(),
        pdf.iloc[train_end:val_end].copy(),
        pdf.iloc[val_end:].copy(),
    )


def fit_thresholds(train_df: pd.DataFrame) -> DemandThresholds:
    q25 = float(train_df["ride_count"].quantile(0.25))
    q75 = float(train_df["ride_count"].quantile(0.75))
    return DemandThresholds(q25=q25, q75=q75)


def class_from_ride_count(ride_count: float, thresholds: DemandThresholds) -> int:
    if ride_count < thresholds.q25:
        return 0
    if ride_count <= thresholds.q75:
        return 1
    return 2


def apply_thresholds(pdf: pd.DataFrame, thresholds: DemandThresholds) -> pd.DataFrame:
    pdf = pdf.copy()
    pdf["demand_class"] = pdf["ride_count"].apply(
        lambda x: class_from_ride_count(x, thresholds)
    )
    return pdf


def prepare_model_frames(
    pdf: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, DemandThresholds, list[str]]:
    """Full training-prep path used by the training driver."""
    if 'station_id' in pdf and pdf.station_id.astype(str).nunique()!=1:
        raise ValueError('Train each station on a separate dense history')
    times=pd.to_datetime(pdf['station_hour_ts']).sort_values()
    if times.duplicated().any() or not times.diff().dropna().eq(pd.Timedelta(hours=1)).all():
        raise ValueError('Station history must contain unique consecutive UTC hours')
    feature_df = build_training_features(pdf)
    train_df, val_df, test_df = chronological_split(feature_df)
    thresholds = fit_thresholds(train_df)
    train_df = apply_thresholds(train_df, thresholds)
    val_df = apply_thresholds(val_df, thresholds)
    test_df = apply_thresholds(test_df, thresholds)
    return train_df, val_df, test_df, thresholds, REQUIRED_FEATURES.copy()


def calculate_penalty(
    actual_class: int | None, predicted_class: int | None
) -> float | None:
    """Returns the unified operational cost penalty for a single inference instance."""
    if actual_class is None or predicted_class is None:
        return None
    if pd.isna(actual_class) or pd.isna(predicted_class):
        return None
    return float(COST_MATRIX[int(actual_class)][int(predicted_class)])


def mean_operational_penalty(
    y_true: pd.Series | np.ndarray, y_pred: np.ndarray
) -> float:
    """Computes total average asymmetric cost over an entire series (For Optuna / Validation)."""
    actuals = np.asarray(y_true, dtype=int)
    predictions = np.asarray(y_pred, dtype=int)
    return float(COST_MATRIX[actuals, predictions].mean())


def shifted_simulation_time(
    real_now: str | datetime | None = None,
    months_back: int = 2,
) -> pd.Timestamp:
    if real_now is None:
        ts = pd.Timestamp.utcnow().tz_localize(None)
    else:
        ts = (
            pd.Timestamp(real_now).tz_convert('UTC').tz_localize(None)
            if pd.Timestamp(real_now).tzinfo
            else pd.Timestamp(real_now)
        )
    shifted = ts.to_pydatetime() - relativedelta(months=months_back)
    return pd.Timestamp(shifted).floor("h")


def build_one_prediction_feature_row(
    history_pdf: pd.DataFrame,
    station_id: str,
    feature_window_end_ts: str | datetime,
    thresholds: DemandThresholds,
    feature_columns: Iterable[str] = REQUIRED_FEATURES,
) -> tuple[pd.DataFrame, dict]:
    feature_window_end_ts = pd.Timestamp(feature_window_end_ts).floor("h")
    prediction_hour_ts = feature_window_end_ts + pd.Timedelta(hours=1)

    pdf = history_pdf.copy()
    pdf["station_hour_ts"] = pd.to_datetime(pdf["station_hour_ts"])
    pdf = pdf.sort_values("station_hour_ts").reset_index(drop=True)

    hist = pdf[pdf["station_hour_ts"] <= feature_window_end_ts].copy()
    if hist.empty:
        raise ValueError(f"No history available through {feature_window_end_ts}")

    if hist['station_hour_ts'].duplicated().any():raise ValueError('Duplicate history hours')
    ride_by_ts = hist.set_index("station_hour_ts")["ride_count"]

    def get_lag(hours_back: int) -> float:
        ts = prediction_hour_ts - pd.Timedelta(hours=hours_back)
        if ts not in ride_by_ts.index:
            raise ValueError(f"Missing required lag timestamp {ts}")
        return float(ride_by_ts.loc[ts])

    rolling_6 = [get_lag(i) for i in range(1, 7)]
    rolling_24 = [get_lag(i) for i in range(1, 25)]

    row = pd.DataFrame(
        [
            {
                "station_id": str(station_id),
                "station_hour_ts": prediction_hour_ts,
                "ride_count": np.nan,
                "lag_1h": get_lag(1),
                "lag_2h": get_lag(2),
                "lag_24h": get_lag(24),
                "lag_168h": get_lag(168),
                "rolling_6h_mean": float(np.mean(rolling_6)),
                "rolling_6h_std": float(np.std(rolling_6, ddof=1)),
                "rolling_24h_mean": float(np.mean(rolling_24)),
                "rolling_24h_std": float(np.std(rolling_24, ddof=1)),
            }
        ]
    )

    row = add_calendar_features(row)
    row = add_cyclical_features(row)

    current_actual = get_lag(1)
    baseline_class = class_from_ride_count(current_actual, thresholds)

    full_by_ts = pdf.set_index("station_hour_ts")["ride_count"]
    actual_ride_count = None
    actual_class = None
    if prediction_hour_ts in full_by_ts.index:
        actual_ride_count = float(full_by_ts.loc[prediction_hour_ts])
        actual_class = class_from_ride_count(actual_ride_count, thresholds)

    meta = {
        "feature_window_end_ts": feature_window_end_ts,
        "prediction_hour_ts": prediction_hour_ts,
        "baseline_predicted_class": baseline_class,
        "actual_ride_count": actual_ride_count,
        "actual_class": actual_class,
    }

    return row[list(feature_columns)], meta
