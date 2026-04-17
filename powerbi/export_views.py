from pathlib import Path

import numpy as np
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
OUTPUT_DIR = ROOT_DIR / "powerbi" / "data"


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    shipments = pd.read_csv(PROCESSED_DIR / "shipments.csv", parse_dates=["ship_date"])
    anomaly_truth = pd.read_parquet(PROCESSED_DIR / "anomaly_ground_truth.parquet")

    flagged_shipments = (
        anomaly_truth.loc[anomaly_truth["is_anomaly"] == 1, ["shipment_id"]]
        .drop_duplicates()
        .assign(is_anomaly_flag=1)
    )
    shipments = shipments.merge(flagged_shipments, on="shipment_id", how="left")
    shipments["is_anomaly_flag"] = shipments["is_anomaly_flag"].fillna(0).astype(int)
    shipments["origin_state"] = shipments["origin_city"].str.split(",", n=1).str[1]
    shipments["month"] = shipments["ship_date"].dt.to_period("M").dt.to_timestamp()
    shipments["cost_per_lb"] = shipments["total_cost"].div(
        shipments["weight_lbs"].replace({0: np.nan})
    )

    return shipments, flagged_shipments


def finalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in result.columns:
        if pd.api.types.is_datetime64_any_dtype(result[column]):
            result[column] = result[column].dt.strftime("%Y-%m-%d")
    return result


def build_vw_carrier_ontime(shipments: pd.DataFrame) -> pd.DataFrame:
    result = (
        shipments.groupby(["carrier_id", "mode", "month"], as_index=False)
        .agg(
            total_shipments=("shipment_id", "size"),
            on_time_shipments=("on_time_flag", "sum"),
        )
        .sort_values(["carrier_id", "mode", "month"], kind="stable")
    )
    result["on_time_rate"] = result["on_time_shipments"].div(result["total_shipments"])
    return result[
        [
            "carrier_id",
            "mode",
            "month",
            "total_shipments",
            "on_time_shipments",
            "on_time_rate",
        ]
    ]


def build_vw_cost_by_mode_region(shipments: pd.DataFrame) -> pd.DataFrame:
    result = (
        shipments.groupby(["mode", "origin_state", "month"], as_index=False)
        .agg(
            shipment_count=("shipment_id", "size"),
            avg_total_cost=("total_cost", "mean"),
            avg_cost_per_lb=("cost_per_lb", "mean"),
            median_total_cost=("total_cost", "median"),
            total_spend=("total_cost", "sum"),
        )
        .sort_values(["mode", "origin_state", "month"], kind="stable")
    )
    return result[
        [
            "mode",
            "origin_state",
            "month",
            "shipment_count",
            "avg_total_cost",
            "avg_cost_per_lb",
            "median_total_cost",
            "total_spend",
        ]
    ]


def build_vw_anomaly_rate_by_region(shipments: pd.DataFrame) -> pd.DataFrame:
    result = (
        shipments.groupby(["origin_state", "mode", "month"], as_index=False)
        .agg(
            total_shipments=("shipment_id", "nunique"),
            flagged_shipments=("is_anomaly_flag", "sum"),
        )
        .sort_values(["origin_state", "mode", "month"], kind="stable")
    )
    result["anomaly_rate"] = result["flagged_shipments"].div(result["total_shipments"])
    return result[
        [
            "origin_state",
            "mode",
            "month",
            "total_shipments",
            "flagged_shipments",
            "anomaly_rate",
        ]
    ]


def build_vw_carrier_scorecard(shipments: pd.DataFrame) -> pd.DataFrame:
    result = (
        shipments.groupby(["carrier_id", "mode"], as_index=False)
        .agg(
            total_shipments=("shipment_id", "size"),
            on_time_rate=("on_time_flag", "mean"),
            avg_cost=("total_cost", "mean"),
            avg_cost_per_lb=("cost_per_lb", "mean"),
            anomaly_rate=("is_anomaly_flag", "mean"),
            total_spend=("total_cost", "sum"),
        )
        .sort_values(["carrier_id", "mode"], kind="stable")
    )
    return result[
        [
            "carrier_id",
            "mode",
            "total_shipments",
            "on_time_rate",
            "avg_cost",
            "avg_cost_per_lb",
            "anomaly_rate",
            "total_spend",
        ]
    ]


def build_vw_executive_summary(
    shipments: pd.DataFrame, flagged_shipments: pd.DataFrame
) -> pd.DataFrame:
    total_shipments = len(shipments)
    total_anomalies_flagged = flagged_shipments["shipment_id"].nunique()
    result = pd.DataFrame(
        [
            {
                "total_shipments": total_shipments,
                "total_freight_spend": shipments["total_cost"].sum(),
                "avg_cost_per_shipment": shipments["total_cost"].mean(),
                "overall_on_time_rate": shipments["on_time_flag"].mean(),
                "total_anomalies_flagged": total_anomalies_flagged,
                "overall_anomaly_rate": total_anomalies_flagged / total_shipments,
                "data_start_date": shipments["ship_date"].min(),
                "data_end_date": shipments["ship_date"].max(),
            }
        ]
    )
    return result


def build_vw_lane_risk(shipments: pd.DataFrame) -> pd.DataFrame:
    lane_stats = (
        shipments.groupby("lane_id", as_index=False)
        .agg(
            total_shipments=("shipment_id", "size"),
            anomaly_count=("is_anomaly_flag", "sum"),
            late_count=("on_time_flag", lambda s: (s == 0).sum()),
        )
        .sort_values("lane_id", kind="stable")
    )
    lane_stats = lane_stats[lane_stats["total_shipments"] >= 50].copy()
    lane_stats["anomaly_rate"] = lane_stats["anomaly_count"].div(
        lane_stats["total_shipments"]
    )
    lane_stats["late_rate"] = lane_stats["late_count"].div(lane_stats["total_shipments"])

    avg_normal_cost = (
        shipments.loc[shipments["is_anomaly_flag"] == 0]
        .groupby("lane_id")["total_cost"]
        .mean()
        .rename("avg_normal_cost")
    )
    avg_anomalous_cost = (
        shipments.loc[shipments["is_anomaly_flag"] == 1]
        .groupby("lane_id")["total_cost"]
        .mean()
        .rename("avg_anomalous_cost")
    )
    lane_stats = lane_stats.merge(avg_normal_cost, on="lane_id", how="left")
    lane_stats = lane_stats.merge(avg_anomalous_cost, on="lane_id", how="left")

    cost_ratio = (
        lane_stats["avg_anomalous_cost"] - lane_stats["avg_normal_cost"]
    ).div(lane_stats["avg_normal_cost"])

    result = lane_stats.assign(
        anomaly_rate_pct=lambda df: (df["anomaly_rate"] * 100).round(2),
        late_rate_pct=lambda df: (df["late_rate"] * 100).round(2),
        avg_normal_cost=lambda df: df["avg_normal_cost"].round(2),
        avg_anomalous_cost=lambda df: df["avg_anomalous_cost"].round(2),
        cost_overrun_pct=cost_ratio.mul(100).round(1),
        risk_score=(
            0.4 * lane_stats["anomaly_rate"]
            + 0.3 * lane_stats["late_rate"]
            + 0.3 * np.minimum(cost_ratio, 10) / 10
        ).round(4),
    )
    result = result.sort_values("risk_score", ascending=False, kind="stable")

    return result[
        [
            "lane_id",
            "total_shipments",
            "anomaly_count",
            "anomaly_rate_pct",
            "late_count",
            "late_rate_pct",
            "avg_normal_cost",
            "avg_anomalous_cost",
            "cost_overrun_pct",
            "risk_score",
        ]
    ]


def export_views() -> list[Path]:
    shipments, flagged_shipments = load_inputs()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    exports = {
        "vw_carrier_ontime.csv": build_vw_carrier_ontime(shipments),
        "vw_cost_by_mode_region.csv": build_vw_cost_by_mode_region(shipments),
        "vw_anomaly_rate_by_region.csv": build_vw_anomaly_rate_by_region(shipments),
        "vw_carrier_scorecard.csv": build_vw_carrier_scorecard(shipments),
        "vw_executive_summary.csv": build_vw_executive_summary(
            shipments, flagged_shipments
        ),
        "vw_lane_risk.csv": build_vw_lane_risk(shipments),
    }

    written_files: list[Path] = []
    for filename, dataframe in exports.items():
        output_path = OUTPUT_DIR / filename
        finalize_dates(dataframe).to_csv(output_path, index=False)
        written_files.append(output_path)

    return written_files


if __name__ == "__main__":
    for path in export_views():
        print(path)
