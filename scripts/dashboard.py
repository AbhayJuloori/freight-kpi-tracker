"""
Interactive Freight Analytics Dashboard
Run: python scripts/dashboard.py
Then open http://localhost:8050
"""
import json
from pathlib import Path

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html, dash_table

PROCESSED_DIR = Path("data/processed")

# ── Colour palette ────────────────────────────────────────────
C_NORMAL = "#4C8BF5"      # steel-blue
C_ANOMALY = "#F5A623"     # amber
C_HIGH = "#E53935"        # red
C_BG = "#1E1E2E"
C_SURFACE = "#2A2A3E"
C_TEXT = "#E0E0E0"
C_MUTED = "#888"

TEMPLATE = "plotly_dark"


# ── Data loading ──────────────────────────────────────────────

def load_data():
    ships = pd.read_csv(PROCESSED_DIR / "shipments.csv", parse_dates=["ship_date"])
    gt = pd.read_parquet(PROCESSED_DIR / "anomaly_ground_truth.parquet")
    meta = json.loads((PROCESSED_DIR / "generation_metadata.json").read_text())

    eval_report = None
    eval_path = PROCESSED_DIR / "evaluation_report.json"
    if eval_path.exists():
        eval_report = json.loads(eval_path.read_text())

    return ships, gt, meta, eval_report


def compute_local_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Recompute Z-score and IQR flags in pandas. Adds 'flagged' column."""
    df = df.copy()
    df["cpl"] = df["total_cost"] / df["weight_lbs"].clip(lower=1e-6)

    # Z-score
    stats = (
        df.groupby(["lane_id", "mode"])["cpl"]
        .agg(mean="mean", std="std", count="count")
        .reset_index()
        .query("count >= 10")
    )
    merged = df.merge(stats, on=["lane_id", "mode"], how="left")
    merged["z"] = (merged["cpl"] - merged["mean"]) / merged["std"].clip(lower=1e-6)
    zscore_ids = set(merged.loc[merged["z"].abs() > 2.5, "shipment_id"])

    # IQR
    iqr_stats = (
        df.groupby(["lane_id", "mode"])["cpl"]
        .agg(
            q1=lambda x: x.quantile(0.25),
            q3=lambda x: x.quantile(0.75),
            count="count",
        )
        .reset_index()
        .query("count >= 10")
    )
    iqr_stats["lower"] = iqr_stats["q1"] - 1.5 * (iqr_stats["q3"] - iqr_stats["q1"])
    iqr_stats["upper"] = iqr_stats["q3"] + 1.5 * (iqr_stats["q3"] - iqr_stats["q1"])
    merged2 = df.merge(iqr_stats, on=["lane_id", "mode"], how="left")
    iqr_ids = set(merged2.loc[
        (merged2["cpl"] > merged2["upper"]) | (merged2["cpl"] < merged2["lower"]),
        "shipment_id"
    ])

    df["flagged"] = df["shipment_id"].isin(zscore_ids | iqr_ids).astype(int)
    return df


def compute_lane_week_trends(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["cpl"] = df["total_cost"] / df["weight_lbs"].clip(lower=1e-6)
    df["week_start"] = df["ship_date"].dt.to_period("W").dt.start_time
    weekly = (
        df.groupby(["week_start", "lane_id", "mode"])
        .agg(shipment_count=("shipment_id", "count"), avg_cpl=("cpl", "mean"))
        .reset_index()
        .sort_values(["lane_id", "mode", "week_start"])
    )
    weekly["rolling_4wk"] = (
        weekly.groupby(["lane_id", "mode"])["avg_cpl"]
        .transform(lambda x: x.shift(1).rolling(4, min_periods=4).mean())
    )
    weekly["pct_dev"] = (
        (weekly["avg_cpl"] - weekly["rolling_4wk"]) /
        weekly["rolling_4wk"].clip(lower=1e-6)
    )
    weekly["prior_weeks"] = weekly.groupby(["lane_id", "mode"]).cumcount()
    weekly["is_anomalous"] = (
        (weekly["pct_dev"].abs() > 0.20) & (weekly["prior_weeks"] >= 4)
    ).astype(int)
    return weekly


# ── KPI cards ─────────────────────────────────────────────────

def kpi_card(title: str, value: str, color: str = C_TEXT) -> dbc.Card:
    return dbc.Card(
        dbc.CardBody([
            html.P(title, style={"color": C_MUTED, "fontSize": "0.8rem", "marginBottom": "4px"}),
            html.H4(value, style={"color": color, "fontWeight": "bold"}),
        ]),
        style={"backgroundColor": C_SURFACE, "border": "none"},
    )


# ── Chart builders ────────────────────────────────────────────

def fig_violin_cpl(df: pd.DataFrame) -> go.Figure:
    df = df.copy()
    df["anomaly_label"] = df["flagged"].map({0: "Normal", 1: "Flagged"})
    fig = px.violin(
        df, x="mode", y="cpl", color="anomaly_label",
        color_discrete_map={"Normal": C_NORMAL, "Flagged": C_ANOMALY},
        box=True, points=False,
        labels={"cpl": "Cost per lb ($)", "mode": "Mode", "anomaly_label": ""},
        title="Cost-per-lb Distribution by Mode",
        template=TEMPLATE,
    )
    fig.update_layout(paper_bgcolor=C_BG, plot_bgcolor=C_BG, font_color=C_TEXT)
    return fig


def fig_weekly_cpl(df: pd.DataFrame, trends: pd.DataFrame) -> go.Figure:
    df_weekly = (
        df.groupby(["ship_date", "mode"])
        .agg(avg_cpl=("cpl", "mean"))
        .reset_index()
        .rename(columns={"ship_date": "week"})
    )
    df_weekly["week"] = pd.to_datetime(df_weekly["week"])
    df_weekly = df_weekly.resample("W", on="week").agg(avg_cpl=("avg_cpl", "mean")).reset_index()

    anomaly_weeks = trends.loc[trends["is_anomalous"] == 1, "week_start"].unique()

    fig = px.line(
        df_weekly, x="week", y="avg_cpl",
        labels={"avg_cpl": "Avg Cost/lb ($)", "week": ""},
        title="Weekly Avg Cost-per-lb (All Modes)",
        template=TEMPLATE,
        color_discrete_sequence=[C_NORMAL],
    )
    for wk in anomaly_weeks:
        fig.add_vline(x=pd.Timestamp(wk), line_color=C_ANOMALY, line_width=1, opacity=0.4)
    fig.update_layout(paper_bgcolor=C_BG, plot_bgcolor=C_BG, font_color=C_TEXT)
    return fig


def fig_lane_heatmap(df: pd.DataFrame) -> go.Figure:
    top_lanes = (
        df.groupby("lane_id")["total_cost"].sum()
        .nlargest(20).index.tolist()
    )
    subset = df[df["lane_id"].isin(top_lanes)]
    agg = subset.groupby("lane_id").agg(
        total_cost=("total_cost", "sum"),
        anomaly_count=("flagged", "sum"),
        total_count=("shipment_id", "count"),
    ).reset_index()
    agg["anomaly_density"] = agg["anomaly_count"] / agg["total_count"].clip(lower=1)
    agg = agg.sort_values("total_cost", ascending=True)

    fig = px.bar(
        agg, x="total_cost", y="lane_id", orientation="h",
        color="anomaly_density",
        color_continuous_scale=["#4C8BF5", "#F5A623", "#E53935"],
        labels={"total_cost": "Total Freight Spend ($)", "lane_id": "Lane", "anomaly_density": "Anomaly Density"},
        title="Top 20 Lanes by Total Spend (colored by anomaly density)",
        template=TEMPLATE,
    )
    fig.update_layout(paper_bgcolor=C_BG, plot_bgcolor=C_BG, font_color=C_TEXT, height=550)
    return fig


def fig_carrier_scorecard(df: pd.DataFrame) -> go.Figure:
    agg = df.groupby("carrier_id").agg(
        on_time_rate=("on_time_flag", "mean"),
        avg_cost=("total_cost", "mean"),
        volume=("shipment_id", "count"),
    ).reset_index()
    fig = px.scatter(
        agg, x="avg_cost", y="on_time_rate", size="volume",
        hover_name="carrier_id",
        color="on_time_rate",
        color_continuous_scale=["#E53935", "#F5A623", "#4C8BF5"],
        labels={"avg_cost": "Avg Cost/Shipment ($)", "on_time_rate": "On-Time Rate", "volume": "Shipments"},
        title="Carrier Scorecard — On-Time Rate vs Avg Cost",
        template=TEMPLATE,
    )
    fig.update_layout(paper_bgcolor=C_BG, plot_bgcolor=C_BG, font_color=C_TEXT)
    return fig


def eval_table(eval_report) -> dbc.Card:
    if eval_report is None:
        return dbc.Card(
            dbc.CardBody(html.P("Run `make evaluate` to generate evaluation metrics.", style={"color": C_MUTED})),
            style={"backgroundColor": C_SURFACE, "border": "none"},
        )
    rows = []
    for method, m in eval_report["per_method"].items():
        rows.append({
            "Method": method,
            "Precision": f"{m['precision']:.3f}",
            "Recall": f"{m['recall']:.3f}",
            "F1": f"{m['f1']:.3f}",
            "FPR": f"{m['fpr']:.3f}",
            "Flagged": f"{m['n_flagged']:,}",
        })
    d = eval_report["distinct_shipment"]
    rows.append({
        "Method": "ALL (distinct)",
        "Precision": f"{d['precision']:.3f}",
        "Recall": f"{d['recall']:.3f}",
        "F1": f"{d['f1']:.3f}",
        "FPR": f"{d['fpr']:.3f}",
        "Flagged": f"{d['n_flagged']:,}",
    })
    table = dash_table.DataTable(
        data=rows,
        columns=[{"name": c, "id": c} for c in rows[0].keys()],
        style_header={"backgroundColor": C_BG, "color": C_MUTED, "fontWeight": "bold"},
        style_cell={"backgroundColor": C_SURFACE, "color": C_TEXT, "border": "1px solid #333"},
        style_data_conditional=[
            {"if": {"filter_query": '{Method} = "ALL (distinct)"'},
             "fontWeight": "bold", "color": C_ANOMALY},
        ],
    )
    return dbc.Card(
        dbc.CardBody([html.H6("Anomaly Detection Evaluation", style={"color": C_TEXT}), table]),
        style={"backgroundColor": C_SURFACE, "border": "none"},
    )


# ── App layout ────────────────────────────────────────────────

def build_layout(ships, gt, meta, eval_report):
    ships = compute_local_flags(ships)
    ships["cpl"] = ships["total_cost"] / ships["weight_lbs"].clip(lower=1e-6)
    trends = compute_lane_week_trends(ships)

    total_ships = f"{len(ships):,}"
    anomaly_rate = f"{ships['flagged'].mean():.1%}"
    avg_cpl = f"${ships['cpl'].mean():.3f}"
    on_time = f"{ships['on_time_flag'].mean():.1%}"

    layout = dbc.Container([
        dbc.Row(dbc.Col(
            html.H3("Freight Cost Anomaly & KPI Tracker",
                    style={"color": C_TEXT, "marginTop": "20px", "marginBottom": "4px"}),
        )),

        dbc.Row([
            dbc.Col(kpi_card("Total Shipments", total_ships), md=3),
            dbc.Col(kpi_card("Anomaly Rate", anomaly_rate, C_ANOMALY), md=3),
            dbc.Col(kpi_card("Avg Cost/lb", avg_cpl), md=3),
            dbc.Col(kpi_card("On-Time Rate", on_time, C_NORMAL), md=3),
        ], className="mb-3"),

        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_violin_cpl(ships)), md=6),
            dbc.Col(dcc.Graph(figure=fig_weekly_cpl(ships, trends)), md=6),
        ], className="mb-3"),

        dbc.Row(dbc.Col(dcc.Graph(figure=fig_lane_heatmap(ships))), className="mb-3"),

        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_carrier_scorecard(ships)), md=7),
            dbc.Col(eval_table(eval_report), md=5),
        ], className="mb-3"),

        dbc.Row(dbc.Col(
            html.P(
                f"seed_source={meta['seed_source']} | "
                f"run_id={meta['run_id']} | "
                f"generated_at={meta['generated_at'][:19]}",
                style={"color": C_MUTED, "fontSize": "0.75rem", "marginTop": "8px"},
            )
        )),
    ], fluid=True, style={"backgroundColor": C_BG, "minHeight": "100vh", "padding": "0 24px"})

    return layout


def main():
    ships, gt, meta, eval_report = load_data()
    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.CYBORG],
        title="Freight KPI Tracker",
    )
    app.layout = build_layout(ships, gt, meta, eval_report)
    app.run(debug=False, host="0.0.0.0", port=8050)


if __name__ == "__main__":
    main()
