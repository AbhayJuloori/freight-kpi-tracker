"""
Build a standalone Plotly HTML dashboard from processed freight KPI data.

Usage:
    python3 scripts/build_dashboard.py
"""
from pathlib import Path

import pandas as pd

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit(
        "plotly is required to build the dashboard. Install it with `pip install plotly`."
    ) from exc


ROOT_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
OUTPUT_FILE = ROOT_DIR / "dashboard.html"

TITLE = "Freight Cost Anomaly & KPI Tracker"
SUBTITLE = "US DOT BTS Data | Jan 2023 - Jun 2024 | 75K Shipments"

PAPER_BG = "#0f1117"
PLOT_BG = "#1a1d2e"
GRID_COLOR = "#2d3347"
TEXT_COLOR = "#FFFFFF"
MUTED_TEXT = "#B8C1D1"

MODE_COLORS = {
    "PARCEL": "#4ECDC4",
    "LTL": "#FFE66D",
    "FTL": "#FF6B6B",
}

FLAG_COLORS = {
    "ZSCORE": "#FF6B6B",
    "IQR": "#FF9F43",
    "MOVING_AVG": "#FF4444",
}

MODE_ORDER = ["PARCEL", "LTL", "FTL"]
FLAG_ORDER = ["ZSCORE", "IQR", "MOVING_AVG"]


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    shipments = pd.read_csv(PROCESSED_DIR / "shipments.csv", parse_dates=["ship_date"])
    fuel_surcharges = pd.read_csv(
        PROCESSED_DIR / "fuel_surcharges.csv",
        parse_dates=["week_start"],
    )
    anomaly_flags = pd.read_csv(
        PROCESSED_DIR / "anomaly_flags.csv",
        parse_dates=["FLAG_DATE", "CREATED_AT"],
    )

    for name, frame in {
        "shipments.csv": shipments,
        "fuel_surcharges.csv": fuel_surcharges,
        "anomaly_flags.csv": anomaly_flags,
    }.items():
        if frame.empty:
            raise ValueError(f"{name} is empty; dashboard cannot be built.")

    return shipments, fuel_surcharges, anomaly_flags


def prepare_shipments(shipments: pd.DataFrame) -> pd.DataFrame:
    prepared = shipments.copy()
    prepared["origin_state"] = prepared["origin_city"].str.rsplit(",", n=1).str[-1]
    prepared["ship_month"] = prepared["ship_date"].dt.to_period("M").dt.to_timestamp()
    prepared["cost_per_lb"] = (
        prepared["total_cost"] / prepared["weight_lbs"].replace(0, pd.NA)
    ).fillna(0.0)
    return prepared


def build_figure(
    shipments: pd.DataFrame,
    fuel_surcharges: pd.DataFrame,
    anomaly_flags: pd.DataFrame,
) -> go.Figure:
    shipments = prepare_shipments(shipments)

    total_shipments = len(shipments)
    total_spend_millions = shipments["total_cost"].sum() / 1_000_000
    on_time_rate = shipments["on_time_flag"].mean()
    anomaly_flag_rate = anomaly_flags["SHIPMENT_ID"].nunique() / total_shipments

    monthly_cost = (
        shipments.groupby(["ship_month", "mode"], as_index=False)["cost_per_lb"]
        .mean()
        .sort_values("ship_month")
    )

    top_states = (
        shipments.groupby("origin_state", as_index=False)["total_cost"]
        .sum()
        .nlargest(10, "total_cost")
        .sort_values("total_cost")
    )

    on_time_by_mode = (
        shipments.groupby("mode", as_index=False)["on_time_flag"]
        .mean()
        .sort_values("on_time_flag", ascending=False)
    )

    carrier_perf = (
        shipments.groupby(["carrier_id", "mode"], as_index=False)
        .agg(
            avg_cost=("total_cost", "mean"),
            on_time_rate=("on_time_flag", "mean"),
            shipment_count=("shipment_id", "count"),
        )
        .sort_values(["mode", "carrier_id"])
    )

    anomaly_counts = (
        anomaly_flags.groupby("FLAG_TYPE", as_index=False)["FLAG_ID"]
        .count()
        .rename(columns={"FLAG_ID": "anomaly_count"})
    )
    anomaly_counts["FLAG_TYPE"] = pd.Categorical(
        anomaly_counts["FLAG_TYPE"],
        categories=FLAG_ORDER,
        ordered=True,
    )
    anomaly_counts = anomaly_counts.sort_values("FLAG_TYPE")

    weekly_anomalies = (
        anomaly_flags.assign(
            week_start=anomaly_flags["FLAG_DATE"].dt.to_period("W-SUN").dt.start_time
        )
        .groupby("week_start", as_index=False)["FLAG_ID"]
        .count()
        .rename(columns={"FLAG_ID": "anomaly_count"})
        .sort_values("week_start")
    )

    fuel_window_start = fuel_surcharges["week_start"].min()
    fuel_window_end = fuel_surcharges["week_start"].max()
    weekly_anomalies = weekly_anomalies[
        weekly_anomalies["week_start"].between(fuel_window_start, fuel_window_end)
    ]

    fig = make_subplots(
        rows=4,
        cols=4,
        specs=[
            [{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}],
            [{"type": "xy", "colspan": 2}, None, {"type": "xy", "colspan": 2}, None],
            [{"type": "xy", "colspan": 2}, None, {"type": "xy", "colspan": 2}, None],
            [{"type": "xy", "colspan": 2}, None, {"type": "xy", "colspan": 2}, None],
        ],
        row_heights=[0.16, 0.28, 0.28, 0.28],
        horizontal_spacing=0.08,
        vertical_spacing=0.10,
        subplot_titles=[
            "",
            "",
            "",
            "",
            "Avg Cost / lb by Month",
            "Top 10 Origin States by Freight Spend",
            "On-Time Rate by Mode",
            "Carrier Cost vs Service",
            "Anomaly Count by Type",
            "Weekly Anomaly Count",
        ],
    )

    fig.add_trace(
        go.Indicator(
            mode="number",
            value=total_shipments,
            title={"text": "<b>Total Shipments</b>"},
            number={"valueformat": ",.0f"},
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Indicator(
            mode="number",
            value=total_spend_millions,
            title={"text": "<b>Total Freight Spend</b>"},
            number={"prefix": "$", "suffix": "M", "valueformat": ",.2f"},
        ),
        row=1,
        col=2,
    )
    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=on_time_rate * 100,
            title={"text": "<b>Overall On-Time Rate</b>"},
            number={"suffix": "%", "valueformat": ".1f"},
            delta={
                "reference": 90,
                "relative": False,
                "valueformat": ".1f",
                "increasing": {"color": MODE_COLORS["PARCEL"]},
                "decreasing": {"color": FLAG_COLORS["MOVING_AVG"]},
            },
        ),
        row=1,
        col=3,
    )
    fig.add_trace(
        go.Indicator(
            mode="number",
            value=anomaly_flag_rate * 100,
            title={"text": "<b>Anomaly Flag Rate</b>"},
            number={"suffix": "%", "valueformat": ".1f"},
        ),
        row=1,
        col=4,
    )

    for mode in MODE_ORDER:
        mode_slice = monthly_cost[monthly_cost["mode"] == mode]
        fig.add_trace(
            go.Scatter(
                x=mode_slice["ship_month"],
                y=mode_slice["cost_per_lb"],
                mode="lines+markers",
                name=mode,
                legendgroup=mode,
                line={"color": MODE_COLORS[mode], "width": 3},
                marker={"size": 7},
                hovertemplate=(
                    f"{mode}<br>%{{x|%b %Y}}"
                    "<br>Avg Cost / lb: $%{y:.2f}<extra></extra>"
                ),
            ),
            row=2,
            col=1,
        )

    fig.add_trace(
        go.Bar(
            x=top_states["total_cost"],
            y=top_states["origin_state"],
            orientation="h",
            name="Freight Spend",
            showlegend=False,
            marker={"color": MODE_COLORS["PARCEL"]},
            hovertemplate="%{y}<br>Total Spend: $%{x:,.0f}<extra></extra>",
        ),
        row=2,
        col=3,
    )

    fig.add_trace(
        go.Bar(
            x=on_time_by_mode["mode"],
            y=on_time_by_mode["on_time_flag"],
            name="On-Time Rate",
            showlegend=False,
            marker={
                "color": [MODE_COLORS[mode] for mode in on_time_by_mode["mode"]],
            },
            hovertemplate="%{x}<br>On-Time Rate: %{y:.1%}<extra></extra>",
        ),
        row=3,
        col=1,
    )

    max_bubble = carrier_perf["shipment_count"].max()
    sizeref = (2.0 * max_bubble) / (36.0 ** 2)

    for mode in MODE_ORDER:
        mode_slice = carrier_perf[carrier_perf["mode"] == mode]
        fig.add_trace(
            go.Scatter(
                x=mode_slice["avg_cost"],
                y=mode_slice["on_time_rate"],
                mode="markers",
                name=mode,
                legendgroup=mode,
                customdata=mode_slice[["carrier_id", "shipment_count"]],
                marker={
                    "color": MODE_COLORS[mode],
                    "size": mode_slice["shipment_count"],
                    "sizemode": "area",
                    "sizeref": sizeref,
                    "sizemin": 8,
                    "opacity": 0.85,
                    "line": {"color": "#FFFFFF", "width": 0.8},
                },
                hovertemplate=(
                    "<b>%{customdata[0]}</b>"
                    f"<br>Mode: {mode}"
                    "<br>Avg Cost: $%{x:,.2f}"
                    "<br>On-Time Rate: %{y:.1%}"
                    "<br>Shipments: %{customdata[1]:,.0f}<extra></extra>"
                ),
            ),
            row=3,
            col=3,
        )

    fig.add_trace(
        go.Bar(
            x=anomaly_counts["FLAG_TYPE"].astype(str),
            y=anomaly_counts["anomaly_count"],
            name="Anomaly Count",
            showlegend=False,
            marker={"color": [FLAG_COLORS[flag] for flag in anomaly_counts["FLAG_TYPE"]]},
            hovertemplate="%{x}<br>Flags: %{y:,.0f}<extra></extra>",
        ),
        row=4,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=weekly_anomalies["week_start"],
            y=weekly_anomalies["anomaly_count"],
            mode="lines+markers",
            name="Weekly Anomalies",
            showlegend=False,
            line={"color": FLAG_COLORS["MOVING_AVG"], "width": 3},
            marker={"size": 6},
            hovertemplate="%{x|%b %d, %Y}<br>Flags: %{y:,.0f}<extra></extra>",
        ),
        row=4,
        col=3,
    )

    fig.update_layout(
        title={
            "text": (
                f"<b>{TITLE}</b><br>"
                f"<span style='font-size:14px;color:{MUTED_TEXT};'>{SUBTITLE}</span>"
            ),
            "x": 0.5,
            "xanchor": "center",
            "y": 0.98,
            "yanchor": "top",
        },
        barmode="group",
        height=1200,
        autosize=True,
        paper_bgcolor=PAPER_BG,
        plot_bgcolor=PLOT_BG,
        font={"color": TEXT_COLOR, "family": "Arial, sans-serif"},
        margin={"l": 60, "r": 40, "t": 150, "b": 70},
        legend={
            "orientation": "h",
            "x": 0.5,
            "xanchor": "center",
            "y": 0.86,
            "yanchor": "bottom",
            "bgcolor": "rgba(0,0,0,0)",
        },
        hoverlabel={"bgcolor": "#20253a", "font_color": TEXT_COLOR},
    )

    fig.update_xaxes(
        showgrid=False,
        zeroline=False,
        linecolor=GRID_COLOR,
        tickfont={"color": MUTED_TEXT},
        title_font={"color": MUTED_TEXT},
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor=GRID_COLOR,
        zeroline=False,
        linecolor=GRID_COLOR,
        tickfont={"color": MUTED_TEXT},
        title_font={"color": MUTED_TEXT},
    )

    fig.update_xaxes(tickformat="%b\n%Y", row=2, col=1)
    fig.update_yaxes(tickprefix="$", tickformat=",.2f", title_text="Avg Cost / lb", row=2, col=1)

    fig.update_xaxes(tickprefix="$", tickformat=",.0f", title_text="Total Spend", row=2, col=3)
    fig.update_yaxes(title_text="Origin State", row=2, col=3)

    fig.update_xaxes(title_text="Mode", row=3, col=1)
    fig.update_yaxes(tickformat=".0%", title_text="On-Time Rate", row=3, col=1)

    fig.update_xaxes(tickprefix="$", tickformat=",.0f", title_text="Avg Shipment Cost", row=3, col=3)
    fig.update_yaxes(tickformat=".0%", title_text="On-Time Rate", row=3, col=3)

    fig.update_xaxes(title_text="Flag Type", row=4, col=1)
    fig.update_yaxes(title_text="Flag Count", row=4, col=1)

    fig.update_xaxes(tickformat="%b\n%Y", row=4, col=3)
    fig.update_yaxes(title_text="Flag Count", row=4, col=3)

    for annotation in fig.layout.annotations:
        annotation.font = {"size": 13, "color": TEXT_COLOR}

    section_annotations = [
        ("KPI Cards", 0.94),
        ("Cost Analysis", 0.70),
        ("Carrier Performance", 0.43),
        ("Anomaly Explorer", 0.17),
    ]
    for label, y_pos in section_annotations:
        fig.add_annotation(
            x=0.0,
            y=y_pos,
            xref="paper",
            yref="paper",
            xanchor="left",
            showarrow=False,
            text=f"<b>{label}</b>",
            font={"size": 16, "color": MUTED_TEXT},
        )

    return fig


def main() -> None:
    shipments, fuel_surcharges, anomaly_flags = load_data()
    figure = build_figure(shipments, fuel_surcharges, anomaly_flags)
    figure.write_html(
        str(OUTPUT_FILE),
        include_plotlyjs=True,
        full_html=True,
        default_width="100%",
        default_height="1200px",
        config={"responsive": True},
    )
    print("Dashboard saved to dashboard.html")


if __name__ == "__main__":
    main()
