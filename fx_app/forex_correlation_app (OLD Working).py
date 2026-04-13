"""
Forex Correlation Streamlit App
Processes two tick-data CSVs (MT5 format), resamples to a chosen timeframe,
filters by date/time range, computes correlation indices, and shows analytics.
Supports side-by-side comparison of two separate analyses.
"""

import io
import math
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# ─────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Forex Correlation Analyser",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# Custom CSS
# ─────────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    /* White background */
    .stApp { background: #ffffff; }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #f0f4f8 0%, #e8edf2 100%);
        border-right: 1px solid #d0d7de;
    }

    /* Header gradient text */
    .gradient-title {
        background: linear-gradient(90deg, #1a73e8, #7c3aed, #e53e3e);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2.4rem;
        font-weight: 700;
        margin-bottom: 0;
    }
    .subtitle {
        color: #5a6472;
        font-size: 1rem;
        margin-top: 4px;
        margin-bottom: 24px;
    }

    /* Section headers */
    .section-header {
        color: #1a73e8;
        font-size: 1.1rem;
        font-weight: 600;
        border-bottom: 2px solid #e8edf2;
        padding-bottom: 6px;
        margin-bottom: 16px;
    }

    /* Upload boxes */
    [data-testid="stFileUploader"] {
        background: #f6f8fa;
        border: 1px dashed #d0d7de;
        border-radius: 10px;
        padding: 8px;
    }

    /* Buttons */
    .stButton > button {
        background: linear-gradient(90deg, #1a73e8, #1557b0);
        color: white;
        border: none;
        border-radius: 8px;
        font-weight: 600;
        padding: 10px 24px;
        transition: all 0.2s;
        width: 100%;
    }
    .stButton > button:hover {
        background: linear-gradient(90deg, #1557b0, #0d47a1);
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(26,115,232,0.35);
    }

    /* Download button */
    .stDownloadButton > button {
        background: linear-gradient(90deg, #0f9d58, #0b8043);
        color: white;
        border: none;
        border-radius: 8px;
        font-weight: 600;
        padding: 10px 24px;
        width: 100%;
    }

    /* Info / warning boxes */
    .info-box {
        background: #e8f0fe;
        border-left: 4px solid #1a73e8;
        border-radius: 6px;
        padding: 12px 16px;
        color: #1a1a2e;
        font-size: 0.9rem;
        margin-bottom: 12px;
    }

    /* Summary table */
    .summary-table {
        width: 100%;
        border-collapse: collapse;
        font-family: 'Inter', sans-serif;
        font-size: 0.95rem;
        margin-bottom: 8px;
    }
    .summary-table th {
        background: #f0f4f8;
        color: #1a1a2e;
        font-weight: 600;
        padding: 10px 14px;
        text-align: left;
        border-bottom: 2px solid #d0d7de;
    }
    .summary-table td {
        padding: 9px 14px;
        border-bottom: 1px solid #e8edf2;
        color: #1a1a2e;
    }
    .summary-table tr:hover { background: #f6f8fa; }
    .summary-table .val {
        font-weight: 700;
        font-size: 1.05rem;
        color: #1a73e8;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

TIMEFRAME_MAP = {
    "10 Seconds":  "10s",
    "30 Seconds":  "30s",
    "1 Minute":    "1min",
    "5 Minutes":   "5min",
    "15 Minutes":  "15min",
    "30 Minutes":  "30min",
    "1 Hour":      "1h",
    "4 Hours":     "4h",
    "1 Day":       "1D",
}


@st.cache_data(show_spinner=False)
def load_tick_csv(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Parse MT5 tick-data CSV (tab-separated, columns: DATE TIME BID ASK ...)."""
    try:
        df = pd.read_csv(
            io.BytesIO(file_bytes),
            sep="\t",
            header=0,
            usecols=lambda c: c.strip("<>") in ("DATE", "TIME", "BID", "ASK"),
            dtype=str,
        )
        df.columns = [c.strip("<>") for c in df.columns]
        df["datetime"] = pd.to_datetime(
            df["DATE"].str.strip() + " " + df["TIME"].str.strip(),
            format="%Y.%m.%d %H:%M:%S.%f",
            errors="coerce",
        )
        df = df.dropna(subset=["datetime"])
        df["BID"] = pd.to_numeric(df["BID"], errors="coerce")
        df["ASK"] = pd.to_numeric(df["ASK"], errors="coerce")
        df = df.dropna(subset=["BID", "ASK"])
        df = df.set_index("datetime").sort_index()
        df["MID"] = (df["BID"] + df["ASK"]) / 2
        return df[["BID", "ASK", "MID"]]
    except Exception as e:
        st.error(f"Error parsing {filename}: {e}")
        return pd.DataFrame()


def resample_to_ohlc(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Resample tick MID prices to OHLC bars."""
    ohlc = df["MID"].resample(rule).ohlc()
    ohlc = ohlc.dropna()
    return ohlc


def compute_correlation_output(
    df1_ohlc: pd.DataFrame,
    df2_ohlc: pd.DataFrame,
    sym1: str,
    sym2: str,
) -> pd.DataFrame:
    """
    Align two OHLC series on their common timestamps and compute:
    - pct_change, cumulative index, spread, position, flip, flip_loss
    """
    common = df1_ohlc.index.intersection(df2_ohlc.index)
    if len(common) == 0:
        return pd.DataFrame()

    c1 = df1_ohlc.loc[common, "close"]
    c2 = df2_ohlc.loc[common, "close"]

    out = pd.DataFrame(index=common)
    out.index.name = "timestamp"
    out[f"{sym1}_price"] = c1.values
    out[f"{sym2}_price"] = c2.values

    out[f"{sym1}_pct_change"] = c1.pct_change().fillna(0) * 100
    out[f"{sym2}_pct_change"] = c2.pct_change().fillna(0) * 100

    out[f"{sym1}_index"] = 1000 * (1 + out[f"{sym1}_pct_change"] / 100).cumprod()
    out[f"{sym2}_index"] = 1000 * (1 + out[f"{sym2}_pct_change"] / 100).cumprod()

    out["index_spread"] = out[f"{sym1}_index"] - out[f"{sym2}_index"]

    out["current_position"] = out["index_spread"].apply(
        lambda s: f"LONG {sym1} / SHORT {sym2}" if s >= 0 else f"SHORT {sym1} / LONG {sym2}"
    )

    pos_shifted = out["current_position"].shift(1)
    out["flip_occurred"] = out["current_position"] != pos_shifted
    out.loc[out.index[0], "flip_occurred"] = False

    spread_shifted = out["index_spread"].shift(1)
    out["flip_loss"] = 0.0
    flip_mask = out["flip_occurred"]
    out.loc[flip_mask, "flip_loss"] = (
        (out.loc[flip_mask, "index_spread"] - spread_shifted[flip_mask]).abs()
    )

    for col in out.columns:
        if out[col].dtype == float:
            out[col] = out[col].round(4)

    return out.reset_index()


def combine_dt(d, t):
    if d is None:
        return None
    if t is None:
        import datetime as _dt
        t = _dt.time(0, 0, 0)
    return pd.Timestamp(str(d) + " " + str(t))


def get_summary_metrics(result: pd.DataFrame) -> dict:
    """Compute summary metrics from a result DataFrame."""
    return {
        "Total Bars":            f"{len(result):,}",
        "Total Flips":           f"{int(result['flip_occurred'].sum()):,}",
        "Total Flip Loss":       f"{float(result['flip_loss'].sum()):.4f}",
        "Max |Spread|":          f"{float(result['index_spread'].abs().max()):.4f}",
        "Avg |Spread|":          f"{float(result['index_spread'].abs().mean()):.4f}",
        "Max Single Flip Loss":  f"{float(result['flip_loss'].max()):.4f}",
    }


def render_summary_table(metrics: dict, label: str = ""):
    """Render summary metrics as an HTML table."""
    title_col = "Metric" if not label else f"Metric"
    rows = ""
    for k, v in metrics.items():
        rows += f'<tr><td>{k}</td><td class="val">{v}</td></tr>\n'
    header = f"<th>{title_col}</th><th>{label if label else 'Value'}</th>"
    html = f'<table class="summary-table"><thead><tr>{header}</tr></thead><tbody>{rows}</tbody></table>'
    st.markdown(html, unsafe_allow_html=True)


def render_comparison_summary_table(m1: dict, m2: dict, label1: str, label2: str):
    """Render two sets of summary metrics side-by-side in one table."""
    rows = ""
    for k in m1:
        rows += f'<tr><td>{k}</td><td class="val">{m1[k]}</td><td class="val">{m2[k]}</td></tr>\n'
    header = f"<th>Metric</th><th>{label1}</th><th>{label2}</th>"
    html = f'<table class="summary-table"><thead><tr>{header}</tr></thead><tbody>{rows}</tbody></table>'
    st.markdown(html, unsafe_allow_html=True)


def run_pipeline(df1, df2, tf_rule, tf_label, dt_start, dt_end, sym1, sym2):
    """Run the full processing pipeline and return result or None."""
    if dt_start:
        df1 = df1[df1.index >= dt_start]
        df2 = df2[df2.index >= dt_start]
    if dt_end:
        df1 = df1[df1.index <= dt_end]
        df2 = df2[df2.index <= dt_end]

    if df1.empty or df2.empty:
        st.error("No data in the selected date/time range.")
        return None

    ohlc1 = resample_to_ohlc(df1, tf_rule)
    ohlc2 = resample_to_ohlc(df2, tf_rule)

    if ohlc1.empty or ohlc2.empty:
        st.error("No OHLC bars generated. Try a larger timeframe.")
        return None

    result = compute_correlation_output(ohlc1, ohlc2, sym1, sym2)
    if result.empty:
        st.error("No overlapping timestamps between the two assets.")
        return None

    return result


def render_charts(result, sym1, sym2, key_suffix=""):
    """Render all charts for a single result set."""

    # ── Correlation chart ──
    st.markdown('<p class="section-header">📈 Asset Index Correlation</p>', unsafe_allow_html=True)

    idx1 = result[f"{sym1}_index"]
    idx2 = result[f"{sym2}_index"]

    fig_idx = go.Figure()
    fig_idx.add_trace(
        go.Scatter(x=result["timestamp"], y=idx1, mode="lines", name=sym1,
                   line=dict(color="#7c3aed", width=1.8))
    )
    fig_idx.add_trace(
        go.Scatter(x=result["timestamp"], y=idx2, mode="lines", name=sym2,
                   line=dict(color="#f97316", width=1.8))
    )
    fig_idx.add_hline(y=1000, line_dash="dot", line_color="#9ca3af", line_width=1)
    fig_idx.update_layout(
        height=460, paper_bgcolor="#ffffff", plot_bgcolor="#fafafa",
        font=dict(color="#1a1a2e", family="Inter"),
        legend=dict(bgcolor="rgba(255,255,255,0.9)", bordercolor="#d0d7de", borderwidth=1,
                    orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=50, b=40), hovermode="x unified",
        yaxis=dict(title="Index (base 1000)", gridcolor="#e8edf2", zeroline=False),
        xaxis=dict(gridcolor="#e8edf2", zeroline=False),
    )
    st.plotly_chart(fig_idx, use_container_width=True, key=f"idx_{key_suffix}")

    # ── Spread ──
    st.markdown('<p class="section-header">📉 Index Spread Over Time</p>', unsafe_allow_html=True)
    flip_rows = result[result["flip_occurred"]]
    fig_spread = go.Figure()
    fig_spread.add_trace(
        go.Scatter(x=result["timestamp"], y=result["index_spread"], mode="lines",
                   name="Index Spread", line=dict(color="#0f9d58", width=1.5),
                   fill="tozeroy", fillcolor="rgba(15,157,88,0.07)")
    )
    fig_spread.add_hline(y=0, line_dash="dash", line_color="#9ca3af", line_width=1)
    fig_spread.add_trace(
        go.Scatter(x=flip_rows["timestamp"], y=flip_rows["index_spread"], mode="markers",
                   name="Flip", marker=dict(color="#ff7b72", size=8, symbol="x"))
    )
    fig_spread.update_layout(
        height=320, paper_bgcolor="#ffffff", plot_bgcolor="#fafafa",
        font=dict(color="#1a1a2e", family="Inter"),
        legend=dict(bgcolor="rgba(255,255,255,0.9)", bordercolor="#d0d7de", borderwidth=1),
        margin=dict(l=60, r=20, t=20, b=40),
        xaxis=dict(gridcolor="#e8edf2"), yaxis=dict(gridcolor="#e8edf2"),
    )
    st.plotly_chart(fig_spread, use_container_width=True, key=f"spread_{key_suffix}")

    # ── Flip Loss ──
    st.markdown('<p class="section-header">💸 Flip Loss Over Time</p>', unsafe_allow_html=True)
    flip_loss_rows = result[result["flip_loss"] > 0]
    fig_fl = go.Figure()
    fig_fl.add_trace(
        go.Bar(x=flip_loss_rows["timestamp"], y=flip_loss_rows["flip_loss"], name="Flip Loss",
               marker_color="#ff7b72", marker_line_color="#ff4444", marker_line_width=0.5)
    )
    fig_fl.update_layout(
        height=280, paper_bgcolor="#ffffff", plot_bgcolor="#fafafa",
        font=dict(color="#1a1a2e", family="Inter"),
        legend=dict(bgcolor="rgba(255,255,255,0.9)", bordercolor="#d0d7de", borderwidth=1),
        margin=dict(l=60, r=20, t=20, b=40),
        xaxis=dict(gridcolor="#e8edf2"), yaxis=dict(gridcolor="#e8edf2", title="Flip Loss"),
        bargap=0.2,
    )
    st.plotly_chart(fig_fl, use_container_width=True, key=f"fl_{key_suffix}")

    # ── Distributions ──
    st.markdown('<p class="section-header">📊 Distribution Analytics</p>', unsafe_allow_html=True)
    col_a, col_b = st.columns(2)
    with col_a:
        fig_hist = px.histogram(result, x="index_spread", nbins=60,
                                title="Index Spread Distribution",
                                color_discrete_sequence=["#1a73e8"], template="plotly_white")
        fig_hist.update_layout(paper_bgcolor="#ffffff", plot_bgcolor="#fafafa",
                               font=dict(color="#1a1a2e", family="Inter"),
                               margin=dict(l=40, r=20, t=40, b=40), height=300)
        st.plotly_chart(fig_hist, use_container_width=True, key=f"hist_{key_suffix}")
    with col_b:
        if not flip_loss_rows.empty:
            fig_fl_hist = px.histogram(flip_loss_rows, x="flip_loss", nbins=40,
                                       title="Flip Loss Distribution",
                                       color_discrete_sequence=["#e53e3e"], template="plotly_white")
            fig_fl_hist.update_layout(paper_bgcolor="#ffffff", plot_bgcolor="#fafafa",
                                      font=dict(color="#1a1a2e", family="Inter"),
                                      margin=dict(l=40, r=20, t=40, b=40), height=300)
            st.plotly_chart(fig_fl_hist, use_container_width=True, key=f"flhist_{key_suffix}")
        else:
            st.info("No flip losses in this range.")

    # ── Position breakdown ──
    st.markdown('<p class="section-header">🔄 Position Breakdown</p>', unsafe_allow_html=True)
    col_p1, col_p2 = st.columns([1, 2])
    with col_p1:
        pos_counts = result["current_position"].value_counts()
        fig_pie = px.pie(values=pos_counts.values, names=pos_counts.index,
                         color_discrete_sequence=["#1a73e8", "#f97316"],
                         hole=0.45, template="plotly_white")
        fig_pie.update_layout(paper_bgcolor="#ffffff", font=dict(color="#1a1a2e", family="Inter"),
                              margin=dict(l=20, r=20, t=20, b=20), height=280,
                              legend=dict(bgcolor="rgba(255,255,255,0.9)", bordercolor="#d0d7de", borderwidth=1))
        st.plotly_chart(fig_pie, use_container_width=True, key=f"pie_{key_suffix}")
    with col_p2:
        result_copy = result.copy()
        result_copy["flip_cumsum"] = result_copy["flip_occurred"].cumsum()
        fig_cumflip = go.Figure()
        fig_cumflip.add_trace(
            go.Scatter(x=result_copy["timestamp"], y=result_copy["flip_cumsum"],
                       mode="lines", name="Cumulative Flips", line=dict(color="#f0883e", width=2))
        )
        fig_cumflip.update_layout(
            title="Cumulative Flips Over Time", height=280,
            paper_bgcolor="#ffffff", plot_bgcolor="#fafafa",
            font=dict(color="#1a1a2e", family="Inter"),
            margin=dict(l=40, r=20, t=40, b=40),
            xaxis=dict(gridcolor="#e8edf2"), yaxis=dict(gridcolor="#e8edf2", title="Cumulative Flips"),
        )
        st.plotly_chart(fig_cumflip, use_container_width=True, key=f"cumflip_{key_suffix}")


def render_data_and_download(result, sym1, sym2, tf_label, key_suffix=""):
    """Render data preview and download button."""
    st.divider()
    st.markdown('<p class="section-header">🗂️ Output Data Preview</p>', unsafe_allow_html=True)
    st.dataframe(result.head(200), use_container_width=True, height=320)

    st.markdown('<p class="section-header">⬇️ Download Output CSV</p>', unsafe_allow_html=True)
    tf_label_safe = tf_label.replace(" ", "_")
    out_filename = f"{sym1}_{sym2}_{tf_label_safe}.csv"
    csv_bytes = result.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=f"💾 Download {out_filename}",
        data=csv_bytes,
        file_name=out_filename,
        mime="text/csv",
        use_container_width=True,
        key=f"dl_{key_suffix}",
    )


# ─────────────────────────────────────────────
# Sidebar – Inputs
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown('<p class="gradient-title" style="font-size:1.4rem;">⚙️ Configuration</p>', unsafe_allow_html=True)

    st.markdown('<p class="section-header">📂 Upload Tick Data (A)</p>', unsafe_allow_html=True)
    file1 = st.file_uploader("Asset 1 CSV", type=["csv"], key="f1")
    sym1 = st.text_input("Asset 1 Symbol", value="XAUUSDm", key="s1")

    file2 = st.file_uploader("Asset 2 CSV", type=["csv"], key="f2")
    sym2 = st.text_input("Asset 2 Symbol", value="USDJPYm", key="s2")

    st.markdown('<p class="section-header">⏱️ Timeframe</p>', unsafe_allow_html=True)
    tf_label = st.selectbox("Resample Timeframe", list(TIMEFRAME_MAP.keys()), index=0)
    tf_rule = TIMEFRAME_MAP[tf_label]

    st.markdown('<p class="section-header">📅 Date / Time Range</p>', unsafe_allow_html=True)
    start_date = st.date_input("Start Date", value=None, key="sd")
    start_time = st.time_input("Start Time", value=None, key="st_time")
    end_date = st.date_input("End Date", value=None, key="ed")
    end_time = st.time_input("End Time", value=None, key="et_time")

    st.divider()

    # ── Comparison toggle ──
    compare_mode = st.toggle("🔀 Enable Comparison (Set B)", value=False, key="compare_toggle")

    if compare_mode:
        st.markdown('<p class="section-header">📂 Upload Tick Data (B)</p>', unsafe_allow_html=True)
        file1_b = st.file_uploader("Asset 1 CSV (B)", type=["csv"], key="f1b")
        sym1_b = st.text_input("Asset 1 Symbol (B)", value="XAUUSDm", key="s1b")
        file2_b = st.file_uploader("Asset 2 CSV (B)", type=["csv"], key="f2b")
        sym2_b = st.text_input("Asset 2 Symbol (B)", value="USDJPYm", key="s2b")

        st.markdown('<p class="section-header">⏱️ Timeframe (B)</p>', unsafe_allow_html=True)
        tf_label_b = st.selectbox("Resample Timeframe (B)", list(TIMEFRAME_MAP.keys()), index=0, key="tf_b")
        tf_rule_b = TIMEFRAME_MAP[tf_label_b]

        st.markdown('<p class="section-header">📅 Date / Time Range (B)</p>', unsafe_allow_html=True)
        start_date_b = st.date_input("Start Date (B)", value=None, key="sd_b")
        start_time_b = st.time_input("Start Time (B)", value=None, key="st_time_b")
        end_date_b = st.date_input("End Date (B)", value=None, key="ed_b")
        end_time_b = st.time_input("End Time (B)", value=None, key="et_time_b")

    st.divider()
    run_btn = st.button("▶ Run Analysis", use_container_width=True)

# ─────────────────────────────────────────────
# Main header
# ─────────────────────────────────────────────
st.markdown('<h1 class="gradient-title">📊 Forex Correlation Analyser</h1>', unsafe_allow_html=True)
st.markdown('<p class="subtitle">Resample tick data · Compute correlation indices · Visualise spreads & flips</p>', unsafe_allow_html=True)

if not run_btn:
    st.markdown(
        """
        <div class="info-box">
        👈 Upload two MT5 tick-data CSV files in the sidebar, choose a timeframe and date/time range, then click <strong>Run Analysis</strong>.
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.stop()

# ─────────────────────────────────────────────
# Validate & Load Set A
# ─────────────────────────────────────────────
if file1 is None or file2 is None:
    st.error("Please upload both CSV files for Set A before running.")
    st.stop()

dt_start = combine_dt(start_date, start_time)
dt_end = combine_dt(end_date, end_time)

with st.spinner("Loading tick data (Set A)…"):
    df1 = load_tick_csv(file1.read(), file1.name)
    df2 = load_tick_csv(file2.read(), file2.name)

if df1.empty or df2.empty:
    st.error("Set A: one or both files could not be parsed.")
    st.stop()

with st.spinner(f"Processing Set A ({tf_label})…"):
    result_a = run_pipeline(df1, df2, tf_rule, tf_label, dt_start, dt_end, sym1, sym2)

if result_a is None:
    st.stop()

# ─────────────────────────────────────────────
# Optionally load Set B
# ─────────────────────────────────────────────
result_b = None
tf_label_b_used = tf_label  # fallback
if compare_mode:
    if file1_b is None or file2_b is None:
        st.warning("Comparison enabled but Set B files not uploaded. Showing Set A only.")
    else:
        dt_start_b = combine_dt(start_date_b, start_time_b)
        dt_end_b = combine_dt(end_date_b, end_time_b)
        tf_label_b_used = tf_label_b

        with st.spinner("Loading tick data (Set B)…"):
            df1_b = load_tick_csv(file1_b.read(), file1_b.name)
            df2_b = load_tick_csv(file2_b.read(), file2_b.name)

        if df1_b.empty or df2_b.empty:
            st.warning("Set B: one or both files could not be parsed. Showing Set A only.")
        else:
            with st.spinner(f"Processing Set B ({tf_label_b})…"):
                result_b = run_pipeline(df1_b, df2_b, tf_rule_b, tf_label_b, dt_start_b, dt_end_b, sym1_b, sym2_b)

# ─────────────────────────────────────────────
# Render — Single or Side-by-Side
# ─────────────────────────────────────────────
metrics_a = get_summary_metrics(result_a)
label_a = f"Set A ({sym1} / {sym2} · {tf_label})"

if result_b is not None:
    metrics_b = get_summary_metrics(result_b)
    label_b = f"Set B ({sym1_b} / {sym2_b} · {tf_label_b_used})"

    # ── Summary table: comparison ──
    st.markdown('<p class="section-header">📌 Summary — Comparison</p>', unsafe_allow_html=True)
    render_comparison_summary_table(metrics_a, metrics_b, label_a, label_b)
    st.divider()

    # ── Side-by-side charts ──
    col_left, col_right = st.columns(2)
    with col_left:
        st.markdown(f"### {label_a}")
        render_charts(result_a, sym1, sym2, key_suffix="a")
        render_data_and_download(result_a, sym1, sym2, tf_label, key_suffix="a")
    with col_right:
        st.markdown(f"### {label_b}")
        render_charts(result_b, sym1_b, sym2_b, key_suffix="b")
        render_data_and_download(result_b, sym1_b, sym2_b, tf_label_b_used, key_suffix="b")
else:
    # ── Summary table: single ──
    st.markdown('<p class="section-header">📌 Summary</p>', unsafe_allow_html=True)
    render_summary_table(metrics_a, label_a)
    st.divider()

    render_charts(result_a, sym1, sym2, key_suffix="a")
    render_data_and_download(result_a, sym1, sym2, tf_label, key_suffix="a")

st.markdown(
    "<br><p style='color:#9ca3af; text-align:center; font-size:0.8rem;'>Forex Correlation Analyser · Built with Streamlit & Plotly</p>",
    unsafe_allow_html=True,
)
