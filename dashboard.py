import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from sqlalchemy import create_engine, text
import numpy as np

st.set_page_config(
    page_title="Real-Time Stock Trading Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📈 Real-Time Stock Market Trading Dashboard")

DATABASE_URL = "postgresql://stock_user:stock_password@localhost:5432/stock_db"


@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)


engine = get_engine(DATABASE_URL)


def load_trades(symbol_filter=None, limit=500):
    """Load recent trades from database"""
    base_query = "SELECT * FROM trades"
    params = {}

    if symbol_filter and symbol_filter != "All":
        base_query += " WHERE symbol = :symbol"
        params["symbol"] = symbol_filter

    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = limit

    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        if not df.empty and "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        st.error(f"Error loading trades: {e}")
        return pd.DataFrame()


def calculate_technical_indicators(df, symbol):
    """Calculate moving averages and RSI"""
    symbol_df = df[df["symbol"] == symbol].sort_values("timestamp")

    if len(symbol_df) < 20:
        return None

    # Simple Moving Averages
    symbol_df["SMA_10"] = symbol_df["price"].rolling(window=10).mean()
    symbol_df["SMA_20"] = symbol_df["price"].rolling(window=20).mean()

    # RSI Calculation
    delta = symbol_df["price"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    symbol_df["RSI"] = 100 - (100 / (1 + rs))

    return symbol_df


def predict_next_move(df, symbol):
    """Simple prediction based on recent trend"""
    symbol_df = df[df["symbol"] == symbol].sort_values("timestamp").tail(20)

    if len(symbol_df) < 10:
        return "Insufficient Data", 0.5

    recent_prices = symbol_df["price"].values
    trend = np.polyfit(range(len(recent_prices)), recent_prices, 1)[0]

    # Calculate confidence based on price consistency
    volatility = np.std(recent_prices) / np.mean(recent_prices)
    confidence = max(0.5, min(0.95, 1 - volatility * 10))

    if trend > 0.5:
        prediction = "📈 Bullish"
    elif trend < -0.5:
        prediction = "📉 Bearish"
    else:
        prediction = "➡️ Neutral"

    return prediction, round(confidence * 100, 1)


# Sidebar controls
st.sidebar.header("⚙️ Dashboard Controls")

# Get available symbols
try:
    symbols_query = "SELECT DISTINCT symbol FROM trades ORDER BY symbol"
    symbols_df = pd.read_sql_query(text(symbols_query), con=engine.connect())
    available_symbols = ["All"] + symbols_df["symbol"].tolist()
except Exception:
    available_symbols = ["All"]

selected_symbol = st.sidebar.selectbox("Filter by Symbol", available_symbols)
update_interval = st.sidebar.slider("Update Interval (seconds)", 2, 20, 5)
limit_records = st.sidebar.number_input("Records to load", 200, 2000, 500, 100)

show_anomalies = st.sidebar.checkbox("Show Anomalies Only", False)
show_predictions = st.sidebar.checkbox("Show ML Predictions", True)

if st.sidebar.button("🔄 Refresh Now"):
    st.rerun()

# Main dashboard
df_trades = load_trades(selected_symbol, limit=int(limit_records))

if df_trades.empty:
    st.warning("⏳ Waiting for trade data...")
    time.sleep(update_interval)
    st.rerun()

# Filter anomalies if requested
if show_anomalies:
    df_trades = df_trades[df_trades["is_anomaly"]]
    if df_trades.empty:
        st.info("No anomalies detected in current data window")
        time.sleep(update_interval)
        st.rerun()

# === KPI METRICS ===
st.subheader(f"📊 Market Overview ({selected_symbol})")

total_trades = len(df_trades)
total_volume = df_trades["volume"].sum()
total_value = df_trades["trade_value"].sum()
avg_price = df_trades["price"].mean()
anomaly_count = df_trades["is_anomaly"].sum()
anomaly_rate = (anomaly_count / total_trades * 100) if total_trades > 0 else 0

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Total Trades", f"{total_trades:,}")
col2.metric("Total Volume", f"{total_volume:,}")
col3.metric("Trade Value", f"${total_value:,.0f}")
col4.metric("Avg Price", f"${avg_price:.2f}")
col5.metric("Anomalies", f"{anomaly_count}", f"{anomaly_rate:.1f}%")

# Buy/Sell Ratio
buy_count = len(df_trades[df_trades["trade_type"] == "BUY"])
sell_count = len(df_trades[df_trades["trade_type"] == "SELL"])
buy_sell_ratio = buy_count / sell_count if sell_count > 0 else 0
col6.metric("Buy/Sell Ratio", f"{buy_sell_ratio:.2f}")

st.markdown("---")

# === TWO COLUMN LAYOUT ===
left_col, right_col = st.columns([2, 1])

with left_col:
    # Price chart with volume
    st.subheader("💹 Price & Volume Over Time")

    if selected_symbol != "All":
        # Detailed chart for single symbol
        symbol_data = calculate_technical_indicators(df_trades, selected_symbol)

        if symbol_data is not None and len(symbol_data) > 0:
            fig = make_subplots(
                rows=2,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.05,
                row_heights=[0.7, 0.3],
                subplot_titles=(
                    f"{selected_symbol} Price with Moving Averages",
                    "Volume",
                ),
            )

            # Price and MA
            fig.add_trace(
                go.Scatter(
                    x=symbol_data["timestamp"],
                    y=symbol_data["price"],
                    name="Price",
                    line=dict(color="blue", width=2),
                ),
                row=1,
                col=1,
            )
            fig.add_trace(
                go.Scatter(
                    x=symbol_data["timestamp"],
                    y=symbol_data["SMA_10"],
                    name="SMA 10",
                    line=dict(color="orange", width=1, dash="dash"),
                ),
                row=1,
                col=1,
            )
            fig.add_trace(
                go.Scatter(
                    x=symbol_data["timestamp"],
                    y=symbol_data["SMA_20"],
                    name="SMA 20",
                    line=dict(color="red", width=1, dash="dash"),
                ),
                row=1,
                col=1,
            )

            # Volume
            colors = [
                "green" if t == "BUY" else "red" for t in symbol_data["trade_type"]
            ]
            fig.add_trace(
                go.Bar(
                    x=symbol_data["timestamp"],
                    y=symbol_data["volume"],
                    name="Volume",
                    marker_color=colors,
                    opacity=0.6,
                ),
                row=2,
                col=1,
            )

            fig.update_layout(height=500, showlegend=True, hovermode="x unified")
            st.plotly_chart(
                fig,
                use_container_width=True,
                key=f"price_chart_{selected_symbol}",
            )

            # Prediction
            if show_predictions:
                prediction, confidence = predict_next_move(df_trades, selected_symbol)
                st.info(
                    f"🤖 **ML Prediction**: {prediction} (Confidence: {confidence}%)"
                )
    else:
        # Multi-symbol view
        recent_trades = df_trades.sort_values("timestamp").tail(100)
        fig = px.scatter(
            recent_trades,
            x="timestamp",
            y="price",
            color="symbol",
            size="volume",
            hover_data=["trade_type", "exchange"],
            title="All Symbols - Recent Price Action",
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True, key="multi_symbol_chart")

with right_col:
    # Top symbols by volume
    st.subheader("🏆 Top Symbols by Volume")
    top_symbols = (
        df_trades.groupby("symbol")["volume"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    fig_top = px.bar(
        top_symbols,
        orientation="h",
        labels={"value": "Total Volume", "symbol": "Symbol"},
    )
    fig_top.update_layout(height=300, showlegend=False)
    st.plotly_chart(fig_top, use_container_width=True, key="top_symbols_chart")

    # Sector distribution
    st.subheader("🏢 Trading by Sector")
    sector_value = df_trades.groupby("sector")["trade_value"].sum()
    fig_sector = px.pie(values=sector_value.values, names=sector_value.index, hole=0.4)
    fig_sector.update_layout(height=300)
    st.plotly_chart(fig_sector, use_container_width=True, key="sector_chart")

# === BOTTOM SECTION ===
st.markdown("---")

bottom_left, bottom_right = st.columns(2)

with bottom_left:
    st.subheader("📊 Trade Distribution")

    # Trade type distribution
    trade_type_counts = df_trades["trade_type"].value_counts()
    fig_trade_type = px.bar(
        x=trade_type_counts.index,
        y=trade_type_counts.values,
        labels={"x": "Trade Type", "y": "Count"},
        color=trade_type_counts.index,
        color_discrete_map={"BUY": "green", "SELL": "red"},
    )
    fig_trade_type.update_layout(height=250, showlegend=False)
    st.plotly_chart(fig_trade_type, use_container_width=True, key="trade_type_chart")

with bottom_right:
    st.subheader("🔥 Anomaly Detection")

    if anomaly_count > 0:
        anomaly_df = df_trades[df_trades["is_anomaly"]].sort_values(
            "anomaly_score", ascending=False
        )
        st.dataframe(
            anomaly_df[
                ["symbol", "price", "volume", "anomaly_score", "timestamp"]
            ].head(5),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No anomalies detected in current window 🎯")

# Recent trades table
st.subheader("📜 Recent Trades")
display_cols = [
    "timestamp",
    "symbol",
    "price",
    "volume",
    "trade_value",
    "trade_type",
    "exchange",
    "is_anomaly",
]
st.dataframe(
    df_trades[display_cols].head(15), use_container_width=True, hide_index=True
)

# Footer
st.markdown("---")
st.caption(
    f"🕐 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
    f"Auto-refresh: {update_interval}s | "
    f"Total Records: {total_trades:,}"
)

# Auto-refresh mechanism
time.sleep(update_interval)
st.rerun()
