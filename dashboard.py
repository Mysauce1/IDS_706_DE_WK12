import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text


st.set_page_config(page_title="Ride-Sharing Trips Dashboard", layout="wide")
st.title("Real-Time Ride-Sharing Trips Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"


@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)


engine = get_engine(DATABASE_URL)


def load_data(status_filter: str | None = None, limit: int = 200) -> pd.DataFrame:
    base_query = "SELECT * FROM trips"
    params = {}
    if status_filter and status_filter != "All":
        base_query += " WHERE status = :status"
        params["status"] = status_filter
    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = limit
    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


status_options = ["All", "Completed", "Cancelled", "Ongoing"]
selected_status = st.sidebar.selectbox("Filter by Status", status_options)

update_interval = st.sidebar.slider("Update Interval (seconds)", 2, 20, 5)
limit_records = st.sidebar.number_input("Number of records to load", 50, 2000, 200, 50)

if st.sidebar.button("Refresh now"):
    st.rerun()


placeholder = st.empty()
df_trips = load_data(selected_status, limit=int(limit_records))

with placeholder.container():

    if df_trips.empty:
        st.warning("No records found. Waiting for data...")
        time.sleep(update_interval)
        st.rerun()

    df_trips["timestamp"] = pd.to_datetime(df_trips["timestamp"])

    total_trips = len(df_trips)
    total_revenue = df_trips["fare"].sum()
    avg_fare = total_revenue / total_trips if total_trips > 0 else 0
    avg_rating = df_trips["passenger_rating"].mean() if total_trips > 0 else 0
    completed = len(df_trips[df_trips["status"] == "Completed"])
    cancelled = len(df_trips[df_trips["status"] == "Cancelled"])
    conversion_rate = completed / total_trips * 100 if total_trips > 0 else 0

    st.subheader(f"Displaying {total_trips} trips (Filter: {selected_status})")
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Trips", total_trips)
    k2.metric("Total Revenue", f"${total_revenue:,.2f}")
    k3.metric("Average Fare", f"${avg_fare:,.2f}")
    k4.metric("Avg Passenger Rating", f"{avg_rating:.1f}/5")
    k5.metric("Cancelled Trips", cancelled)

    st.markdown("### Raw Data (Top 10)")
    st.dataframe(df_trips.head(10), width="stretch")

    chart_col1, chart_col2 = st.columns(2)

    vehicle_grouped = df_trips.groupby("vehicle_type")["fare"].sum().reset_index()
    fig_vehicle = px.bar(
        vehicle_grouped, x="vehicle_type", y="fare", title="Revenue by Vehicle Type"
    )
    chart_col1.plotly_chart(fig_vehicle, width="stretch", key="vehicle_chart")

    city_grouped = df_trips.groupby("city")["trip_id"].count().reset_index()
    fig_city = px.pie(
        city_grouped, values="trip_id", names="city", title="Trips by City"
    )
    chart_col2.plotly_chart(fig_city, width="stretch", key="city_chart")

    weather_grouped = df_trips.groupby("weather")["trip_id"].count().reset_index()
    fig_weather = px.bar(
        weather_grouped, x="weather", y="trip_id", title="Trips by Weather"
    )
    st.plotly_chart(fig_weather, width="stretch", key="weather_chart")

    st.markdown("---")
    st.caption(
        f"Last updated: {datetime.now().isoformat()} • Auto-refresh every {update_interval}s"
    )


time.sleep(update_interval)
st.rerun()
