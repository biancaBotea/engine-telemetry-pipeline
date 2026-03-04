import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go
import plotly.express as px
import os

st.set_page_config(page_title="Engine Analytics", layout="wide")

# Database Connection
DB_PATH = 'results/engine_analytics.db'
if not os.path.exists(DB_PATH):
    st.error("⚠️ Database not found! Please run the Airflow pipeline first to generate results.")
    st.stop()

def get_data(query):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql(query, conn)

st.title("Engine Telemetry Analytics")

tab1, tab2, tab3, tab4 = st.tabs([ "Engine Deep-Dive", "Engine Comparison","Error Audit & DQ", "Inspector"])

# Engine Deep-Dive (Correlations & Stats)
with tab1:
    all_engines = get_data("SELECT DISTINCT engine_id FROM cleaned_telemetry")['engine_id'].tolist()
    selected_engine = st.selectbox("Select Engine ID", all_engines)
    
    # Get telemetry and stats for the selected engine
    df_telemetry = get_data(f"SELECT * FROM cleaned_telemetry WHERE engine_id = '{selected_engine}'")
    df_stats = get_data(f"SELECT * FROM engine_stats WHERE engine_id = '{selected_engine}'")

    st.subheader(f"Correlation: {selected_engine} Sensors")

    # Start and end timestamps for the telemetry data
    if not df_telemetry.empty:
        start_time = pd.to_datetime(df_telemetry['timestamp'].min())
        end_time = pd.to_datetime(df_telemetry['timestamp'].max())
        duration = (end_time - start_time).total_seconds() / 60
        st.info(f"**Analysis Window:** {start_time.strftime('%H:%M:%S')} to {end_time.strftime('%H:%M:%S')} ({duration:.1f} Minutes)")
    
    import plotly.subplots as sp

    # Create 4 rows, 1 column
    fig = sp.make_subplots(rows=4, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.05,
                        subplot_titles=("RPM", "Temperature (°C)", "Oil Pressure (Bar)"))

    fig.add_trace(go.Scatter(x=df_telemetry['timestamp'], y=df_telemetry['rpm'], name="RPM"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df_telemetry['timestamp'], y=df_telemetry['temp'], name="Temp"), row=2, col=1)
    fig.add_trace(go.Scatter(x=df_telemetry['timestamp'], y=df_telemetry['oil_pressure'], name="Pressure"), row=3, col=1)
    fig.add_trace(go.Scatter(x=df_telemetry['timestamp'], y=df_telemetry['fuel_consumption'], name="Fuel Consumption"), row=4, col=1)

    fig.update_layout(height=800, title_text=f"Synchronized Telemetry: {selected_engine}", showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Sensor vs. Performance Stats")
    metric_to_plot = st.radio("Select Sensor to view with Min/Max/Mean", ["rpm", "temp", "oil_pressure", "fuel_consumption"], horizontal=True)
    
    if not df_stats.empty:
        stat_prefix = "avg_" if metric_to_plot == "rpm" else "max_"
        fig_stats = px.line(df_telemetry, x='timestamp', y=metric_to_plot, title=f"{metric_to_plot.upper()} with Stat Thresholds")
        fig_stats.add_hline(y=df_stats[f'{metric_to_plot}_mean'].values[0], line_dash="dash", line_color="orange", annotation_text="Mean")
        fig_stats.add_hline(y=df_stats[f'{metric_to_plot}_max'].values[0], line_dash="dot", line_color="red", annotation_text="Max")
        st.plotly_chart(fig_stats, use_container_width=True)


# Engines Comparison
with tab2:
    st.header("Engines Variance Analysis")
    # We use the cleaned_telemetry table to get the full distribution, not just stats
    df_all_telemetry = get_data("SELECT engine_id, rpm, temp, oil_pressure, fuel_consumption FROM cleaned_telemetry")
    
    metric_choice = st.selectbox("Select Metric to Compare Distribution", 
                                 ["rpm", "temp", "oil_pressure", "fuel_consumption"])
    
    fig_box = px.box(df_all_telemetry, 
                     x="engine_id", 
                     y=metric_choice, 
                     color="engine_id",
                     title=f"Distribution of {metric_choice.upper()} across Fleet",
                     points="outliers") # This highlights your 'Chaos' data!
    
    st.plotly_chart(fig_box, use_container_width=True)
    
    st.info("💡 **How to read this:** The 'box' shows where 50% of the data lives. "
            "Dots outside the whiskers are the anomalies caught by your pipeline.")
    
#  Error Audit & Data Quality
with tab3:
    st.header("🛠️ Data Quality & Pipeline Health")
    
    # Load Data
    df_err = get_data("SELECT * FROM validation_errors")
    df_clean = get_data("SELECT engine_id, timestamp FROM cleaned_telemetry")
    
    if not df_err.empty:
        # Fleet-Wide Error Summary
        st.subheader("Fleet-Wide Error Comparison")
        
        err_counts = df_err.groupby('engine_id').size().reset_index(name='error_count')
        
        # Horizontal bar chart for fleet-wide errors
        fig_fleet_err = px.bar(err_counts, 
                               y='engine_id', 
                               x='error_count', 
                               orientation='h',
                               color='error_count',
                               color_continuous_scale="Reds",
                               title="Total Anomalies per Engine",
                               labels={'error_count': 'Number of Errors', 'engine_id': 'Engine ID'})
        
        fig_fleet_err.update_layout(showlegend=False, height=300)
        st.plotly_chart(fig_fleet_err, use_container_width=True)

        st.divider()
        total_errs = len(df_err)
        st.write(f"**Total Fleet Anomalies Caught:** {total_errs}")
        
        st.divider()

        # Engine Selector for Audit
        all_engines = df_clean['engine_id'].unique()
        selected_audit_engine = st.selectbox("Select Engine to Audit", all_engines, key="audit_engine")

        # Filter errors for this specific engine
        engine_errs = df_err[df_err['engine_id'] == selected_audit_engine]
        engine_clean_count = len(df_clean[df_clean['engine_id'] == selected_audit_engine])
        
        # Calculate Engine Health Score
        affected_pct = (len(engine_errs) / engine_clean_count + len(engine_errs[engine_errs['engine_id'] == "Duplicate Timestamp"])) * 100
        health_score = 100 - affected_pct

        # Metrics for the specific engine
        c1, c2, c3 = st.columns(3)
        c1.metric(f"Health: {selected_audit_engine}", f"{health_score:.1f}%")
        c2.metric("Anomalies Found", len(engine_errs))
        c3.metric("Reliability Grade", "A" if health_score > 95 else "B" if health_score > 85 else "C")

        # Visualizing the Engine's Failure Modes
        col_left, col_right = st.columns([1, 2])

        with col_left:
            st.subheader("Error Types")
            if not engine_errs.empty:
                err_type_fig = px.pie(engine_errs, names='error_reason', hole=0.3,
                                        color_discrete_sequence=px.colors.sequential.Reds_r)
                st.plotly_chart(err_type_fig, use_container_width=True)
            else:
                st.success("No errors for this engine!")

        with col_right:
            st.subheader("Anomaly Timeline")
            if not engine_errs.empty:
                # Scatter plot showing exactly WHEN the errors happened
                fig_err_time = px.scatter(engine_errs, x='timestamp', y='error_reason', 
                                          color='error_reason',
                                          title="When did failures occur?",
                                          labels={'error_reason': 'Failure Mode'})
                st.plotly_chart(fig_err_time, use_container_width=True)
            else:
                st.info("Perfect sensor performance recorded.")
            
    else:
        st.success("✅ No validation errors found in the database!")
# Database Inspector
with tab4:
    st.header("Database Table Viewer")
    table_choice = st.selectbox("Select Table to Inspect", 
                                ["cleaned_telemetry", "engine_stats", "validation_errors"])
    df_preview = get_data(f"SELECT * FROM {table_choice} LIMIT 100")
    st.dataframe(df_preview, use_container_width=True)
