from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import os
from utils.config import THRESHOLDS

DATA_DIR = '/opt/airflow/engine_data'
DB_PATH = '/opt/airflow/results/engine_analytics.db'



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'engine_telemetry_pipeline',
    default_args=default_args,
    description='ETL pipeline for Engine IoT data',
    schedule_interval='@hourly', 
    catchup=False
)

def process_data():
    print("DEBUG: Starting data processing...")

    if not os.path.exists(DATA_DIR):
        print(f"ERROR: {DATA_DIR} not found")
        return
        
    files = [f for f in os.listdir(DATA_DIR) if f.lower().endswith('.csv')]
    if not files:
        print("No data files found.")
        return
    
    all_data = [pd.read_csv(os.path.join(DATA_DIR, f)) for f in files]
    raw_df = pd.concat(all_data, ignore_index=True)
    error_logs = []

    # Deduplication
    duplicate_mask = raw_df.duplicated(subset=['engine_id', 'timestamp'], keep='first')
    if duplicate_mask.any():
        dupes = raw_df[duplicate_mask].copy()
        dupes['error_reason'] = 'Duplicate Timestamp'
        error_logs.append(dupes)
    
    df = raw_df[~duplicate_mask].copy()

    # Range Cleaning
    for col, limits in THRESHOLDS.items():
        if col not in df.columns: continue
        
        invalid_mask = (df[col] < limits['min']) | (df[col] > limits['max']) | (df[col].isna())
        
        if invalid_mask.any():
            errs = df[invalid_mask].copy()
            errs['error_reason'] = f'Invalid {col} value'
            error_logs.append(errs)
        
            means = df.groupby('engine_id')[col].transform('mean')
            df[col] = df[col].mask(invalid_mask, means)

    # Final Aggregation 
    final_errors_df = pd.concat(error_logs, ignore_index=True) if error_logs else pd.DataFrame()

    # Analytics
    stats_df = df.groupby('engine_id').agg({
        'rpm': ['mean', 'median', 'min', 'max'],
        'temp': ['mean', 'median', 'min', 'max'],
        'oil_pressure': ['mean', 'median', 'min', 'max'],
        'fuel_consumption': ['mean', 'median', 'min', 'max']
    })
    stats_df.columns = ['_'.join(col).strip() for col in stats_df.columns.values]
    stats_df = stats_df.reset_index()

    # Storage
    conn = sqlite3.connect(DB_PATH)
    df.to_sql('cleaned_telemetry', conn, if_exists='replace', index=False)
    final_errors_df.to_sql('validation_errors', conn, if_exists='replace', index=False)
    stats_df.to_sql('engine_stats', conn, if_exists='replace', index=False)
    conn.commit()
    
    # Quick Check
    row_count = pd.read_sql("SELECT count(*) FROM cleaned_telemetry", conn).iloc[0,0]
    print(f"Verification: DB now contains {row_count} rows.")
    conn.close()

with dag:
    process_task = PythonOperator(
        task_id='process_engine_data',
        python_callable=process_data
    )