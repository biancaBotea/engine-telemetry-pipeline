FROM apache/airflow:2.7.1

# Install pandas and any other libraries needed for Task 2
RUN pip install --no-cache-dir pandas scipy