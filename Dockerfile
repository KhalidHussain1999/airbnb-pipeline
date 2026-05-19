FROM apache/airflow:3.1.7

USER airflow
RUN pip install --user --no-warn-script-location \
    dbt-snowflake==1.11.5 \
    apache-airflow-providers-snowflake

# Create .dbt folder
RUN mkdir -p /home/airflow/.dbt