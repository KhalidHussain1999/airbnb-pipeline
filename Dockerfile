FROM apache/airflow:3.1.7

USER airflow
RUN pip install --user --no-warn-script-location \
    dbt-snowflake==1.11.5 \
    apache-airflow-providers-snowflake \
    pandas>=2.0 \
    s3fs>=2024.2 \
    boto3>=1.34

# Create .dbt folder
RUN mkdir -p /home/airflow/.dbt