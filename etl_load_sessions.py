from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='etl_load_sessions_hook',
    default_args=default_args,
    schedule_interval="0 2 * * *",
    catchup=False,
    description='ETL using SnowflakeHook to create tables, stage, and load data',
    tags=['etl', 'snowflake'],
) as dag:

    @task
    def setup_and_load_all():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_wau')
        cur = hook.get_conn().cursor()

        try:
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dev.raw.user_session_channel (
                    userId INT NOT NULL,
                    sessionId VARCHAR(32) PRIMARY KEY,
                    channel VARCHAR(32) DEFAULT 'direct'
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS dev.raw.session_timestamp (
                    sessionId VARCHAR(32) PRIMARY KEY,
                    ts TIMESTAMP
                );
            """)

            
            cur.execute("""
                CREATE OR REPLACE STAGE dev.raw.blob_stage
                URL = 's3://s3-geospatial/readonly/'
                FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
            """)

            
            cur.execute("""
                COPY INTO dev.raw.user_session_channel
                FROM @dev.raw.blob_stage/user_session_channel.csv;
            """)

            cur.execute("""
                COPY INTO dev.raw.session_timestamp
                FROM @dev.raw.blob_stage/session_timestamp.csv;
            """)

        except Exception as e:
            logging.error(f"ETL failed: {str(e)}")
            raise

    setup_and_load_all()
