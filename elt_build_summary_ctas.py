from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_wau')
    conn = hook.get_conn()
    return conn.cursor()

@task
def run_ctas(database, schema, table, select_sql, primary_key=None):
    cur = return_snowflake_conn()
    try:
        
        sql = f"CREATE OR REPLACE TABLE {database}.{schema}.temp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        
        if primary_key is not None:
            check_sql = f"""
                SELECT {primary_key}, COUNT(1) AS cnt
                FROM {database}.{schema}.temp_{table}
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 1
            """
            cur.execute(check_sql)
            result = cur.fetchone()
            if int(result[1]) > 1:
                raise Exception(f"Primary key uniqueness failed: {result}")

        
        init_sql = f"""
            CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} AS
            SELECT * FROM {database}.{schema}.temp_{table} WHERE 1=0
        """
        cur.execute(init_sql)

        
        swap_sql = f"ALTER TABLE {database}.{schema}.{table} SWAP WITH {database}.{schema}.temp_{table}"
        cur.execute(swap_sql)

    except Exception as e:
        raise


with DAG(
    dag_id='elt_build_summary_ctas',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['ELT', 'CTAS'],
    description='Build session_summary table using CTAS with PK check and swap'
) as dag:

    database = "dev"
    schema = "analytics"
    table = "session_summary"
    primary_key = "sessionId"

    select_sql = """
        SELECT u.userId, u.sessionId, u.channel, s.ts
        FROM dev.raw.user_session_channel u
        JOIN dev.raw.session_timestamp s
        ON u.sessionId = s.sessionId
        WHERE u.sessionId IN (
            SELECT sessionId
            FROM dev.raw.user_session_channel
            GROUP BY sessionId
            HAVING COUNT(*) = 1
        )
    """

    run_ctas(database, schema, table, select_sql, primary_key)
