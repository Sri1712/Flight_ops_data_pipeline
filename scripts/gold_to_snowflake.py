import pandas as pd
import snowflake.connector
from airflow.hooks.base import BaseHook

def gold_to_snowflake(**context):
    """
    Loads the aggregated flight data from the gold layer into Snowflake.
    Args:
        **context: Airflow context variables.
    """
    execution_date = context['data_interval_start'].strftime("%Y-%m-%d %H:%M:%S")

    gold_file = context["ti"].xcom_pull(
        key="gold_file",
        task_ids="gold_aggregate"
    )

    if not gold_file:
        raise ValueError("Gold file path not found in XCom.")

    # Load the CSV data
    df = pd.read_csv(gold_file)

    # Normalize column names to uppercase
    df.columns = df.columns.str.upper()
    print("Gold CSV columns:", df.columns.tolist())

    # Convert NaN and infinite values to None/SQL NULL
    df = df.astype(object).where(pd.notna(df), None)

    # Connect to Snowflake
    connection = BaseHook.get_connection('flight_snowflake')  # Snowflake connection ID
    conn = snowflake.connector.connect(
        user=connection.login,
        password=connection.password,
        account=connection.extra_dejson.get('account'),
        warehouse=connection.extra_dejson.get('warehouse'),
        database=connection.extra_dejson.get('database'),
        schema=connection.extra_dejson.get('schema'),
        role=connection.extra_dejson.get('role')
    )

    merge_sql = """
    MERGE INTO FLIGHTS.KPI.FLIGHT_KPIS AS target
    USING (
        SELECT 
            TO_TIMESTAMP(%s) AS WINDOW_START,
            %s AS ORIGIN_COUNTRY, 
            %s AS TOTAL_FLIGHTS, 
            %s AS AVG_VELOCITY, 
            %s AS ON_GROUND, 
            %s AS AVG_GEO_ALTITUDE) AS source
    ON target.WINDOW_START = source.WINDOW_START 
        AND target.ORIGIN_COUNTRY = source.ORIGIN_COUNTRY   

    WHEN MATCHED THEN
        UPDATE SET 
            TOTAL_FLIGHTS = source.TOTAL_FLIGHTS,
            AVG_VELOCITY = source.AVG_VELOCITY,
            ON_GROUND = source.ON_GROUND,
            AVG_GEO_ALTITUDE = source.AVG_GEO_ALTITUDE,
            LOAD_TIME = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN
        INSERT (WINDOW_START, ORIGIN_COUNTRY, TOTAL_FLIGHTS, AVG_VELOCITY, ON_GROUND, AVG_GEO_ALTITUDE)
        VALUES (source.WINDOW_START, source.ORIGIN_COUNTRY, source.TOTAL_FLIGHTS, source.AVG_VELOCITY, source.ON_GROUND, source.AVG_GEO_ALTITUDE);
    """ 

    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            cursor.execute(merge_sql, (
                execution_date,
                row['ORIGIN_COUNTRY'],
                int(row['TOTAL_FLIGHTS']),
                row['AVG_VELOCITY'],
                int(row['ON_GROUND']),
                row['AVG_GEO_ALTITUDE']
            ))

    print("Data loaded into Snowflake successfully.")
    conn.close()