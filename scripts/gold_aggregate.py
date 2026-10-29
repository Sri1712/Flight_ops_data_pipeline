import pandas as pd
from pathlib import Path

def run_gold_aggregate(**context):
    """
    Aggregates the transformed flight data to compute average velocity and altitude per country.
    Args:
        **context: Airflow context variables.
    """
    execution_date = context['ds_nodash']  # Get the execution date from the context

    silver_file = context["ti"].xcom_pull(
        key="silver_file",
        task_ids="silver_transform"
    )

    if not silver_file:
        raise ValueError("Silver file path not found in XCom.")

    gold_file_path = Path("/opt/airflow/data/gold/")
    gold_file_path.mkdir(parents=True, exist_ok=True)

    # Load the CSV data
    df = pd.read_csv(silver_file)

    # Group by origin_country and calculate average velocity and altitude
    df_aggregated = df.groupby('origin_country').agg(
        total_flights=('icao24', 'count'),
        avg_velocity=('velocity', 'mean'),
        on_ground=('on_ground', 'sum'),
        avg_geo_altitude=('geo_altitude', 'mean')
    ).reset_index()

    output_file = gold_file_path / f"flights_aggregated_{execution_date}.csv"
    df_aggregated.to_csv(output_file, index=False)

    context["ti"].xcom_push(
        key="gold_file", 
        value=str(output_file)
        )

    print("Gold aggregation completed successfully. Output file:", output_file)