import json
import pandas as pd
from pathlib import Path

def run_silver_transform(**context):
    """
    Transforms the ingested flight data from JSON to CSV format.
    Args:
        **context: Airflow context variables.
    """
    execution_date = context['ds_nodash']  # Get the execution date from the context

    bronze_file = context["ti"].xcom_pull(
        key="bronze_file",
        task_ids="bronze_ingest"
    )

    if not bronze_file:
        raise ValueError("Bronze file path not found in XCom.")

    silver_file_path = Path("/opt/airflow/data/silver/")
    silver_file_path.mkdir(parents=True, exist_ok=True)

    # Load the JSON data
    with open(bronze_file,"r") as f:
        data = json.load(f)

    df_raw = pd.DataFrame(data['states'])

    # Define the column names based on the OpenSky API documentation
    columns = [
        "icao24", "callsign", "origin_country", "time_position", "last_contact",
        "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
        "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
        "spi", "position_source"
    ]

    df_raw = pd.DataFrame(data['states'], columns=columns)

    df = df_raw[
        [
            "icao24",
            "origin_country",
            "velocity",
            "on_ground",
            "geo_altitude"
        ]
    ]

    output_file = silver_file_path / f"flights_{execution_date}.csv"
    df.to_csv(output_file, index=False)

    context["ti"].xcom_push(
        key="silver_file", 
        value=str(output_file)
        )

    print("Silver transformation completed successfully. Output file:", output_file)