import requests
import json
from datetime import datetime
from pathlib import Path

URL = "https://opensky-network.org/api/states/all"

def run_bronze_ingest(**context):
    """
    Ingests flight data from the OpenSky Network API and saves it to a JSON file.

    Args:
        **context: Airflow context variables. """
    
    response = requests.get(URL,timeout=60)
    response.raise_for_status()  # Raise an error for bad responses

    data = response.json()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    file_path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")

    with open(file_path, "w") as f:
        json.dump(data, f)
    
    context["ti"].xcom_push(key="bronze_file", value=str(file_path))

