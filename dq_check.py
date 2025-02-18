from pathlib import Path
import re
import logging
from google.cloud import bigquery
from airflow.models import Variable  # Optional, used only when integrating with Airflow

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get DAG ID from filename
DAG_ID = Path(__file__).stem
current_version = re.findall(r"v\d*", DAG_ID)[0] if re.findall(r"v\d*", DAG_ID) else "unknown_version"


def dq_check(
    project_id=None,
    dataset_id=None,
    table_name=None,
    non_null_fields=None,
    use_airflow=False
):
    """
    Executes a data quality check in BigQuery by identifying NULL values in key fields.
    
    Parameters:
    - project_id (str): The GCP project ID.
    - dataset_id (str): The BigQuery dataset containing the table.
    - table_name (str): The BigQuery table to check.
    - non_null_fields (list): A list of column names that should not contain NULL values.
    - use_airflow (bool): If True, retrieves parameters from Airflow variables.

    Returns:
    - (str, int, int): The result status ("PASS"/"FAIL"), total record count, and null count.
    """

    # Retrieve parameters from Airflow if enabled
    if use_airflow:
        project_id = Variable.get("project_id")
        dataset_id = Variable.get("dataset_id")
        table_name = Variable.get("table_name")
        non_null_fields = Variable.get("non_null_fields").split(",")

    # Ensure required parameters are set
    if not all([project_id, dataset_id, table_name, non_null_fields]):
        raise ValueError("Missing required parameters. Ensure project_id, dataset_id, table_name, and non_null_fields are set.")

    client = bigquery.Client()
    
    try:
        # Construct query to count total records and NULL values in specified fields
        query = f'''SELECT total_records, null_count,
                    (CASE WHEN null_count = 0 THEN "PASS" ELSE "FAIL" END) AS result
                    FROM (
                        SELECT COUNT(*) AS total_records,
                        COUNT(TRIM(BOTH FROM COALESCE({','.join(non_null_fields)}, ''))) AS null_count
                        FROM `{project_id}.{dataset_id}.{table_name}`,
                        UNNEST(ARRAY[{','.join(non_null_fields)}]) AS non_null_fields)'''
        
        response = client.query(query)

        # Check for complete query failure
        if response.errors is not None:
            raise Exception(f"{response.errors[0].get('reason')}: {response.errors[0].get('message')}")

        
        if response.done():
            # should be one or none records coming back
            for r in response:
                result = r.result
                null_count = r.null_count
                total_records = r.total_records
                logger.info(f"DQ Check Result: {result}, Null Count: {null_count}, Total Records: {total_records}")
                print(f"DQ Check Result:, Null Count: {null_count}, Total Records: {total_records}")   

            return result, total_records, null_count

    except Exception as e:
        logger.error(f"Error performing DQ Check for {table_name}: {str(e)}")
        raise

