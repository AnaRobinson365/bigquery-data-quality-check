# bigquery-data-quality-check

Overview
This project ensures data integrity in BigQuery tables by automating data validation checks. It detects missing, incomplete, or improperly formatted records that could affect data pipelines and reporting accuracy.

The system executes SQL-based data quality checks and integrates with Apache Airflow for scheduling and automation. If missing or incorrect data is found, it logs issues and triggers alerts to notify teams of potential problems.

Key Features
- Automated Data Validation  
  - Detects missing or null values in key fields  
  - Flags data inconsistencies affecting pipelines  
- Integration with Apache Airflow  
  - Automates execution through DAGs for scheduled data checks  
- Error Logging & Alerts  
  - Captures failures and sends notifications for real-time issue tracking  
- Scalability & Configurability  
  - Supports multiple datasets with custom validation rules  


How It Works
1. Data Validation Query Execution  
   - Runs SQL queries in BigQuery to check for null values and missing records 
2. Optional Airflow Integration  
   - The function can be used as part of an Airflow DAG for automated execution  
   - Airflow can schedule validation checks and trigger alert
2. Airflow Integration (Optional)
   - This function was originally designed to be used in an Airflow DAG for automated execution.
   - When executed inside Airflow, the function retrieves parameters from Airflow Variables instead of requiring direct input.
3. Error Handling & Logging  
   - Logs results to GCP Logging and external monitoring systems  
   - Triggers alerts when validation thresholds are exceeded  


Repository Structure
bigquery-data-quality-check/
│── src/
│   ├── dq_check.py          # Main script for running data quality checks
│   ├── airflow_dag.py       # Airflow DAG for automated execution
│── tests/
│   ├── test_dq_check.py     # Unit tests for dq_check.py
│── README.md                # Project documentation
│── requirements.txt         # Dependencies


Installation & Usage
Clone the repository:
git clone https://github.com/yourusername/bigquery-data-quality-check.git
cd bigquery-data-quality-check

Install dependencies:
pip install -r requirements.txt

Run the data quality check manually:
python src/dq_check.py

Run unit tests:
pytest tests/

Example Usage
from dq_check import dq_check

Define table and fields to check
project_id = "your_project"
dataset_id = "your_dataset"
table_name = "your_table"
non_null_fields = ["column1", "column2"]

Run Data Quality Check
result, total_records, null_count = dq_check(project_id, dataset_id, table_name, non_null_fields)

print(f"Result: {result}, Total Records: {total_records}, Null Count: {null_count}")

Technology Stack
- Python → Scripting and automation  
- BigQuery → SQL-based data validation  
- Apache Airflow → Workflow orchestration  
- GCP Logging & Monitoring → Alerting and error tracking

Future Enhancements  
- Extend validation rules to support schema drift detection  
- Integrate machine learning-based anomaly detection  
- Implement a self-healing mechanism for automated remediation  
