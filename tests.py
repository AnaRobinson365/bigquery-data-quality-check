import pytest
from src.dq_check import dq_check

def test_dq_check_valid_data():
    project_id = "test_project"
    dataset_id = "test_dataset"
    table_name = "test_table"
    non_null_fields = ["column1", "column2"]

    # Simulating a valid case where all fields are non-null
    result, total_records, null_count = dq_check(
        project_id=project_id, dataset_id=dataset_id, table_name=table_name, non_null_fields=non_null_fields
    )

    assert result in ["PASS", "FAIL"]
    assert isinstance(total_records, int)
    assert isinstance(null_count, int)

def test_dq_check_missing_params():
    """Ensure it raises an error when required parameters are missing."""
    with pytest.raises(ValueError):
        dq_check()

def test_dq_check_no_nulls():
    """Simulating a case where no nulls exist."""
    project_id = "test_project"
    dataset_id = "test_dataset"
    table_name = "test_table"
    non_null_fields = ["column1"]

    result, total_records, null_count = dq_check(
        project_id, dataset_id, table_name, non_null_fields
    )

    assert result == "PASS"
    assert null_count == 0

