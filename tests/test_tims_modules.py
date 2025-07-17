"""
Julyan van der Westhuizen
17/07/25

This scrip runs tests using PyTest to test all TIMS related modules
"""

import pytest
import sys
sys.path.append("../")
from unittest.mock import patch, Mock
import requests

from extract import fetch_TIMS

# EXTRACTING TESTS

def test_TIMS_connection():
     # Check if we can connect to the TIMS API
    API_ENDPOINT = fetch_TIMS.API_ENDPOINT
    query_params = fetch_TIMS.query_params

    response = requests.get(API_ENDPOINT, params=query_params, timeout=10) #prevents the test form hanging
    assert response.status_code == 200

def test_TIMS_json_format():
    # Check if the API returns json
    API_ENDPOINT = fetch_TIMS.API_ENDPOINT
    query_params = fetch_TIMS.query_params

    response = requests.get(API_ENDPOINT, params=query_params, timeout=10) #prevents the test form hanging

    try:
        data = response.json()
    except ValueError:
        pytest.fail("API Response not in JSON format")

def test_fetch_TIMS_snapshot_writing():
    # Check if the json data is being written to the file system
    # note: usage of mock to avoid test-writes in directory

    with patch("extract.fetch_TIMS.requests.get") as mock_get, patch("extract.fetch_TIMS.LakeManager") as mock_lake_manager:

        mock_response = Mock()
        mock_response.json.return_value = [{"id": "TIMS-206772"}]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        mock_manager_instance = mock_lake_manager.return_value
        mock_manager_instance.write_TIMS_raw_snapshot = Mock()

        fetch_TIMS.fetch_tims_data()

        mock_manager_instance.write_TIMS_raw_snapshot.assert_called_once_with([{"id": "TIMS-206772"}])

    
