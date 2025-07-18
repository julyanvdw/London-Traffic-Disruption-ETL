"""
Julyan van der Westhuizen
17/07/25

This scrip runs tests using PyTest to test all TIMS related modules
"""

import sys
sys.path.append("../")
from unittest.mock import patch, Mock
import requests
import tempfile
import os
import json
import pytest
from extract import fetch_TIMS
from datalake_manager import LakeManager
from tranform import tims_models

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

# data_manager TESTS

def test_LakeManager_write_TIMS_raw_snapshot():
    # Checks if LakeManager properly writes raw snapshots to a real file (in a temp dir)
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = LakeManager()
        manager.tims_raw_dir_location = tmpdir  #setting it up so that it uses a fake temporary dir

        test_data = [{"id": "TIMS-206772"}]
        manager.write_TIMS_raw_snapshot(test_data)

        # check that the file was created and that the contents are the same as that of the test data
        files = os.listdir(tmpdir)
        assert len(files) == 1
        filepath = tmpdir + "/" + files[0]
        with open(filepath, "r") as f:
            data = json.load(f)
        assert data == test_data

def test_LakeManager_read_TIMS_raw_snapshot():
    # Checks if LakeManager can properly read the contents of a specified directory and return the appropriate data
    with tempfile.TemporaryDirectory() as tmpdir: 
        # create fake file to read

        test_data = [{"id": "TIMS-206772"}]

        with open(f"{tmpdir}/fake.json", "w") as f:
            json.dump(test_data, f)
        
        # Setup manager
        manager = LakeManager()
        manager.tims_raw_dir_location = tmpdir
        data = manager.read_TIMS_raw_snapshot()

        assert len(data) == 1
        assert data[0] == test_data

def test_LakeManager_write_TIMS_transformed_snapshot():
    # Checks if the LakeManager properly writes transformed snapshots to a real file (in a temp dir)
    with tempfile.TemporaryDirectory() as tmpdir: 
        manager = LakeManager()
        manager.tims_transformed_dir_location = tmpdir


        # create some test data
        test_item = {"id": "TIMS-206772"}
        disruption = tims_models.Disruption(**test_item)
        test_data = [disruption]
        manager.write_TIMS_transformed_snapshot(test_data)

        # check if the writing was successful
        files = os.listdir(tmpdir)
        assert len(files) == 1 #only one file written
        filepath = tmpdir + "/" + files[0]
        with open(filepath, "r") as f:
            data = json.load(f)
            assert len(data) == 1
        assert data == [test_data[0].model_dump()]

def test_LakeManager_read_TIMS_transformed_snapshot():
    # Checks if the LakeManager properly reads the transformed snapshots
    with tempfile.TemporaryDirectory() as tmpdir:
        # setup manager
        manager = LakeManager()
        manager.tims_transformed_dir_location = tmpdir

        # Create a fake pydantic object
        disruption = tims_models.Disruption(id="TIMS-206772")

        # write some fake data for it to read
        with open(f"{tmpdir}/fake.json", "w") as f:
            json.dump([disruption.model_dump()], f, indent=2, default=str)

        # test the function
        data = manager.read_TIMS_transformed_snapshot()
        assert len(data) == 1
        assert data[0] == [disruption.model_dump()]
