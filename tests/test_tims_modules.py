"""
Julyan van der Westhuizen
17/07/25

This scrip runs tests using PyTest to test all TIMS-related modules (unit testing)
"""

import sys
sys.path.append("../")
from unittest.mock import patch, Mock
import tempfile
import os
import json
import pytest
from extract import fetch_TIMS
from datalake_manager import LakeManager
from transform import tims_models, transformer
from load import loader

# EXTRACTING TESTS

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

# Pydantic model tests
def test_Disruption_parsing():
    # Checks if the pydantic model parses JSON as expected (strips what's not defined and keeps what is)

    fake_data = [
        {
            "$type": "Tfl.Api.Presentation.Entities.RoadDisruption, Tfl.Api.Presentation.Entities", 
            "id": "TIMS-214298", 
            "url": "/Road/All/Disruption/TIMS-214298", 
            "point": "[0.119257,51.568495]", 
            "severity": "Serious", 
            "ordinal": 1, 
            "category": "Collisions"
        }
    ]

    disruption = tims_models.Disruption(**fake_data[0])

    assert disruption.tims_id == "TIMS-214298"
    assert disruption.url == "/Road/All/Disruption/TIMS-214298"
    assert disruption.severity == "Serious"
    assert disruption.ordinal == 1
    assert disruption.category == "Collisions"

def test_Disruption_optionality():
    # Checks if the pydantic object still parses even though it doesn't have some of its optional fields

    fake_data = [
        {
            "id": "TIMS-214298", #needs to include
            # lack of other, optional, fields
        }
    ]

    disruption = tims_models.Disruption(**fake_data[0])
    assert disruption.tims_id == "TIMS-214298"
    assert disruption.url == None #properly assigned None to a field not given
    
def test_Disruption_valid_id(): 
    # Checks if the disruption object properly checks valid IDs
    disruption = tims_models.Disruption(id="TIMS-214298")
    assert disruption.tims_id == "TIMS-214298"

def test_Disruption_invalid_id():
    # Checks if the disryption object properly checks for INVALID IDs
    with pytest.raises(Exception):
        disruption = tims_models.Disruption(id="SOME INVALID ID")

def test_Disruption_missing_id():
    # Checks if the parsing fails when the REQUIRED ID is not there
    with pytest.raises(Exception):
        tims_models.Disruption()

def test_invalid_ordinal_type():
    # Checks proper type checking with the example field of ordinal
    with pytest.raises(Exception):
        tims_models.Disruption(id="TIMS-214298", ordinal="stringwhenitshouldbeint")

def test_nested_structure():
    # Checks if the pydantic model correctly parses nested JSON structures as per model def
    segment_data = {"toid": "0"}
    street_data = [{"name": "Buitengracht Street", "segments": [segment_data]}]
    disruption = tims_models.Disruption(id="TIMS-214298", streets=street_data)

    # Check that it worked
    assert disruption.streets[0].name == "Buitengracht Street"
    assert disruption.streets[0].segments[0].toid == "0"

def test_StreetSegment_convert_to_list_validator():
    # Checks that a lineString can be converted into a list of lists of floats
    fake_lineString = "[[0.1, 51.5], [0.2, 51.6]]"
    segment = tims_models.StreetSegment(lineString=fake_lineString)
    assert segment.coords == [[0.1, 51.5], [0.2, 51.6]]

def test_StreetSegment_coords_invalid_string():
    # Checks that non-valid lineStrings just get passed as None
    fake_lineString = 'some invalid lineString'
    segment = tims_models.StreetSegment(lineString=fake_lineString)
    assert segment.coords is None

# TRANSFORM TESTS 

def test_ingestion():
    # Checks that the ingestion method works by creating a fake file, then checking if the output matches 

    with tempfile.TemporaryDirectory() as raw_dir, tempfile.TemporaryDirectory() as transformed_dir:
        # Create a fake raw snapshot file
        test_data = [{"id": "TIMS-214298"}]
        with open(f"{raw_dir}/raw.json", "w") as f:
            json.dump(test_data, f)

        # Creating a stub lakeMakanger which temporary points to tmpdirs
        class TestLakeManager(LakeManager):
            def __init__(self):
                super().__init__()
                self.tims_raw_dir_location = raw_dir
                self.tims_transformed_dir_location = transformed_dir

        # Assigning the temp lakeManager to the one used in the transformer
        transformer.LakeManager = TestLakeManager

        # Run the ingestion method
        transformer.ingest_tims_data()

        # Check that a file was created in transformed_dir and that it matches what we expect
        files = os.listdir(transformed_dir)
        assert len(files) == 1
        with open(f"{transformed_dir}/{files[0]}", "r") as f:
            data = json.load(f)

        # Check the contents of what's returned
        assert data[0]["tims_id"] == "TIMS-214298"

def test_deduplication_ingestion(): 
    # Checks that the ingestion method does indeed perform deduplication
    
    with tempfile.TemporaryDirectory() as raw_dir, tempfile.TemporaryDirectory() as transformed_dir:
        # Create raw data with duplicate entries (duplication on tims ID)
        test_data = [{"id": "TIMS-214298"}, {"id": "TIMS-214298"}, {"id": "TIMS-000111"}]
        with open(f"{raw_dir}/raw.json", "w") as f:
            json.dump(test_data, f)

        class TestLakeManager(LakeManager):
            def __init__(self):
                super().__init__()
                self.tims_raw_dir_location = raw_dir
                self.tims_transformed_dir_location = transformed_dir

        transformer.LakeManager = TestLakeManager
        transformer.ingest_tims_data()

        files = os.listdir(transformed_dir)
        assert len(files) == 1
        with open(f"{transformed_dir}/{files[0]}", "r") as f:
            data = json.load(f)

        # Only two (out of the three) entries should be present after deduplication
        assert len(data) == 2
        assert data[0]["tims_id"] == "TIMS-214298"
        assert data[1]["tims_id"] == "TIMS-000111"

# LOAD TESTS

def test_flatten_disruption():
    # Checks that the flattening logic works properly

    # The loader script obtains disruptions in python dict/list format via LakeManager
    disruption = {
        "tims_id": "TIMS-214298",
        "severity": "Serious",
        "streets": [
            {
                "name": "Buitengracht Street",
                "closure": "closed",
                "directions": "Both ways",
                "segments": [{"toid": "1"}]
            }
        ],
        "isProvisional": True,
        "hasClosures": False
    }

    disruption_row, street_rows = loader.flatten_disruption(disruption)


    # Check disruption_row fields
    assert disruption_row[0] == "TIMS-214298"
    assert disruption_row[3] == "Serious"
    assert disruption_row[-2] == True
    assert disruption_row[-1] == False

    # Check street_rows
    assert len(street_rows) == 1
    assert street_rows[0][2] == "Buitengracht Street"
    assert street_rows[0][3] == "closed"
    assert street_rows[0][4] == "Both ways"
    assert '"toid": "1"' in street_rows[0][5]  # segments JSON

