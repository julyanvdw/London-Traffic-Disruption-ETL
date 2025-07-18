"""
Julyan van der Westhuizen
18/07/25

This scrip runs tests using PyTest to test all TIMS-related integrations
"""

import sys
sys.path.append("../")
import requests
import pytest
from extract import fetch_TIMS
from load import loader
from pipeline_log_manager import shared_logger

# Temporarily disable logger outputs
shared_logger.to_file = False
shared_logger.verbose = False

# EXTRACT INTEGRATIONS

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

# LOAD INTEGRATIONS

def test_db_connection():
    # Checks if we can actually connect to the DB
    conn = loader.connect_to_db()
    assert conn != None
    conn.close()
