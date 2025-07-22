
"""
Julyan van der Westhuizen
22/07/25

This script details an example application which uses API endpoints from the service layer.
As a simulation, this application represents the last phase of the data-pipline process: usage
"""

import pandas as pd
import requests

# CONNECTING TO MY LOCAL API

API_ENDPOINT = "http://127.0.0.1:8000/disruption-data?n=10" 

try:
    response = requests.get(API_ENDPOINT)
    data = response.json()
except requests.RequestException as e:
    print("Could not connect to API endpoint")

# LOAD THE DATA INTO A PANDAS DF
df = pd.DataFrame(data)

# Extract lat and long from the nested structure, add to the df using a normal for loop
df["longitude"] = None
df["latitude"] = None
for index in df.index:
    coords = df.at[index, "geography"]["coordinates"]
    df.at[index, "longitude"] = coords[0]
    df.at[index, "latitude"] = coords[1]

print(df[["longitude", "latitude"]])

