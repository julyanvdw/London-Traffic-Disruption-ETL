"""
Julyan van der Westhuizen
22/07/25

This script details an example application which uses API endpoints from the service layer.
As a simulation, this application represents the last phase of the data-pipline process: usage

For this example application, we'll be plotting disruptions in London over a period of time. We'll also do some stats at each point.
"""

import pandas as pd
import requests
import plotly.express as px
from dotenv import load_dotenv #type: ignore
import os

# CONNECTING TO MY LOCAL API
# Load the value from the .env file (note: there should be a .env file in the root dir)
load_dotenv()

API_ENDPOINT = os.getenv("PIPELINE_API_ENDPOINT")

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


# Use startDateTime for animation instead of snapshot_time
df["startdatetime"] = pd.to_datetime(df["startdatetime"])
df["start_minute"] = df["startdatetime"].dt.strftime('%Y-%m-%d %H:%M')

# PLOT ANIMATION USING PLOTLY WITH TIMELINE ANIMATION

# Fields to show in the popups
hover_data=[
    "tims_id", "startdatetime", "severity", "category", "subcategory", "comments",
    "currentupdate", "levelofinterest", "location", "status"
]


fig = px.scatter_mapbox(
    df,
    lat="latitude",
    lon="longitude",
    hover_data=hover_data,
    animation_frame="start_minute",
    zoom=10,
    height=800,
    mapbox_style="open-street-map"
)

# Create a static figure with all points - toggled by the btn
static_fig = px.scatter_mapbox(
    df,
    lat="latitude",
    lon="longitude",
    hover_data=hover_data,
    zoom=10,
    height=700,
    mapbox_style="open-street-map"
)

fig.update_traces(marker=dict(size=15, opacity=0.8))
fig.update_layout(
    title={
        'text': "London Traffic Disruptions Over Time",
        'y':0.95,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'
    })

# CALCULATE STATISTICS FOR THIS API PULL - add as annotation above map

stats = []
total_unique = df['tims_id'].nunique()
stats.append("Total unique disruptions: " + str(total_unique))

stats.append("Disruptions' status:")
status_counts = df['status'].value_counts()
for i in status_counts.index:
    stats.append(str(i) + ": " + str(status_counts[i]))

stats.append("Disruptions per category:")
category_counts = df['category'].value_counts()
for k in category_counts.index:
    stats.append(str(k) + ": " + str(category_counts[k]))

first_snapshot = df['snapshot_time'].min()
last_snapshot = df['snapshot_time'].max()
stats.append("First snapshot time: " + str(first_snapshot))
stats.append("Last snapshot time: " + str(last_snapshot))
stats_text = '<br>'.join(stats) #for the plotly displaying

fig.add_annotation(
    text=stats_text,
    xref="paper", yref="paper",
    x=0, y=0.9, showarrow=False,  
    align="left",
    font=dict(size=14),
    bordercolor="black",
    borderwidth=1,
    bgcolor="white",
    opacity=0.8
)
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":180})  

# UPDATING THE LAYOUT TO SUPPORT BOTH ANIMATION / STATIC

# Add a button to toggle between animation and all points
fig.update_layout(
    updatemenus=[
        dict(
            type="buttons",
            direction="right",
            x=0.7,
            y=1.1,
            showactive=True,
            buttons=list([
                dict(
                    label="Animate",
                    method="animate",
                    args=[None, {"frame": {"duration": 500, "redraw": True}, "fromcurrent": True}]
                ),
                dict(
                    label="Show All",
                    method="update",
                    args=[
                        {
                            "lat": [df["latitude"]],
                            "lon": [df["longitude"]],
                            "marker": [{"size": 15, "opacity": 0.8}],
                            "customdata": [df[hover_data].values]
                        }
                    ]
                )
            ]),
        )
    ]
)

fig.show()

