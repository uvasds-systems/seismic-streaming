import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import os
import pandas as pd
from datetime import datetime

# Initialize the Dash app
app = dash.Dash(__name__)

# Path to CSV file (relative to this script)
## CSV_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "seismic.csv")
CSV_FILE = "seismic.csv"

# App layout
app.layout = html.Div([
    html.H1("Real-Time Seismic Activity", 
            style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': 20}),
    
    html.Div(id='last-update', 
             style={'textAlign': 'center', 'marginBottom': 10, 'color': '#7f8c8d'}),
    
    html.Div(id='event-count',
             style={'textAlign': 'center', 'marginBottom': 20, 'fontSize': 18}),
    
    dcc.Graph(id='earthquake-map', 
              style={'height': '80vh'}),
    
    # Interval component for updates (updates every 5 seconds)
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # in milliseconds (5 seconds)
        n_intervals=0
    )
], style={'fontFamily': 'Arial, sans-serif', 'padding': '20px'})

# Callback to update the map
@app.callback(
    [Output('earthquake-map', 'figure'),
     Output('last-update', 'children'),
     Output('event-count', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_map(n):
    try:
        # Read CSV file
        df = pd.read_csv(CSV_FILE)
        
        # Map CSV columns to expected field names
        # CSV has: unid, time, mag, flynn_region, longitude, latitude, depth
        # Expected: latitude, longitude, magnitude, depth, datetime, location_name
        df = df.rename(columns={
            'time': 'datetime',
            'mag': 'magnitude',
            'flynn_region': 'location_name'
        })
        
        # Ensure required columns exist and are numeric where needed
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['magnitude'] = pd.to_numeric(df['magnitude'], errors='coerce')
        df['depth'] = pd.to_numeric(df['depth'], errors='coerce')
        
        # Drop rows with missing essential data
        df = df.dropna(subset=['latitude', 'longitude', 'magnitude'])
        
        # Prepare data for plotting
        lats = df['latitude'].tolist()
        lons = df['longitude'].tolist()
        magnitudes = df['magnitude'].tolist()
        
        # Create colors based on magnitude
        colors = []
        hover_texts = []
        
        for idx, row in df.iterrows():
            mag = row['magnitude']
            
            # Color based on magnitude
            if mag < 3.0:
                color = 'yellow'
            elif mag < 5.0:
                color = 'orange'
            elif mag < 7.0:
                color = 'red'
            else:
                color = 'darkred'
            colors.append(color)
            
            # Create hover text
            location = row.get('location_name', 'Unknown')
            depth = row.get('depth', 0)
            dt = row.get('datetime', 'Unknown')
            
            hover_text = (
                f"<b>{location}</b><br>"
                f"Magnitude: {mag:.2f}<br>"
                f"Depth: {depth:.2f} km<br>"
                f"Time: {dt}<br>"
                f"Coords: ({row['latitude']:.2f}, {row['longitude']:.2f})"
            )
            hover_texts.append(hover_text)
        
        # Create the map figure
        fig = go.Figure()
        
        # Add earthquake markers
        fig.add_trace(go.Scattermap(
            lat=lats,
            lon=lons,
            mode='markers',
            marker=dict(
                size=[mag * 5 for mag in magnitudes],  # Scale circle size by magnitude
                color=colors,
                opacity=0.7,
                sizemode='diameter'
            ),
            hovertext=hover_texts,
            hoverinfo='text',
            name='Earthquakes'
        ))
        
        # Calculate center of map (average of all coordinates)
        if lats and lons:
            center_lat = sum(lats) / len(lats)
            center_lon = sum(lons) / len(lons)
        else:
            center_lat, center_lon = 0, 0
        
        # Update map layout
        fig.update_layout(
            map=dict(
                style='open-street-map',
                center=dict(lat=center_lat, lon=center_lon),
                zoom=1
            ),
            margin=dict(l=0, r=0, t=0, b=0),
            showlegend=False,
            hovermode='closest'
        )
        
        # Update timestamp and count
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        last_update = f"Last updated: {timestamp}"
        event_count = f"Total Events: {len(df)}"
        
        return fig, last_update, event_count
    
    except Exception as e:
        # Return empty figure on error
        print(f"Error fetching data: {e}")
        fig = go.Figure()
        fig.update_layout(
            mapbox=dict(
                style='open-street-map',
                center=dict(lat=0, lon=0),
                zoom=1
            ),
            margin=dict(l=0, r=0, t=0, b=0)
        )
        return fig, f"Error updating: {str(e)}", "Unable to fetch events"

# Run the app
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8050, use_reloader=True)
