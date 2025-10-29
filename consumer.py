from quixstreams import Application
import json
import time
import requests
import os
import pandas as pd
from datetime import datetime


def insert_seismic_record(kafka_key, offset, value):
    """Insert a seismic record into the database"""
    try:
        action = value.get('action', 'unknown')
        
        # Extract properties from the nested structure
        properties = {}
        if 'data' in value and 'properties' in value['data']:
            properties = value['data']['properties']
        
        unid = properties.get('unid', '')
        time_val = properties.get('time', '')
        time_val = datetime.strptime(time_val, '%Y-%m-%dT%H:%M:%S.%fZ')
        time_val = time_val.strftime('%Y-%m-%d %H:%M:%S')
        mag = properties.get('mag', None)
        flynn_region = properties.get('flynn_region', '')
        longitude = properties.get('lon', '')
        latitude = properties.get('lat', '')
        depth = properties.get('depth', '')
        
        # Write to CSV file
        csv_file = "seismic.csv"
        df = pd.DataFrame({
            "unid": [unid],
            "time": [time_val],
            "mag": [mag],
            "flynn_region": [flynn_region],
            "longitude": [longitude],
            "latitude": [latitude],
            "depth": [depth]
        })
        df.to_csv(csv_file, mode='a', header=not os.path.exists(csv_file), index=False)
        
        return True
    except Exception as e:
        print(f"Error inserting record: {e}")
        return False

def main():
    
    app = Application(
        broker_address="localhost:19092",
        loglevel="DEBUG",
        consumer_group="seismic_reader",
        auto_offset_reset="earliest",
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["eu_seismic"])

        while True:
            msg = consumer.poll(5)

            if msg is None:
                print("Waiting...")
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode("utf8")
                value = json.loads(msg.value())
                offset = msg.offset()

                # print(f"{offset} {key} {value}")
                
                # Insert into MySQL
                # if insert_seismic_record(conn, key, offset, value):
                if insert_seismic_record(key, offset, value):
                    print(f"✓ Inserted record {offset} into MySQL")
                else:
                    print(f"✗ Failed to insert record {offset}")
                
                consumer.store_offsets(msg)
                time.sleep(10)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
