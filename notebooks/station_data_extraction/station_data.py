import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm.auto import tqdm
import time
from pprint import pprint
import warnings 

warnings.filterwarnings('ignore')

# extracting global weather stations info:

colspecs = [
        (0, 11),     # Station ID
        (12, 20),    # Latitude
        (21, 30),    # Longitude
        (31, 37),    # Elevation
        (38, 78)     # Station Name
    ]

column_names = [
        'STATION_ID',
        'LATITUDE',
        'LONGITUDE',
        'ELEVATION',
        'STATION_NAME'
    ] 


df = pd.read_fwf(
            "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/ghcnh-station-list.txt",
            colspecs=colspecs,
            names=column_names,
            na_values=['', ' '],
            dtype={
                'STATION_ID': str,
                'STATION_NAME': str
            }
        )

# getting south african weather stations specifically:

sa_data = df[df['STATION_ID'].str.startswith('SF')]
sa_data.reset_index(drop=True, inplace=True)

# sa_data.to_csv('./station_data/south_africa_stations.csv', index=False)

# # importing South Africa station data:
# sa_data = pd.read_csv('./station_data/south_africa_stations.csv')

#print(sa_data['STATION_ID'].values)
sa_ids = sa_data['STATION_ID'].values

error_ids = []

for idx, station_id in tqdm(enumerate(sa_ids), total=len(sa_ids)):

    url = f"https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-station/GHCNh_{station_id}_por.psv"

    try:
        response = requests.get(url)
        response.raise_for_status()

        df = pd.read_csv(
            pd.io.common.StringIO(response.text),
            sep='|',  
            skipinitialspace=True,  
            na_values=['', ' ', 'NA'] # handling missing values
        )
        
        df.to_csv(f"./data/station_data/{station_id}.csv", index=False) # just keeping track of the individual station_id data

        
        if idx == 0:
            df.to_csv("../../data/combined_weather_data.csv", mode='w', index=False)
        else:
            df.to_csv("../../data/combined_weather_data.csv", mode='a', header=False, index=False)

        # print(f"=== Successfully Processed and appended Station {station_id} ===")

        time.sleep(2)

    except Exception as e:
            print(f"Error processing {station_id}: {str(e)}")
            error_ids.append(station_id)
            continue

print("=== DONE ===")
pprint(f" ERROR Ids :\n {error_ids}")