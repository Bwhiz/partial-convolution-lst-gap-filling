import pandas as pd
import requests
from tqdm.auto import tqdm
import time
from pprint import pprint
import warnings

warnings.filterwarnings("ignore")

# manually handling identified error ids
error_ids = ['SFA00682413', 'SFI0000FAAB', 'SFI0000FABM', 'SFI0000FAEO', 'SFI0000FAER', 
             'SFI0000FAGC', 'SFI0000FAGG', 'SFI0000FAHS', 'SFI0000FAIR', 'SFI0000FAJB', 
             'SFI0000FALA', 'SFI0000FALE']

errors = []
for idx, station_id in tqdm(enumerate(error_ids), total=len(error_ids)):

    url = f"https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-station/GHCNh_{station_id}_por.psv"

    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()

        df = pd.read_csv(
            pd.io.common.StringIO(response.text),
            sep='|',  
            skipinitialspace=True,  
            na_values=['', ' ', 'NA'] # handling missing values
        )
        
        df.to_csv(f"./station_data/{station_id}.csv", index=False) # just keeping track of the individual station_id data

        
        if idx == 0:
            df.to_csv("./large_combined_weather_data.csv", mode='w', index=False)
        else:
            df.to_csv("./large_combined_weather_data.csv", mode='a', header=False, index=False)

        # print(f"=== Successfully Processed and appended Station {station_id} ===")

        time.sleep(2)

    except Exception as e:
            print(f"Error processing {station_id}: {str(e)}")
            error_ids.append(station_id)
            continue

print("=== DONE ===")
pprint(f" ERROR Ids :\n {errors}")