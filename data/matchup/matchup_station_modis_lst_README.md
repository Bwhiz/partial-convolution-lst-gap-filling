
# Matched Station data with MODIS LST

Data was created with
```python
from xarray import xr
from lst_filler.data.stations import read_stations
from lst_filler.data.matchup import match_station_with_modis

# open downloaded MODIS data
da = xr.open_mfdataset('../data/modis/*.nc', parallel=True).modis_LST
t0, t1 = da_modis.time.min().values, da_modis.time.max().values

# open station data
df = read_stations(**config.stations, progressbar=True)
df = df.loc[slice(t0, t1)].reorder_levels(['station_name', 'datetime']).sort_index()


df_matched = df.groupby('station_name').apply(
    lambda x: match_station_with_modis(x, da))
```

|                              | modis_time | modis_lst | station_var | station_var_tm1 † |
|------------------------------|------------|-----------|-------------|-------------------|
| [station_name, station_time] |            |           |             |                   |
| [station_name, station_time] |            |           |             |                   |

† where tm1 represents the station data from the previous timestep,
  where that timestep is within 4 hours of the modis data.

## Usage
These data can be used to compute the linear relationship between
the station temperature and MODIS land surface temperature.

This linear relationship can be used to conver the station data
to LST. Unfortunately, the other way around is not possible since
we cannot estimate dew_point_temperature, station_level_pressure, etc.
by remote sensing. But, what this means, is that we can have
station data at a 3 hourly interval that is relatable to MODIS LST.

