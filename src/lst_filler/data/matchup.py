# src/lst_filler/data/matchup.py:

import pandas as pd
import xarray as xr

README = """
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

"""


def match_station_with_modis(df_station, da_modis):
    """ "
    Match the station data with the MODIS data
    """
    # we have to drop this so we don't get duplicates when using groupby
    df_station = df_station.reset_index("station_name", drop=True)

    # create indexer for dataArray from dataframe
    station_coords = create_modis_selection(df_station)

    # here we fetch the nearest points from the MODIS data, but this does
    # not mean that the matches are within the period we want
    matched_lst_points = da_modis.sel(**station_coords, method="nearest")

    df_station["modis_LST"] = matched_lst_points.values
    df_station["modis_time"] = matched_lst_points.time.values

    # get the times from modis and the station
    time_station = xr.DataArray(
        data=df_station.reset_index()["datetime"].values,
        dims=["index"],
        name="time",
        coords={"index": matched_lst_points.index.values},
    )
    time_modis = matched_lst_points.time.drop_vars(["time", "y", "x"])
    time_diff_hrs = (time_station - time_modis).dt.total_seconds() / 3600

    mask_t1, mask_t0 = get_matching_and_prev(time_diff_hrs)
    df_t1 = df_station.loc[mask_t1.values]
    df_t0 = df_station.loc[mask_t0.values].set_index(df_t1.index)

    df_matched = (
        df_t1.join(df_t0, rsuffix="_tm1")
        .dropna(subset=["modis_LST"])
        .drop(
            columns=[
                "modis_time_tm1",
                "modis_LST_tm1",
                "latitude_tm1",
                "longitude_tm1",
            ]
        )
    )

    return df_matched


def get_matching_and_prev(
    time_diff_hrs: xr.DataArray,
) -> tuple[xr.DataArray, xr.DataArray]:
    # for matches, we use only data that falls within 1 hour (either side)
    mask_match_t1 = time_diff_hrs.pipe(abs) <= 1

    # for the previous time, we shift the mask by -1, however, this
    # means that some values might have a larger time difference than 4 hours
    mask_match_t0 = mask_match_t1.shift(index=-1, fill_value=False)
    # so, we make sure that tehse values are within 4 hours
    mask_match_t0_within_4hrs = time_diff_hrs.where(mask_match_t0).pipe(abs) <= 4
    # but we can only have the match if there are previous time steps too!
    # so we need to use the mask from the previous time step and apply it to the initial match
    mask_match_t1_with_prev = mask_match_t0_within_4hrs.shift(index=1, fill_value=False)

    # now we can apply the masks to our data - here we enforce both masks
    mask_match_t1 = mask_match_t1 & mask_match_t1_with_prev
    mask_match_t0 = mask_match_t0 & mask_match_t0_within_4hrs

    return mask_match_t1, mask_match_t0


def create_modis_selection(df, time="datetime", y="latitude", x="longitude"):
    """
    Create a selection from a dataframe to match the dimensions of a modis dataset.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to create the selection from.
    time : str, optional
        Name of the time column in the DataFrame, by default 'datetime'.
    y : str, optional
        Name of the y column in the DataFrame, by default 'latitude'.
    x : str, optional
        Name of the x column in the DataFrame, by default 'longitude'.

    Returns
    -------
    xarray.Dataset
        Selection with the same dimensions as the modis dataset.
        The dimension is called index, with variables time, y, x.

    Example
    -------
    >>> sel = create_modis_selection(df_subset)
    >>> modis_lst.sel(**sel).to_

    """
    index = [time, y, x]
    names = ["time", "y", "x"]
    rename = dict(zip(index, names))

    selection = (
        df.reset_index()  # move the index into the columns (e.g., station, datetime)
        .set_index([time, y, x])  # now, set index to time, y, x
        .sort_index()  # sort by time, then station
        .index.to_frame(index=False)  # convert the index to a dataframe
        .to_xarray()
        .rename(**rename)  # has to be consistent with MODIS names
    )

    # selection has shape
    # xr.DataArray(data=[time, y, x], dims=['index'], coords={'index': ...})

    return selection


def filter_dataframe(df: pd.DataFrame, excl_cols_with: list[str]) -> pd.DataFrame:
    excl_cols_patterns = [f"^(?!.*{col}.*)" for col in excl_cols_with]
    pattern = "".join(excl_cols_patterns)

    return df.filter(regex=pattern)
