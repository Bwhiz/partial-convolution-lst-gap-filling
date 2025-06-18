# src/lst_filler/data/stations.py:
import pandas as pd


def read_stations(
    path, columns=None, filters=[], datetime_cols=[], index_cols=[], progressbar=False
) -> pd.DataFrame:
    """
    Read a list of parquet files and return a pandas dataframe

    Uses dask to read the parquet files in parallel and
    convert them to a pandas dataframe.

    Parameters
    ----------
    path : str
        The path to the parquet files. Can be a glob pattern
    columns : list
        The columns to read from the parquet files
    filters : list
        A list of tuples with the column name and the value to filter.
        e.g., [('Year', ">", 2014), (...)]
    datetime_cols : list
        A list of columns to convert to datetime - e.g., ['Year', 'Month', 'Day']
    index_cols : list
        A list of columns to set as the index at the end
    progressbar : bool
        Whether to show a progress bar when reading the files

    Returns
    -------
    pd.DataFrame
        A pandas dataframe with the data from the parquet files
    """
    from dask import dataframe as dd

    if progressbar:
        from dask.diagnostics import ProgressBar
    else:
        # dummy progress bar that shows nothing if progressbar=False
        ProgressBar = DummyProgress

    df = dd.read_parquet(
        path=path,
        columns=columns,
        # filters must be tuples, but yaml can only read in as a list
        filters=[tuple(f) for f in filters],
        engine="pyarrow",
    )

    def str_func(s):
        return s.replace(" ", "_").lower()

    df = (
        df.assign(datetime=dd.to_datetime(df[datetime_cols]))
        .drop(datetime_cols, axis=1)
        .rename(columns=lambda s: str_func(s))
    )

    with ProgressBar():
        df = df.compute()

    if index_cols != []:
        df = df.set_index([str_func(c) for c in index_cols]).sort_index()

    return df


def convert_to_geopandas(df, aggregate=True, lat_col="latitude", lon_col="longitude"):
    """
    Convert a pandas dataframe to a geopandas dataframe

    Parameters
    ----------
    df : pd.DataFrame
        The input dataframe
    aggregate : bool
        Whether to aggregate the dataframe by the lat and lon columns.
        This can be useful if the dataframe has multiple rows with the same
        lat and lon values, and you only want to keep one of them for plotting
    lat_col : str
        The name of the column containing the latitude values
    lon_col : str
        The name of the column containing the longitude values

    Returns
    -------
    gpd.GeoDataFrame
        A geopandas dataframe with the same data as the input dataframe
        with an additional 'geometry' column containing the lat and lon values
        Reduced to unique lat and lon values if aggregate=True

    """
    # convert df.Latitude and df.Longitude to a geopandas.geometry object
    import geopandas as gpd

    if aggregate:
        df = (
            df.groupby([lat_col, lon_col])
            .apply(lambda x: x.iloc[0])
            .reset_index(drop=True)
        )

    gdf = gpd.GeoDataFrame(
        data=df,
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs="EPSG:4326",
    )

    return gdf


class DummyProgress:
    """with context manager for dask progress bar"""

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass
