# src/lst_filler/data/modis.py:
import pystac_client
import xarray as xr
from cachetools import TTLCache, cached
from loguru import logger

# Create a cache with a TTL of 1 hour
cache = TTLCache(maxsize=1, ttl=3600)


@cached(cache)
def get_planetary_computer_catalog() -> pystac_client.Client:
    import planetary_computer

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )

    return catalog


def get_planetary_computer_data(datetime, search_kwargs, stack_kwargs):
    """
    Search and stack data from the Planetary Computer.

    This function is designed to be used with a YAML configuration file
    that contains the search and stack arguments.

    Parameters
    ----------
    datetime : str
        The date range to search for data in the format 'YYYY-MM-DD/YYYY-MM-DD'.
    search_kwargs : dict
        Keyword arguments to pass to the search function. Typical arguments:
        - collections: [str]
        - bbox: [float, float, float, float]
    stack_kwargs : dict
        Keyword arguments to pass to the stack function. Typical arguments:
        - assets: [str]
        - bounds_latlon: [float, float, float, float]
        - resolution: int
        - epsg: int
        - resampling: str
    """
    import stackstac

    from ..utils import munch_to_dict

    check_planetary_computer_datetime_format(datetime)

    cat = get_planetary_computer_catalog()

    result = cat.search(
        datetime=datetime,
        **munch_to_dict(search_kwargs),
    )

    items = result.item_collection()

    da = stackstac.stack(
        items=items,
        **munch_to_dict(stack_kwargs),
    )

    return da


def check_planetary_computer_datetime_format(datetime):
    import pandas as pd

    assert isinstance(datetime, str), "datetime must be a string"
    slash_count = datetime.count("/")
    assert slash_count == 1, "datetime must have exactly one slash"
    t0, t1 = datetime.split("/")

    try:
        pd.to_datetime(t0)
        pd.to_datetime(t1)
    except Exception:
        raise ValueError("datetime must be in the format 'YYYY-MM-DD/YYYY-MM-DD'")


def convert_modis_viewtime_var_to_coord(da: xr.DataArray) -> xr.DataArray:
    # can be either day or night view time
    contains_day_view = "Day_view_time" in da.band
    contains_night_view = "Night_view_time" in da.band

    # can only be one or the other - for simplicity since function is written for this case
    view_time_count = int(contains_day_view) + int(contains_night_view)
    assert view_time_count == 1, (
        f"Only one view time variable is required, you currently have {view_time_count}"
    )

    # get the name of the view time variable
    view_time_name = "Day_view_time" if contains_day_view else "Night_view_time"

    # convert the view time to a datetime
    view_datetime = get_view_time_as_datetime(da.sel(band=view_time_name))

    da = (
        da
        # assign the datetime as a coordinate and drop the view time variable
        .assign_coords(time=view_datetime)
        # drop the view time variable to slim down the data array
        .drop_sel(band=view_time_name)
        # data aren't sorted by time, so sort them
        .sortby("time")
    )

    return da


def get_view_time_as_datetime(view_time: xr.DataArray) -> xr.DataArray:
    """
    Convert MODIS view time to a datetime DataArray.

    This function processes a MODIS view time band (which represents the hour of day)
    into a proper datetime coordinate. It calculates the average view time across the
    spatial dimensions, converts it to a timedelta, and adds it to the satellite capture
    date to create a complete datetime.

    Parameters
    ----------
    view_time : xr.DataArray
        Input DataArray containing MODIS view time data. Should have:
        - x, y dimensions for spatial coordinates
        - start_datetime attribute with satellite capture date
        - Values representing hours of the day

    Returns
    -------
    xr.DataArray
        A DataArray with a single 'time' dimension containing datetime values.
        Each value represents the date of the satellite capture combined with
        the average view time hour.

    Notes
    -----
    UserWarnings are suppressed during this operation as they commonly occur
    when computing means across spatial dimensions with missing data.
    """
    from warnings import catch_warnings, simplefilter

    # Suppress UserWarnings that commonly occur when processing MODIS data
    with catch_warnings():
        simplefilter("ignore")

        # Calculate mean hour of day across all spatial points and round to nearest integer
        # This gives us a representative time for the entire scene
        view_time_hour = view_time.mean(["x", "y"]).round()

        # Convert hour value to a proper timedelta object (hours)
        # This allows us to add it to a date to get a full datetime
        dt_view_time_hour = view_time_hour.values.astype("timedelta64[h]")

        # Extract the date portion of the satellite capture time
        # This gives us just the day, without time information
        satellite_day = view_time.start_datetime.values.astype("datetime64[D]")

        # Combine the date and hour to create a complete datetime
        # Cast to nanosecond precision to ensure compatibility with xarray
        time_arr = (satellite_day + dt_view_time_hour).astype("datetime64[ns]")

        # Create a DataArray with the time values and a 'time' dimension
        # This makes it compatible for use as a coordinate in other DataArrays
        time_da = xr.DataArray(time_arr, dims="time")

    return time_da


def mask_bad_view_angles(
    da: xr.DataArray,
    angle_band_name="Day_view_angl",
    zenith_angle_offset=-65,
    max_view_angle: float = 40,
    max_bad_angle_ratio: float = 0.1,
) -> xr.DataArray:
    """
    Mask out pixels with view angles greater than a threshold.

    This function filters MODIS data by removing observations where too many pixels
    have extreme view angles. It calculates the zenith angle by applying an offset
    to the provided view angle band, then determines the ratio of "bad" angles
    (those exceeding the maximum threshold) to total angles. Observations with a
    ratio exceeding the maximum allowed ratio are masked out.

    Parameters
    ----------
    da : xr.DataArray
        Input DataArray containing MODIS data with a view angle band.
    angle_band_name : str, optional
        Name of the band containing view angle data, by default "Day_view_angl".
    zenith_angle_offset : int, optional
        Offset applied to the view angle to calculate zenith angle, by default -65.
    max_view_angle : float, optional
        Maximum acceptable view angle in degrees, by default 40.
    max_bad_angle_ratio : float, optional
        Maximum allowed ratio of bad angles to total angles, by default 0.1.
        If the ratio exceeds this value, the observation is masked out.

    Returns
    -------
    xr.DataArray
        DataArray with masked data and angle band removed.
    """
    # Calculate zenith angle by applying the offset to the view angle band
    zenith_angle = da.sel(band=angle_band_name) + zenith_angle_offset
    # Convert to absolute value since we only care about the magnitude of the angle
    zenith_angle = zenith_angle.pipe(abs)

    # Count pixels with angles above the maximum threshold (bad angles)
    bad_angles = (zenith_angle > max_view_angle).sum(["x", "y"])
    # Count pixels with acceptable angles (good angles)
    good_angles = (zenith_angle <= max_view_angle).sum(["x", "y"])

    # Calculate the ratio of bad angles to total angles for each time
    bad_angles_ratio = bad_angles / (bad_angles + good_angles)
    # Create a boolean mask where True means acceptable ratio of bad angles
    mask = bad_angles_ratio < max_bad_angle_ratio

    # Select only times where the bad angle ratio is acceptable and drop the angle band
    masked_data = da.sel(time=mask.values).drop_sel(band=angle_band_name)

    return masked_data


def get_modis_lst(datetime, modis_dict, max_view_angle=40):
    logger.info(f"Getting MODIS LST data for {datetime} from the Planetary Computer")
    modis_lst = get_planetary_computer_data(
        datetime=datetime,
        search_kwargs=modis_dict.search,
        stack_kwargs=modis_dict.stackstac,
    )

    logger.info("Converting view time to datetime")
    modis_lst = convert_modis_viewtime_var_to_coord(modis_lst)

    logger.info("Aggregating data by same times")
    modis_lst = modis_lst.groupby("time").mean()

    if max_view_angle is not None:
        logger.info("Masking out bad view angles")
        modis_lst = mask_bad_view_angles(modis_lst, max_view_angle=max_view_angle)

    logger.info("Downloading data to local memory")
    out = modis_lst.compute()

    return out
