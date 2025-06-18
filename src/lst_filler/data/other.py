# lst_filler.data.other
import xarray as xr


def get_single_timestep_data_from_planetary_computer(search_kwargs, stack_kwargs):
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
    from .modis import get_planetary_computer_catalog

    cat = get_planetary_computer_catalog()

    result = cat.search(**munch_to_dict(search_kwargs))
    items = result.item_collection()

    da = stackstac.stack(
        items=items,
        **munch_to_dict(stack_kwargs),
    )

    return da


def get_dem(
    search_kwargs: dict,
    stack_kwargs: dict,
    target_grid_latlon: xr.DataArray = None,
):
    # DEM data will be in metres instead of LATLON
    dem = get_single_timestep_data_from_planetary_computer(
        search_kwargs=search_kwargs,
        stack_kwargs=stack_kwargs,
    )

    # keep the coordinate reference system to assign later
    # CRS defines the units of the x, y coordinates (e.g., lat/lon, metres)
    crs = dem.rio.crs
    dem = (
        dem.mean("time")  # time dimension is actually the tiles of the DEM
        .isel(band=0, drop=True)  # only one band = elevation
        .rename("elev")  # rename it for clarity
    )
    ds = calc_elev_params(dem)  # calculate slope and aspect
    ds = ds.rio.write_crs(crs)  # write the CRS to the dataset

    # now, we need to reproject the DEM to the target grid (in lat/lon)
    # so we're going from a UTM projection to a lat/lon projection
    if target_grid_latlon is None:
        ds = ds.rio.reproject(epsg=4326, resolution=0.01)
    else:
        target_grid_latlon = target_grid_latlon.rio.write_crs("EPSG:4326")
        ds = ds.rio.reproject_match(target_grid_latlon)

    return ds


def calc_elev_params(
    da: xr.DataArray,
):
    import numpy as np
    import xrspatial as xs

    ds = da.to_dataset(name="elev")
    ds["slope"] = xs.slope(da)
    ds["aspect"] = xs.aspect(da)
    ds["aspect_cos"] = np.cos(np.deg2rad(ds["aspect"]))
    ds["aspect_sin"] = np.sin(np.deg2rad(ds["aspect"]))

    return ds
