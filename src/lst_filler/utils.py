from typing import Union

import munch


def load_yaml_config(fname: str, **kwargs) -> munch.Munch:
    """
    Load and parse a YAML configuration file with Jinja2 template support.

    This function reads a YAML file, renders it as a Jinja2 template using the provided
    keyword arguments, and then parses the rendered content. If no keyword arguments
    are provided, the function will perform a second pass, using values from the YAML
    file itself as template variables, which enables self-referential configurations.

    Parameters
    ----------
    fname : str
        Path to the YAML configuration file.
    **kwargs : Dict[str, Any]
        Keyword arguments to use for rendering the Jinja2 template.
        These values will replace corresponding template variables in the YAML file.

    Returns
    -------
    config_dict : dict
        A dictionary-like object with attribute-style access to the configuration properties

    Examples
    --------
    >>> config = load_yaml_config('config.yaml')
    >>> print(config.some_property)

    >>> config = load_yaml_config('config.yaml', custom_var='value')
    >>> print(config.some_property)
    """
    import jinja2
    import yaml

    # Load the file and render it as a Jinja2 template
    with open(fname, "r") as f:
        template = jinja2.Template(f.read())
        rendered = template.render(**kwargs)

    # Parse the rendered YAML content
    config_dict = yaml.safe_load(rendered)

    # If no keyword arguments were provided, perform a second pass
    # using the values from the YAML file itself as template variables
    if kwargs == {}:
        config_dict = load_yaml_config(fname, **config_dict)

    # Evaluate lambda functions
    config_dict = eval_lambdas(config_dict)

    # Convert to Munch object for attribute-style access
    return munch.munchify(config_dict)


def eval_lambdas(config):
    """
    Recursively evaluate lambda expressions in a configuration dictionary.

    Searches through a dictionary for string values that start with "lambda"
    and evaluates them into actual callable functions. This allows defining
    functions in YAML configuration files.

    Parameters
    ----------
    config : dict
        The configuration dictionary to process

    Returns
    -------
    dict
        The processed configuration dictionary with lambda strings converted to functions

    Examples
    --------
    >>> config = {'func': 'lambda x: x * 2', 'nested': {'func2': 'lambda x: x + 10'}}
    >>> processed = eval_lambdas(config)
    >>> processed['func'](5)
    10
    >>> processed['nested']['func2'](5)
    15

    Notes
    -----
    This function uses eval() which can pose security risks if the configuration
    comes from untrusted sources.
    """
    for key, value in config.items():
        if isinstance(value, dict):
            config[key] = eval_lambdas(value)
        elif isinstance(value, str) and value.startswith("lambda"):
            config[key] = eval(value)
    return config


def munch_to_dict(munch_obj: Union[munch.Munch, dict]) -> dict:
    """
    Converts a Munch object to a dictionary

    Parameters
    ----------
    munch_obj : Union[munch.Munch, dict]
        A Munch object or dictionary to convert

    Returns
    -------
    dict
        A dictionary representation of the input object

    Examples
    --------
    >>> from munch import Munch
    >>> m = Munch(a=1, b=Munch(c=2))
    >>> d = munch_to_dict(m)
    >>> isinstance(d, dict)
    True
    >>> d
    {'a': 1, 'b': {'c': 2}}
    """

    if isinstance(munch_obj, munch.Munch):
        return munch_obj.toDict()
    else:
        return munch_obj


def make_dataset_metadata_writable(ds):
    """
    Removes attributes and coordinates that cannot be written to
    a new NetCDF file. This function is useful when saving a dataset
    to disk.

    Removes attributes or coordinates that are not string,
    float, or int.

    Parameters
    ----------
    ds : xr.Dataset or xr.DataArray
        A dataset to make writable

    Returns
    -------
    ds : xr.Dataset
        A dataset with attributes and coordinates that can be written
        to a new NetCDF file

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> # Create dataset with non-writable attribute
    >>> ds = xr.Dataset(data_vars={'temp': ('time', [20, 25])})
    >>> ds.attrs['func'] = lambda x: x*2  # Non-writable attribute
    >>> # Clean dataset
    >>> clean_ds = make_dataset_metadata_writable(ds)
    >>> 'func' in clean_ds.attrs
    False
    >>> # Now the dataset can be saved to NetCDF
    >>> clean_ds.to_netcdf('clean_data.nc')
    """
    from copy import deepcopy

    import xarray as xr

    def remove_non_writable_attrs(obj):
        attrs = deepcopy(obj.attrs)
        for attr in attrs:
            if not isinstance(attrs[attr], (str, float, int)):
                obj.attrs.pop(attr)
        return obj

    def remove_non_writable_coords(obj):
        coords = deepcopy(obj.coords)
        droppable_coords = []
        for coord in coords:
            c = coords[coord]
            dtype = c.dtype.kind.lower()
            if dtype not in ["f", "i", "b", "u", "m"]:
                droppable_coords.append(coord)
        obj = obj.drop_vars(droppable_coords)
        return obj

    def drop_coord_without_dim(obj):
        coords = deepcopy(obj.coords)
        droppable_coords = []
        for coord in coords:
            if coord not in obj.dims:
                droppable_coords.append(coord)
        obj = obj.drop_vars(droppable_coords)
        return obj

    if isinstance(ds, xr.DataArray):
        ds = ds.to_dataset()

    ds = remove_non_writable_attrs(ds)
    ds = remove_non_writable_coords(ds)
    ds = drop_coord_without_dim(ds)

    for var in ds.data_vars:
        ds[var] = remove_non_writable_attrs(ds[var])

    return ds
