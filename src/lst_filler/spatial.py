def calculate_res_metres(da):
    import numpy as np

    m_per_deg = 111_000  # meters per degree
    cos_lat = np.cos(np.deg2rad(da.y.mean().item()))  # mean latitude of the grid

    res_lat = float(da.y.diff("y").mean().item() * m_per_deg)
    res_lon = float(
        da.x.diff("x").mean().item()
        * m_per_deg  # convert to meters
        * cos_lat  # earth is not a perfect sphere, so we need to multiply by the cosine of the latitude
    )

    return np.around([res_lat, res_lon])  # check the resolution of the grid
