# Land Surface Temperature Gap-Filling Project

This project focuses on the reconstruction of missing values in Land Surface Temperature (LST) imagery using both traditional machine learning methods and a deep learning architecture, Partial Convolutional U-Nets.

---

## 📁 Project Structure

```
.
.
.
├── notebooks
│   ├── Baseline-LightGBM
│   │   └── baseline_RF_Masters_Thesis.ipynb       # Baseline model using traditional ML (LightGBM, XGBoost)
│   ├── Partial-Conv-UNet
│   │   ├── Data-Exploration.ipynb                 # Initial exploration and preprocessing
│   │   └── Partial-Unet.ipynb                     # U-Net with Partial Convolutions
│   ├── complete_lst_dataset.nc                    # NetCDF dataset of complete LST imagery
│   ├── data_matchups.ipynb                        # LST matchups across datasets
│   ├── elev_slope.nc                              # Elevation and slope data
│   ├── prototype_data_matchups.ipynb              # Prototype for data alignment
│   └── station_specific_regression.ipynb          # Per-station regression experiments
│
├── src
│   ├── lst_filler
│   │   ├── __init__.py
│   │   ├── spatial.py                             # Spatial interpolation utilities
│   │   ├── utils.py                               # Utility functions
│   │   ├── data
│   │   │   ├── matchup.py                         # LST and station matchup logic
│   │   │   ├── modis.py                           # MODIS data handling
│   │   │   ├── other.py                           # Other data handling
│   │   │   ├── stations.py                        # Weather station data handling
│   └── station_data_extraction
│       ├── convert_to_parquet.py                  # Convert raw station data to Parquet
│       ├── handling_error_ids.py                  # Clean and filter problematic IDs
│       ├── station_data.py                        # Core logic for station data processing
│
├── pyproject.toml                                  # Project dependencies and configuration
├── uv.lock                                          # uv-generated dependency lock file
```

---

## 📌 Key Notes

- The **`notebooks/`** directory contains all experimental work:
  - Baseline implementations using **traditional ML techniques** are in `Baseline-LightGBM/`.
  - The **Partial Convolutional U-Net model** and related exploration are in `Partial-Conv-UNet/`.

- The **`src/`** directory contains reusable Python modules:
  - `lst_filler/` includes logic for spatial processing and LST gap-filling.
  - `station_data_extraction/` includes scripts used to extract, clean, and transform station-level observations.

---

Feel free to explore the notebooks and scripts to understand the methodology and results.
