from concurrent.futures import ProcessPoolExecutor, as_completed
import numpy as np
from shapely.geometry import mapping
import rasterio.mask
import rasterio
import geopandas as gpd
import pandas as pd
import zipfile
from rasterio.io import MemoryFile
from settings import Dirs, SHAPEFILE_PATH
import re
from pathlib import Path
from tqdm import tqdm
from models import Variable, TimeStep


def read_prism_zip(zip_path):
    """
    Reads the first .tif inside a PRISM zip file into a rasterio dataset.

    Parameters
    ----------
    zip_path : str
        Path to the PRISM .zip file

    Returns
    -------
    data : numpy.ndarray
        Raster data array
    profile : dict
        Raster metadata/profile
    """
    with zipfile.ZipFile(zip_path, "r") as z:
        # Find the first tif in the archive
        tif_names = [name for name in z.namelist() if name.endswith(".tif")]
        if not tif_names:
            raise ValueError("No .tif found inside {}".format(zip_path))
        tif_name = tif_names[0]

        # Read the tif bytes into memory
        with z.open(tif_name) as f:
            with MemoryFile(f.read()) as memfile:
                with memfile.open() as dataset:
                    data = dataset.read(1)      # first band
                    profile = dataset.profile
    return data, profile


def read_shapefile_zip(zip_path):
    """
    Reads a zipped shapefile (like TIGER/CB files) into a GeoDataFrame.

    Parameters
    ----------
    zip_path : str
        Path to .zip file containing shapefile

    Returns
    -------
    gdf : geopandas.GeoDataFrame
    """
    gdf = gpd.read_file(f"zip://{zip_path}")

    # construct fip
    if "STATEFP" in gdf.columns and "COUNTYFP" in gdf.columns:
        gdf["FIPS"] = gdf["STATEFP"].astype(str).str.zfill(
            2) + gdf["COUNTYFP"].astype(str).str.zfill(3)

    # select cols
    keep_cols = [c for c in ["FIPS", "GEOID", "geometry"] if c in gdf.columns]
    gdf = gdf[keep_cols]
    return gdf


def zonal_average(data, profile, gdf, value_col):
    """
    Averages raster values over polygons in a GeoDataFrame.

    Parameters
    ----------
    data : np.ndarray
        Raster array (single band)
    profile : dict
        Raster metadata (from rasterio)
    gdf : geopandas.GeoDataFrame
        Input polygons
    value_col : str, optional
        Column name for averaged values

    Returns
    -------
    gdf_out : geopandas.GeoDataFrame
        Copy of gdf with an added column of averaged raster values
    """
    # Ensure projection match
    raster_crs = profile["crs"]
    if gdf.crs != raster_crs:
        gdf = gdf.to_crs(raster_crs)

    results = []
    with MemoryFile() as memfile:
        # Write the raster into memory
        with memfile.open(**profile) as dataset:
            dataset.write(data, 1)

            for geom in gdf.geometry:
                if geom is None or geom.is_empty:
                    results.append(np.nan)
                    continue
                try:
                    out_image, _ = rasterio.mask.mask(
                        dataset, [mapping(geom)], crop=True)
                    masked = out_image[0]
                    nodata = profile.get("nodata")
                    if nodata is not None:
                        masked = masked[masked != nodata]
                    if masked.size == 0:
                        results.append(np.nan)
                    else:
                        results.append(masked.mean())
                except Exception:
                    results.append(np.nan)

    gdf_out = gdf.copy()
    gdf_out[value_col] = results
    return gdf_out


def process_prism_file(fpath, shapefile):
    """
    Processes a single prism file, aggregating to shapefile

    Parameters
    ----------
    fpath : str
        path to a prism file
    shapefile : geopandas.GeoDataFrame
        Raster metadata (from rasterio)

    Returns
    -------
    s : pandas.Series
        A MultiIndexed Series of this files' values
    """
    # figure out what kind of data this is (tmin, tmax, tavg, ppt)
    patt = re.compile(r"prism_(.*)_us")
    m = patt.search(fpath.name)
    data_point = m.group(1)

    data, profile = read_prism_zip(fpath)
    gdf_out = zonal_average(data, profile, shapefile, value_col=data_point)
    gdf_out = gdf_out[["FIPS", "GEOID", data_point]]
    s = gdf_out.set_index(["FIPS", "GEOID"])
    return s.stack()


def process_prism_date(date: str, sp_path=SHAPEFILE_PATH):
    gdf = read_shapefile_zip(sp_path)
    output_fname = f"prism_{date}.parquet"

    # if you have already processed this date, skip
    parquet_match = list(Dirs.clean.rglob(f"*{date}.parquet"))
    if len(parquet_match) > 0:
        print(f"Skipping {date} - Already Processed")
        return

    raw_dir = Dirs.output / "prism_raw"
    files = list(raw_dir.rglob(f"*{date}.zip"))
    output = []
    for f in files:
        output.append(process_prism_file(f, gdf))

    output = pd.concat(output)
    output = output.unstack().reset_index()
    output["date"] = date
    output.to_parquet(Dirs.clean / output_fname, index=False)
    return output


def filter_prism_files(var: Variable = None, scope: TimeStep = None, year=None):
    raw_dir = Dirs.output / "prism_raw"
    output = list(raw_dir.resolve().rglob("*.zip"))
    if var:
        output = [f for f in output if var.value in f.parts]
    if scope:
        output = [f for f in output if scope.value in f.parts]
    if year:
        output = [f for f in output if str(year) in f.parts]

    return output


def main():
    # get all unique dates to loop through
    all_zips = filter_prism_files()
    dates = [Path(f).stem.split("_")[-1] for f in all_zips]
    dates = sorted(set(dates))
    results = []

    # for each date, process that date and parallelize
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_prism_date, d) for d in dates]
        for f in tqdm(as_completed(futures), total=len(futures)):
            try:
                f.result()
            except Exception as e:
                d = futures[f]
                print(f"failed on {d}: {e}")


if __name__ == "__main__":
    main()
