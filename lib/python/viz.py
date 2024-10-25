import datetime
import glob
import hashlib
import logging
import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Annotated, Any

import typer
import xarray as xr
import zarr
from cdo import Cdo
from dask.distributed import Client, LocalCluster
from viz_config import (
    EnsStat,
    FileType,
    RemapMethod,
    TimeStat,
    WeekDay,
    get_config,
)

# setup logger to use stdout
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


app = typer.Typer()

cdo = Cdo(tempdir="./tmp", silent=False)
os.environ["REMAP_EXTRAPOLATE"] = "off"


def to_zip(input, output):
    # 7zz a -tzip archive.zarr.zip archive.zarr/.
    subprocess.run(["7zz", "a", "-tzip", output, f"{input}/."])


def griddes(lonsize, latsize, slon, slat, resolution):
    """
    Create a cdo grid description file:
    gridtype='lonlat'
    xsize=lonsize
    ysize=latsize
    xfirst=slon
    yfirst=slat
    xinc=resolution
    yinc=resolution
    """
    temp_file = (
        "griddes_"
        + create_hash(f"{lonsize} {latsize} {slon} {slat} {resolution}")
        + ".txt"
    )
    with open(temp_file, "w") as f:
        f.write(
            f"""gridtype='lonlat'
                xsize={lonsize}\n
                ysize={latsize}\n
                xfirst={slon}\n
                yfirst={slat}\n
                xinc={resolution}\n
                yinc={resolution}\n
            """
        )
    return temp_file


def create_hash(input_string: str) -> str:
    hash = hashlib.sha256()
    hash.update(input_string.encode("utf-8"))
    return f"{hash.hexdigest()}.nc"
    # return input_string.replace(" ", "").replace("\n", "").replace("/", "_")


def cdo_execute(input):
    logger.info(f"Processing: cdo {input} ")
    output = create_hash(input)
    logger.info(f"hash path: {output}")
    if Path(output).exists():
        logger.info(f"output for `cdo {input}` already exists; not recomputing")
        return output
    tmp = cdo.copy(input=input)
    # Path(output).parent.mkdir(parents=True, exist_ok=True)
    shutil.move(tmp, output)
    logger.info(f"Completed processing: cdo {input}")
    return output


def sort_by_coord(coord, files):
    # Zip the two lists together, sort based on coord, and unzip back into two lists
    sorted_coord_files = sorted(zip(coord, files), key=lambda x: x[0])
    coord_sorted, files_sorted = zip(*sorted_coord_files)
    # Convert the result back to lists
    return list(coord_sorted), list(files_sorted)


def preproc(inputfiles, cdoOpt):
    # start cpu time
    stime = time.time()
    outputfiles = []
    cdo_inputs = []
    for idx, nc_file in enumerate(inputfiles):
        outputfile = f"mem{idx}_{Path(nc_file).name}"
        outputfiles.append(outputfile)
        cdo_inputs.append(f"{cdoOpt} {nc_file}")
    cpu_count = os.cpu_count()
    nworkers = min(len(cdo_inputs), cpu_count)
    threads_per_worker = int(cpu_count / nworkers)
    with LocalCluster(
        n_workers=nworkers, threads_per_worker=threads_per_worker
    ) as cluster, Client(cluster) as client:
        futures = client.map(cdo_execute, cdo_inputs, outputfiles)
        client.gather(futures)
    logger.info(f"Time for preproc: {time.time() - stime}")
    return outputfiles


def get_member_coord(inputfiles: list[str]):
    """
    inputfiles has format /some/directory/mem{d}/file.nc
    get a list of {d}
    """
    return [int(f.split("/mem")[1].split("/")[0]) for f in inputfiles]


def write_to_zarr(
    ncfiles: list[str],
    coord: list[int] | list[float] | list[str],
    dimname: str,
    output: str,
):
    output = f"{output}.zarr"
    # multiprocessing.set_start_method("fork", force=True)
    store = zarr.DirectoryStore(output)

    if len(coord) != len(ncfiles):
        raise ValueError(
            f" len(coord) ({len(coord)}) does not match number of files ({len(ncfiles)})"
        )
    ds = xr.open_dataset(
        ncfiles[0],
        chunks={"lat": 10, "lon": 10},
    )

    if len(coord) == 1:
        ds.to_zarr(store, mode="w")
    else:
        ds = ds.expand_dims({dimname: len(ncfiles)}).chunk(
            {dimname: 1, "lat": 10, "lon": 10}
        )
        ds = ds.assign_coords({dimname: coord})

        ds.to_zarr(store, mode="w", compute=False)

        len_files = len(ncfiles)
        cpu_count = os.cpu_count()
        nworkers = min(len(ncfiles), cpu_count)
        threads_per_worker = int(cpu_count / nworkers)
        with LocalCluster(
            n_workers=nworkers, threads_per_worker=threads_per_worker
        ) as cluster, Client(cluster) as client:
            futures = client.map(
                _write_to_zarr,
                [store] * len_files,
                ncfiles,
                list(range(len_files)),
                [dimname] * len_files,
            )
            client.gather(futures)
    logger.info(f"{output} created")
    to_zip(output, f"{output}.zip")
    logger.info(f"{output}.zip created")
    # remove output dir
    shutil.rmtree(output)
    logger.info(f"{output} removed")


def write_to_nc(
    ncfiles: list[str],
    coord: list[int] | list[float] | list[str],
    dimname: str,
    output: str,
):
    output = f"{output}.nc"

    if len(coord) != len(ncfiles):
        raise ValueError(
            f" len(coord) ({len(coord)}) does not match number of files ({len(ncfiles)})"
        )
    if len(coord) == 1:
        shutil.move(ncfiles[0], output)
        logger.info(f"{output} created")
        return

    ds = xr.open_mfdataset(
        ncfiles,
        chunks={"lat": 10, "lon": 10},
        combine="nested",
        concat_dim=dimname,
    )
    ds = ds.assign_coords({dimname: coord})
    ds.to_netcdf(output)
    logger.info(f"{output} created")


def _write_to_zarr(zarr_store, nc_file, idx, dimname):
    logger.info(f"Writing file: {nc_file} to zarr store")
    # Open the NetCDF file using xarray
    ds = xr.open_dataset(nc_file, chunks={"lat": 10, "lon": 10}).drop_vars(
        ["Times", "lon", "lat"]
    )
    ds = ds.expand_dims({dimname: 1}).chunk({dimname: 1, "lat": 10, "lon": 10})
    # Define the region where this process will write its data
    region = {dimname: slice(idx, idx + 1)}
    # Write the data to the Zarr store
    ds.to_zarr(zarr_store, region=region)
    # Close the dataset
    ds.close()
    logger.info(f"Finished writing file: {nc_file} to zarr store")


def remap_wgts(
    inputfile,
    res: float | None = None,
    method: RemapMethod = RemapMethod.nearestneighbor,
):
    ds = xr.open_dataset(inputfile, chunks={})
    xlat = ds["XLAT"]
    xlong = ds["XLONG"]
    if res is None:
        res = xlat[1, 0].values - xlat[0, 0].values
    minlat = xlat.min().values
    maxlat = xlat.max().values
    minlon = xlong.min().values
    maxlon = xlong.max().values
    xsize = int((maxlon - minlon) / res)
    ysize = int((maxlat - minlat) / res)
    ds.close()

    griddes_file = griddes(xsize, ysize, minlon, minlat, res)

    remap_wgt_file = f"remap_wgt{method}_{griddes_file}.nc"

    # remap to Lat-Lon
    if method == RemapMethod.nearestneighbor:
        remap_wgt_file = cdo.gennn(griddes_file, input=inputfile, output=remap_wgt_file)
    elif method == RemapMethod.bilinear:
        remap_wgt_file = cdo.genbil(
            griddes_file, input=inputfile, output=remap_wgt_file
        )
    elif method == RemapMethod.conservative:
        remap_wgt_file = cdo.gencon(
            griddes_file, input=inputfile, output=remap_wgt_file
        )
    else:
        raise ValueError(f"Invalid remap method: {method}")
    return griddes_file, remap_wgt_file


def cal_weekstart_offset_days(current_day, desired_start):
    """
    Calculate the number of days to skip from current day to the desired start day.

    Parameters:
    current_day (int): Current weekday (0-6).
    desired_start (int): Desired start weekday (0-6).

    Returns:
    int: Number of days to skip.
    """
    skip_days = (desired_start - current_day + 7) % 7
    return skip_days


def get_weekday(date_string, format="%Y-%m-%d"):
    """
    Get the weekday for a given date.

    Parameters:
    date_string (str): The date in 'YYYYMMDD*' format.

    Returns:
    int: The weekday (Monday=0, Sunday=6).
    """
    date = datetime.datetime.strptime(date_string, format)
    return date.weekday()  # Monday=0, Sunday=6


def add_days_to_date(date_str: str, days: int) -> str:
    # Parse the input date string into a datetime object
    date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")

    # Add the specified number of days using timedelta
    new_date = date_obj + datetime.timedelta(days=days)

    # Return the new date as a string in the same format
    return new_date.strftime("%Y-%m-%d")


def get_time_as_dates(nc_file_path: str) -> list[str]:
    # Open the dataset in lazy loading mode
    with xr.open_dataset(nc_file_path, chunks={}) as ds:
        # Ensure the time variable exists in the dataset
        return ds["Times"].dt.strftime("%Y-%m-%d").values.tolist()


def get_cdoinput_seltimestep(file: str, weekday: WeekDay):
    # Open the NetCDF file using xarray
    timestamps = get_time_as_dates(file)
    start_weekday = get_weekday(timestamps[0])
    offset_days = cal_weekstart_offset_days(start_weekday, weekday.value)
    new_start_date = add_days_to_date(timestamps[0], offset_days)
    end_date = add_days_to_date(new_start_date, 42)  # 6 weeks

    start_time_step = None
    end_time_step = None
    for i, date in enumerate(timestamps):
        if start_time_step is None and new_start_date == date:
            start_time_step = i + 1
        if end_time_step is None and end_date == date:
            end_time_step = i
            break

    if start_time_step is None or end_time_step is None:
        raise ValueError("Failed to find start or end time step")

    return f" -seltimestep,{start_time_step}/{end_time_step}"


def process(
    vname: str,
    pre_cdoinput: str,
    post_cdoinput: str,
    config: TimeStat,
    fcst_files: list[str],
    refcst_files: list[str],
):
    time_coarsen_cdoinput = ""
    if config.time_coarsen is not None:
        time_coarsen_cdoinput = config.time_coarsen.get_cdo_opr()

    time_coarsen_name = config.time_coarsen.name if config.time_coarsen else ""

    preprocess_opr = f"{post_cdoinput} {time_coarsen_cdoinput} {pre_cdoinput}"

    if preprocess_opr.strip():
        fcst_files = cdo_execute_parallel([f"{preprocess_opr} {f}" for f in fcst_files])

        refcst_files = []
        if config.reforecast_needed:
            refcst_files = cdo_execute_parallel(
                [f"{preprocess_opr} {f}" for f in refcst_files]
            )

    for ens_stat_name, ens_stat_oprs in config.ens_stats.items():
        if ens_stat_oprs[0].get_cdo_opr(fcst_files) is None:
            raise NotImplementedError()
        process_ens_stat_cdo(
            vname,
            time_coarsen_name,
            ens_stat_name,
            ens_stat_oprs,
            config.get_ens_stat_coord_values(ens_stat_name),
            fcst_files,
            refcst_files,
            config.file_type[ens_stat_name],
        )


def create_file_name(vname: str, time_coarsen_name: str, stat_name: str):
    fname = f"output/{vname}"
    if time_coarsen_name:
        fname = f"{fname}_{time_coarsen_name}"
    if stat_name:
        fname = f"{fname}_{stat_name}"
    return fname


def process_ens_stat_cdo(
    vname: str,
    time_coarsen_name: str,
    ens_stat_name: str,
    ens_stat_oprs: list[EnsStat],
    coord_values: list[Any],
    fcst_files: list[str],
    refcst_files: list[str],
    file_type: FileType,
):

    cdo_inputs = []
    for ens_stat in ens_stat_oprs:
        cdo_inputs.append(ens_stat.get_cdo_opr(fcst_files))
    files = cdo_execute_parallel(cdo_inputs)

    if file_type in [FileType.zarr, FileType.zarr_and_nc]:
        write_to_zarr(
            files,
            coord_values,
            ens_stat_name,
            create_file_name(vname, time_coarsen_name, ens_stat_name),
        )
    if file_type in [FileType.nc, FileType.zarr_and_nc]:
        write_to_nc(
            files,
            coord_values,
            ens_stat_name,
            create_file_name(vname, time_coarsen_name, ens_stat_name),
        )


def get_cdoinput_preprocess(vname, config, fcst_files):
    if config.preprocess is None:
        return ""
    config = config.preprocess
    remap_opr = get_cdoinput_remap(fcst_files[0], config)
    rename_opr = get_cdoinput_rename(vname, config)
    unit_conversion_opr = get_cdoinput_unit_conversion(vname, config)
    res = f"{unit_conversion_opr} {rename_opr} {remap_opr}"
    return res


def get_cdoinput_remap(in_file, config):
    if config.remap is None:
        return ""
    method = config.remap.method
    res = config.remap.res
    griddes_file, remap_wgt_file = remap_wgts(in_file, res, method)
    return f" -remap,{griddes_file},{remap_wgt_file} "


def get_cdoinput_rename(vname, config):
    if config.rename is None:
        return ""
    oldname = config.rename.from_
    return f" -chname,{oldname},{vname}"


def get_cdoinput_unit_conversion(vname, config):
    if config.unit_conversion is None:
        return ""

    addc = config.unit_conversion.addc
    mulc = config.unit_conversion.mulc
    units = config.unit_conversion.to_units
    cdoOpt = f" -addc,{addc} -mulc,{mulc} "
    if units:
        cdoOpt = f" -setattribute,{vname}@units={units} {cdoOpt}"
    return cdoOpt


def cdo_execute_parallel(inputs: list[str]) -> list[str]:
    # start cpu time
    stime = time.time()
    cpu_count = max(os.cpu_count() - 1, 1)
    nworkers = min(len(inputs), cpu_count)
    threads_per_worker = min(int(cpu_count / nworkers), 4)
    logger.info(f"nworkers: {nworkers}, threads_per_worker: {threads_per_worker}")
    logger.info(f"cdo_execute_parallel inputs: {inputs}")
    with LocalCluster(
        n_workers=nworkers, threads_per_worker=threads_per_worker
    ) as cluster, Client(cluster) as client:
        futures = client.map(cdo_execute, inputs)
        output = client.gather(futures)
    logger.info(f"Time for cdo_execute_parallel: {time.time() - stime}")
    return output


@app.command()
def main(
    vname: Annotated[str, typer.Argument(...)],
    fcst_files_glob_str: Annotated[
        str, typer.Argument(..., help="Forecast files glob string")
    ],
    refcst_files_glob_str: Annotated[
        str, typer.Argument(..., help="Re-forecast files glob string")
    ],
):
    config = get_config()

    config = config.fields.get(vname, None)

    if config is None:
        logger.error(f"Could not find config for {vname}")
        return

    # mkdir outout dir
    Path("output").mkdir(parents=True, exist_ok=True)

    fcst_files = glob.glob(fcst_files_glob_str)
    refcst_files = glob.glob(refcst_files_glob_str)

    # create a lat-lon griddes file by infering the nominal resolution from XLAT, XLONG
    post_opr = get_cdoinput_preprocess(vname, config, fcst_files)

    pre_opr = get_cdoinput_seltimestep(fcst_files[0], config.preprocess.week_start)

    for stat in config.stat:
        process(vname, pre_opr, post_opr, stat, fcst_files, refcst_files)
