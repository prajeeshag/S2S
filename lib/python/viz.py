import datetime
import glob
import hashlib
import itertools
import logging
import math
import os
import shutil
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Annotated, Any, Callable

import numpy as np
import psutil
import typer
import xarray as xr
import zarr
from cdo import Cdo
from dask import config as cfg
from dask.distributed import Client, LocalCluster, get_worker
from scipy import ndimage, stats
from viz_config import (
    EnsStat,
    EnsStatOpr,
    FileType,
    RemapMethod,
    TimeStat,
    WeekDay,
    get_config,
)

cfg.set({"distributed.scheduler.worker-ttl": None})

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


def _to_zip(input, output):
    # 7zz a -tzip archive.zarr.zip archive.zarr/.
    tmp = output + ".tmp.zip"
    subprocess.run(["7zz", "a", "-tzip", tmp, f"{input}/."])
    shutil.move(tmp, output)


def _griddes(lonsize, latsize, slon, slat, resolution):
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
        + _create_hash(f"{lonsize} {latsize} {slon} {slat} {resolution}")
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


def _create_hash(input_string: str) -> str:
    hash = hashlib.sha256()
    hash.update(input_string.encode("utf-8"))
    return f"{hash.hexdigest()}.nc"
    # return input_string.replace(" ", "").replace("\n", "").replace("/", "_")


def _cdo_execute(input):
    logger.info(f"Processing: cdo {input} ")
    output = _create_hash(input)
    logger.info(f"hash path: {output}")
    if Path(output).exists():
        logger.info(f"output for `cdo {input}` already exists; not recomputing")
        return output
    tmp = cdo.copy(input=input)
    # Path(output).parent.mkdir(parents=True, exist_ok=True)
    shutil.move(tmp, output)
    logger.info(f"Completed processing: cdo {input}")
    return output


def get_chunk_slices(array: xr.DataArray):
    chunks = array.chunks
    shape = array.shape
    dim_names = array.dims
    logger.info(f"chunks: {chunks}, shape: {shape}, dim_names: {dim_names}")
    dim_slices = []
    # Calculate slices for each dimension
    for dim_chunks, dim_size in zip(chunks, shape):
        dim_indices = []
        start = 0
        for size in dim_chunks:
            end = start + size
            dim_indices.append(slice(start, end))
            start = end
        dim_slices.append(dim_indices)
    # Generate the Cartesian product of all slices across dimensions
    for chunk_slices in itertools.product(*dim_slices):
        isel = {n: x for n, x in zip(dim_names, chunk_slices)}
        yield isel


def _write_to_zarr_ds(input: tuple[xr.Dataset, dict], output: str):
    worker = get_worker()
    ds, region = input
    ds.load(scheduler="synchronous")
    logger.info(f"Worker {worker.address} Writing region: {region} to {output}")
    ds.to_zarr(output, region=region)
    ds.close()
    del ds
    del region


def write_to_zarr_ds(
    vname: str,
    ncfiles: list[str],
    coord: list[int] | list[float] | list[str],
    dimname: str,
    output: str,
    nworkers: int | None = None,
):
    output = f"{output}.zarr"
    if Path(f"{output}.zip").exists():
        logger.info(f"{output}.zip already exists; not recomputing")
        return

    if len(ncfiles) > 1:
        if len(coord) != len(ncfiles):
            raise ValueError(
                f" len(coord) ({len(coord)}) does not match number of files ({len(ncfiles)})"
            )
        ds = xr.open_mfdataset(
            ncfiles,
            combine="nested",
            concat_dim=dimname,
            chunks={},
        ).chunk(
            {"lat": 10, "lon": 10, "Times": -1, dimname: len(ncfiles)},
        )
        ds = ds.assign_coords({dimname: coord})
    else:
        ds = xr.open_dataset(
            ncfiles[0],
            chunks={},
        ).chunk(
            {"lat": 10, "lon": 10, "Times": -1},
        )

    # multiprocessing.set_start_method("fork", force=True)
    store = zarr.DirectoryStore(output)

    ds.to_zarr(store, mode="w", compute=False)
    cpu_count = max(psutil.cpu_count(logical=False) // 2, 1)
    if nworkers is None:
        nworkers = cpu_count
    nworkers = min(nworkers, cpu_count)
    threads_per_worker = int(cpu_count / nworkers)
    logger.info(f"nworkers: {nworkers}, threads_per_worker: {threads_per_worker}")

    ds = ds.drop_vars(set(ds.variables) - {vname})
    ds.attrs = {}
    logger.info(f"ds: {ds}")
    ds_loaded = ds.compute()
    logger.info("Submitting parallel jobs")
    with LocalCluster(
        n_workers=nworkers, threads_per_worker=threads_per_worker
    ) as cluster, Client(cluster) as client:
        futures = []
        for chunk_slice in get_chunk_slices(ds[vname]):
            input = (ds_loaded.isel(**chunk_slice), chunk_slice)
            futures.append(client.submit(_write_to_zarr_ds, input, output))
        client.gather(futures)
    ds.close()
    ds_loaded.close()
    logger.info(f"{output} created")
    _to_zip(output, f"{output}.zip")
    logger.info(f"{output}.zip created")
    # remove output dir
    shutil.rmtree(output)
    logger.info(f"{output} removed")


def write_to_zarr(
    ncfiles: list[str],
    coord: list[int] | list[float] | list[str],
    dimname: str,
    output: str,
    nworkers: int | None = None,
    use_dask: bool = True,
):
    output = f"{output}.zarr"
    if Path(f"{output}.nc").exists():
        return
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
        cpu_count = max(psutil.cpu_count(logical=False) // 2, 1)
        if nworkers is None:
            nworkers = min(len(ncfiles), cpu_count)
        else:
            nworkers = min(nworkers, cpu_count)
        threads_per_worker = int(cpu_count / nworkers)
        if use_dask:
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
        else:
            with ProcessPoolExecutor(max_workers=nworkers) as executor:
                futures = executor.map(
                    _write_to_zarr,
                    [store] * len_files,
                    ncfiles,
                    list(range(len_files)),
                    [dimname] * len_files,
                )
                list(futures)
    logger.info(f"{output} created")
    _to_zip(output, f"{output}.zip")
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

    if len(ncfiles) > 1 and len(coord) != len(ncfiles):
        raise ValueError(
            f" len(coord) ({len(coord)}) does not match number of files ({len(ncfiles)})"
        )
    if len(ncfiles) == 1:
        shutil.move(ncfiles[0], output)
        logger.info(f"{output} created")
        return

    ds = xr.open_mfdataset(
        ncfiles,
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


def _remap_wgts(
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

    griddes_file = _griddes(xsize, ysize, minlon, minlat, res)

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


def get_time_as_dates(nc_file_path: str, format="%Y-%m-%d") -> list[str]:
    # Open the dataset in lazy loading mode
    with xr.open_dataset(nc_file_path, chunks={}) as ds:
        # Ensure the time variable exists in the dataset
        return ds["Times"].dt.strftime(format).values.tolist()


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


def get_cdoinput_settaxis(fcstfile: str):
    # Open the NetCDF file using xarray
    timestamps = get_time_as_dates(fcstfile)
    with xr.open_dataset(fcstfile, chunks={}) as ds:
        # Ensure the time variable exists in the dataset
        timestamps = ds["Times"][0:2].dt.strftime("%Y-%m-%dT%H:%M:%S").values.tolist()
    start_date = timestamps[0].split("T")[0]
    start_time = timestamps[0].split("T")[1]
    time1 = datetime.datetime.strptime(timestamps[0], "%Y-%m-%dT%H:%M:%S")
    time2 = datetime.datetime.strptime(timestamps[1], "%Y-%m-%dT%H:%M:%S")
    # Calculate the difference in hours
    inc_hours = (time2 - time1).total_seconds() / 3600

    return f" -settaxis,{start_date},{start_time},{inc_hours}hour "


def process(
    vname: str,
    pre_cdoinput: str,
    post_cdoinput: str,
    config: TimeStat,
    fcst_files: list[str],
    refcst_files: list[str],
    week_start,
):
    time_coarsen_cdoinput = ""
    if config.time_aggregation is not None:
        time_coarsen_cdoinput = config.time_aggregation.get_cdo_opr()

    time_coarsen_name = config.time_aggregation.name if config.time_aggregation else ""

    skip_week = ""
    if config.skip_week:
        skip_week = get_cdoinput_seltimestep(fcst_files[0], week_start)

    preprocess_opr = (
        f"{post_cdoinput} {time_coarsen_cdoinput} {skip_week} {pre_cdoinput}"
    )

    if config.reforecast_needed:
        logger.info(f"Reforecast files for {vname} needed")
        if not refcst_files:
            raise ValueError(f"Could not find reforecast files for {vname}")
        set_taxis = get_cdoinput_settaxis(fcst_files[0])
        refcst_files = cdo_execute_parallel(
            [f"{preprocess_opr} {set_taxis} {f}" for f in refcst_files]
        )
    if preprocess_opr.strip():
        fcst_files = cdo_execute_parallel([f"{preprocess_opr} {f}" for f in fcst_files])
    else:
        refcst_files = refcst_files

    for ens_stat_name, ens_stat_oprs in config.ens_stats.items():
        if ens_stat_oprs.is_cdo_opr:
            process_ens_stat_cdo(
                vname,
                time_coarsen_name,
                ens_stat_name,
                ens_stat_oprs,
                fcst_files,
                refcst_files,
                config.file_type[ens_stat_name],
            )
        elif ens_stat_oprs.opr == EnsStatOpr.efi:
            logger.info(f"Calculating {ens_stat_name} for {vname}")
            calc_efi_stats(
                vname,
                time_coarsen_name,
                ens_stat_name,
                config.file_type[ens_stat_name],
                fcst_files,
                refcst_files,
            )
            logger.info(f"Completed Calculating {ens_stat_name} for {vname}")
        elif ens_stat_oprs.opr == EnsStatOpr.sotn:
            logger.info(f"Calculating {ens_stat_name} for {vname}")
            calc_sot_stats(
                vname,
                time_coarsen_name,
                ens_stat_name,
                config.file_type[ens_stat_name],
                fcst_files,
                refcst_files,
                calc_sotn,
            )
            logger.info(f"Completed Calculating {ens_stat_name} for {vname}")
        elif ens_stat_oprs.opr == EnsStatOpr.sotp:
            logger.info(f"Calculating {ens_stat_name} for {vname}")
            calc_sot_stats(
                vname,
                time_coarsen_name,
                ens_stat_name,
                config.file_type[ens_stat_name],
                fcst_files,
                refcst_files,
                calc_sotp,
            )
            logger.info(f"Completed Calculating {ens_stat_name} for {vname}")
        elif ens_stat_oprs.opr == EnsStatOpr.members:
            logger.info(f"Writing {ens_stat_name} for {vname}")
            file_name = create_file_name(vname, time_coarsen_name, ens_stat_name)
            write_members(vname, config, fcst_files, ens_stat_name, file_name)
            logger.info(f"Completed writing {ens_stat_name} for {vname}")
        elif ens_stat_oprs.opr == EnsStatOpr.rfmembers:
            logger.info(f"Writing {ens_stat_name} for {vname}")
            file_name = create_file_name(vname, time_coarsen_name, ens_stat_name)
            write_members(vname, config, refcst_files, ens_stat_name, file_name)
            logger.info(f"Completed writing {ens_stat_name} for {vname}")
        else:
            raise NotImplementedError


def write_members(vname, config, fcst_files, ens_stat_name, file_name):
    if config.is_zarr(ens_stat_name):
        write_to_zarr_ds(
            vname,
            fcst_files,
            coord=list(range(len(fcst_files))),
            dimname="member",
            output=file_name,
        )
    if config.is_nc(ens_stat_name):
        write_to_nc(
            fcst_files,
            coord=list(range(len(fcst_files))),
            dimname="member",
            output=file_name,
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
    ens_stat_oprs: EnsStat,
    fcst_files: list[str],
    refcst_files: list[str],
    file_type: FileType,
):

    cdo_inputs = []
    for cdo_opr in ens_stat_oprs.get_cdo_opr(fcst_files):
        cdo_inputs.append(cdo_opr)
    files = cdo_execute_parallel(cdo_inputs)

    coord_values = ens_stat_oprs.values
    if file_type in [FileType.zarr, FileType.zarr_and_nc]:
        write_to_zarr_ds(
            vname,
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
    griddes_file, remap_wgt_file = _remap_wgts(in_file, res, method)
    return f" -remap,{griddes_file},{remap_wgt_file} "


def get_cdoinput_rename(vname, config):
    # if config.rename is None:
    #     return ""
    # oldname = config.rename.from_
    return f" -setname,{vname}"


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
    cpu_count = max(psutil.cpu_count(logical=False) // 2, 1)
    nworkers = min(len(inputs), cpu_count)
    threads_per_worker = min(int(cpu_count / nworkers), 4)
    logger.info(f"nworkers: {nworkers}, threads_per_worker: {threads_per_worker}")
    logger.info(f"cdo_execute_parallel inputs: {inputs}")
    with LocalCluster(
        n_workers=nworkers, threads_per_worker=threads_per_worker
    ) as cluster, Client(cluster) as client:
        futures = client.map(_cdo_execute, inputs)
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

    logger.info(f"fcst_files_glob_str: {fcst_files_glob_str}")
    logger.info(f"refcst_files_glob_str: {refcst_files_glob_str}")
    fcst_files = glob.glob(fcst_files_glob_str)
    refcst_files = glob.glob(refcst_files_glob_str)

    # create a lat-lon griddes file by infering the nominal resolution from XLAT, XLONG
    post_opr = get_cdoinput_preprocess(vname, config, fcst_files)
    pre_opr = ""
    for stat in config.stat:
        process(
            vname,
            pre_opr,
            post_opr,
            stat,
            fcst_files,
            refcst_files,
            config.preprocess.week_start,
        )


def calc_efi(
    tf: np.ndarray, tc: np.ndarray, return_extra=False
) -> float | tuple[float, float, float]:
    tc = np.sort(tc)
    tf = np.sort(tf)
    q = stats.percentileofscore(tc, tc)[:-1] / 100
    qfq = stats.percentileofscore(tf, tc)[:-1] / 100

    # Calculate fraction and EFI
    frac = (q - qfq) / np.sqrt(q * (1 - q))
    efi = 2 / math.pi * np.trapezoid(frac, x=q)
    if return_extra:
        return efi, q, qfq
    return efi


def compute_sot(
    tf: np.ndarray, tc: np.ndarray, quantile_method: str, tail: str
) -> np.ndarray:
    """
    Compute the Shift Of Tail (SOT) for climate and forecast data.
    Parameters:
    - tf: Sorted forecast data (2D array)
    - tc: Sorted climate data (2D array)
    - quantile_method: Method to calculate quantiles ('quick' or 'precise')
    - tail: 'pos' for positive shift, 'neg' for negative shift

    Returns:
    - SOT: Shift Of Tail (masked array with smoothed values)
    """
    nens, cens = tf.shape[0], tc.shape[0]

    # Define quantile indices based on tail type
    quantiles = {"pos": (0.9, 0.99), "neg": (0.1, 0.01)}
    q_low, q_high = quantiles[tail]

    if quantile_method == "quick":
        qc_low = tc[int(cens * q_low) - 1]
        qc_high = tc[int(cens * q_high) - 1]
        qf_low = tf[int(nens * q_low) - 1]
    else:
        qc_low, qc_high = np.nanquantile(tc, [q_low, q_high], axis=0)
        qf_low = np.nanquantile(tf, q_low, axis=0)

    # Compute Shift Of Tail
    sot = (qf_low - qc_high) / (qc_high - qc_low)
    sot_sm = np.ma.masked_invalid(ndimage.gaussian_filter(sot, sigma=1.5))

    return sot_sm


def calc_sotp(
    tf: np.ndarray, tc: np.ndarray, quantile_method: str = "precise"
) -> np.ndarray:
    """Calculate Positive Shift Of Tail (SOT)."""
    return compute_sot(tf, tc, quantile_method, tail="pos")


def calc_sotn(
    tf: np.ndarray, tc: np.ndarray, quantile_method: str = "precise"
) -> np.ndarray:
    """Calculate Negative Shift Of Tail (SOT)."""
    return compute_sot(tf, tc, quantile_method, tail="neg")


def preprocess_times(ds):
    ds = ds.assign_coords(
        Times=range(ds.dims["Times"])
    )  # Reset time to a simple range or keep it consistent
    return ds


def calc_efi3d(
    tf: np.ndarray, tc: np.ndarray, return_extra=False
) -> float | tuple[float, float, float]:
    print(tc)
    tc = np.sort(tc, axis=0)
    tf = np.sort(tf, axis=0)
    logger.info("Sorted arrays")
    q = np.array(
        [
            [
                [
                    stats.percentileofscore(tc[:, i, k, j], tc[:, i, k, j])[:-1] * 0.01
                    for j in range(tc.shape[3])
                ]
                for k in range(tc.shape[2])
            ]
            for i in range(tc.shape[1])
        ]
    )  # Remove last element of Q
    qfq = np.array(
        [
            [
                [
                    stats.percentileofscore(tf[:, i, k, j], tc[:, i, k, j])[:-1] * 0.01
                    for j in range(tc.shape[3])
                ]
                for k in range(tc.shape[2])
            ]
            for i in range(tc.shape[1])
        ]
    )  # Remove last element of Q

    # Calculate fraction and EFI
    frac = (q - qfq) / np.sqrt(q * (1 - q))
    efi = 2 / math.pi * np.trapezoid(frac, x=q)
    if return_extra:
        return efi, q, qfq
    return efi


def calc_sot_stats(
    vname: str,
    time_coarsen_name: str,
    ens_stat_name: str,
    file_type: FileType,
    fcst_files: list[str],
    clim_files: list[str],
    func: Callable,
) -> xr.DataArray:
    fcst = xr.open_mfdataset(fcst_files, combine="nested", concat_dim="member")[vname]
    clim = xr.open_mfdataset(
        clim_files, combine="nested", concat_dim="member", preprocess=preprocess_times
    )[vname]
    var = fcst.isel(member=0)
    fcst_sort = np.sort(fcst.values, axis=0)
    clim_sort = np.sort(clim.values, axis=0)
    for t in range(fcst_sort.shape[1]):
        var[t, :, :] = func(fcst_sort[:, t, :, :], clim_sort[:, t, :, :])
    var_ds = var.to_dataset()
    file_name = create_file_name(vname, time_coarsen_name, ens_stat_name)
    if file_type in [FileType.zarr, FileType.zarr_and_nc]:
        var_ds.to_zarr(f"{file_name}.zarr.zip", mode="w")
    if file_type in [FileType.nc, FileType.zarr_and_nc]:
        var_ds.to_netcdf(f"{file_name}.nc")

    fcst.close()


def calc_efi_stats(
    vname: str,
    time_coarsen_name: str,
    ens_stat_name: str,
    file_type: FileType,
    fcst_files: list[str],
    clim_files: list[str],
) -> xr.DataArray:
    fcst = xr.open_mfdataset(fcst_files, combine="nested", concat_dim="member")[vname]
    clim = xr.open_mfdataset(
        clim_files, combine="nested", concat_dim="member", preprocess=preprocess_times
    )[vname]
    var = fcst.isel(member=0)
    length = fcst.shape[1] * fcst.shape[2] * fcst.shape[3]
    cpu_count = max(psutil.cpu_count(logical=False) // 2, 1)
    nworkers = min(length, cpu_count)
    threads_per_worker = 1
    fcst_sort = np.sort(fcst.values, axis=0)
    clim_sort = np.sort(clim.values, axis=0)
    fcst2d = np.reshape(fcst_sort, (fcst.shape[0], length))
    clim2d = np.reshape(clim_sort, (clim.shape[0], length))
    # block_size * nworkers >= length
    block_size = int(np.ceil(length / nworkers))

    input_list = []
    for t in range(nworkers):
        sb = t * block_size
        eb = (t + 1) * block_size
        input_list.append((fcst2d[:, sb:eb], clim2d[:, sb:eb]))
    logger.info("Submitting parallel jobs")
    with LocalCluster(
        n_workers=nworkers, threads_per_worker=threads_per_worker
    ) as cluster, Client(cluster) as client:
        remote_input_list = client.scatter(input_list)
        futures = []
        for remote_input in remote_input_list:
            futures.append(client.submit(calc_efi1d, remote_input))
        result = client.gather(futures)
        for remote_input in remote_input_list:
            client.cancel(remote_input)
        del remote_input_list
    logger.info("Done parallel jobs")
    # result is a list of numpy arrays of length = nworkers, each array is of shape <= block_size
    # unravel the result to get the full array
    var[:, :, :] = np.concatenate(result).reshape(fcst.shape[1:])
    var_ds = var.to_dataset()
    file_name = create_file_name(vname, time_coarsen_name, ens_stat_name)
    if file_type in [FileType.zarr, FileType.zarr_and_nc]:
        var_ds.to_zarr(f"{file_name}.zarr.zip", mode="w")
    if file_type in [FileType.nc, FileType.zarr_and_nc]:
        var_ds.to_netcdf(f"{file_name}.nc")
    fcst.close()


def calc_efi1d(inp: tuple[np.ndarray, np.ndarray]) -> np.ndarray:
    tf, tc = inp
    q = np.array(
        [
            stats.percentileofscore(tc[:, j], tc[:, j])[:-1] * 0.01
            for j in range(tc.shape[1])
        ]
    )  # Remove last element of Q
    qfq = np.array(
        [
            stats.percentileofscore(tf[:, j], tc[:, j])[:-1] * 0.01
            for j in range(tc.shape[1])
        ]
    )  # Remove last element of Q

    # Calculate fraction and EFI
    frac = (q - qfq) / np.sqrt(q * (1 - q))
    efi = 2 / math.pi * np.trapezoid(frac, x=q)
    return efi


if __name__ == "__main__":
    app()
