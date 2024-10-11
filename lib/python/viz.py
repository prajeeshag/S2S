import datetime
import glob
import os
from pathlib import Path
import shutil
import subprocess
import time
from cdo import Cdo
import typer
from typing import Annotated
import xarray as xr
import tempfile
from dask.distributed import Client, LocalCluster
import multiprocessing as mp

import yaml
import zarr
import logging

# setup logger to use stdout
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


app = typer.Typer()

cdo = Cdo(tempdir="./tmp", silent=False)


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
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    with open(temp_file.name, "w") as f:
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
    return temp_file.name


def cdo_execute(input, output):
    if Path(output).exists():
        logger.info(f"{output} already exists; skipping")
        return output
    logger.info(f"Processing: cdo {input} {output}")
    cdo.copy(input=input, output=output)
    logger.info(f"Completed processing: cdo {input} {output}")
    return output


def get_config():
    current_dir = Path(__file__).parent
    yaml_file_path = current_dir / "viz.yaml"
    with open(yaml_file_path, "r") as file:
        data = yaml.safe_load(file)
        return data


CONFIG = get_config()


def process_rename_var(vname):
    if vname not in CONFIG["fields"]:
        return ""
    rename = CONFIG["fields"][vname].get("rename_from", "")
    if rename:
        return f" -chname,{rename},{vname}"
    return ""


def process_unit_conversion(vname):
    cdoOpt = ""
    if vname not in CONFIG["fields"]:
        unit_conversion = False
    else:
        unit_conversion = CONFIG["fields"][vname].get("unit_conversion", False)

    if unit_conversion:
        addc = CONFIG["fields"][vname].get("addc", 0)
        mulc = CONFIG["fields"][vname].get("mulc", 1)
        units = CONFIG["fields"][vname].get("units", "")
        cdoOpt = f" -setattribute,{vname}@units={units} -addc,{addc} -mulc,{mulc}"

    return cdoOpt


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


def to_zarr(ncfiles: list[str], coord: list[int], dimname: str, output: str):
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
            write_to_zarr,
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


def write_to_zarr(zarr_store, nc_file, idx, dimname):
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


def remap_wgts(inputfiles):
    ds = xr.open_dataset(inputfiles[0], chunks={"Time": 1})
    xlat = ds["XLAT"]
    xlong = ds["XLONG"]
    res = xlat[1, 0].values - xlat[0, 0].values
    minlat = xlat.min().values - res
    maxlat = xlat.max().values + res
    minlon = xlong.min().values - res
    maxlon = xlong.max().values + res
    xsize = int((maxlon - minlon) / res)
    ysize = int((maxlat - minlat) / res)
    ds.close()

    griddes_file = griddes(xsize, ysize, minlon, minlat, res)
    # remap to Lat-Lon
    remap_wgt_file = cdo.gennn(griddes_file, input=inputfiles[0])
    return griddes_file, remap_wgt_file


def cal_skip_days(current_day, desired_start):
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


def get_weekday(date_string):
    """
    Get the weekday for a given date.

    Parameters:
    date_string (str): The date in 'YYYYMMDD*' format.

    Returns:
    int: The weekday (Monday=0, Sunday=6).
    """
    date = datetime.datetime.strptime(date_string[0:10], "%Y-%m-%d")
    return date.weekday()  # Monday=0, Sunday=6


def add_days_to_date(date_str: str, days: int) -> str:
    # Parse the input date string into a datetime object
    date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")

    # Add the specified number of days using timedelta
    new_date = date_obj + datetime.timedelta(days=days)

    # Return the new date as a string in the same format
    return new_date.strftime("%Y-%m-%d")


def weekly_mean(inputfile, outputfile):
    start_date = cdo.showtimestamp(input=f"-seltimestep,1 {inputfile}")[0][0:10]
    end_date = cdo.showtimestamp(input=f"-seltimestep,-1 {inputfile}")[0][0:10]
    weekday = get_weekday(start_date)
    offset = cal_skip_days(weekday, CONFIG["week_start"])
    start_date = add_days_to_date(start_date, offset)
    input = f" -timselmean,7 -daymean -seldate,{start_date},{end_date} {inputfile}"
    cdo_execute(input, outputfile)


def to_weekly(inputfiles):
    outputfiles = []
    for nc_file in inputfiles:
        outputfile = f"weekly_mean_{Path(nc_file).name}"
        outputfiles.append(outputfile)
    cpu_count = os.cpu_count()
    nworkers = min(len(inputfiles), cpu_count)
    threads_per_worker = int(cpu_count / nworkers)
    with LocalCluster(
        n_workers=nworkers, threads_per_worker=threads_per_worker
    ) as cluster, Client(cluster) as client:
        futures = client.map(weekly_mean, inputfiles, outputfiles)
        client.gather(futures)
    return outputfiles


def daily(inputfile, outputfile, operator):
    input = f"-day{operator} {inputfile}"
    cdo_execute(input, outputfile)


def to_daily(inputfiles, operator):
    outputfiles = []
    for nc_file in inputfiles:
        outputfile = f"daily{operator}_{Path(nc_file).name}"
        outputfiles.append(outputfile)
    cpu_count = os.cpu_count()
    nworkers = min(len(inputfiles), cpu_count)
    threads_per_worker = int(cpu_count / nworkers)
    with LocalCluster(
        n_workers=nworkers, threads_per_worker=threads_per_worker
    ) as cluster, Client(cluster) as client:
        futures = client.map(
            daily, inputfiles, outputfiles, [operator] * len(inputfiles)
        )
        client.gather(futures)
    return outputfiles


def process_daily(vname, preprocfiles, operator):
    try:
        daily = CONFIG["fields"][vname][f"daily_{operator}"]
    except KeyError:
        return
    if not daily:
        return

    daily_files = to_daily(preprocfiles, operator=operator)
    ensstat_daily_files = to_ens_stat(
        daily_files, ["min", "median", "max"], f"{vname}_daily_{operator}"
    )

    output = (f"{vname}_daily_{operator}.zarr",)
    if Path(f"{output}.zip").exists():
        logger.info(f"{output}.zip already exists; skipping")
    else:
        to_zarr(
            ensstat_daily_files,
            ["min", "median", "max"],
            "stat",
            f"{vname}_daily_{operator}.zarr",
        )

    try:
        thresholds = CONFIG["fields"][vname][f"daily_{operator}_thresholds"]
    except KeyError:
        thresholds = False

    if thresholds:
        if operator == "min":
            to_ens_prob(daily_files, "ltc", thresholds, f"{vname}_daily_{operator}")
        elif operator == "max":
            to_ens_prob(daily_files, "gtc", thresholds, f"{vname}_daily_{operator}")


def to_ens_prob(inputfiles, toperator, thresholds, vname):
    cpu_count = os.cpu_count()
    nworkers = min(len(thresholds), cpu_count)
    threads_per_worker = int(cpu_count / nworkers)
    with LocalCluster(
        n_workers=nworkers, threads_per_worker=threads_per_worker
    ) as cluster, Client(cluster) as client:
        futures = client.map(
            ens_prob,
            [inputfiles] * len(thresholds),
            [toperator] * len(thresholds),
            thresholds,
            [vname] * len(thresholds),
        )
        res = client.gather(futures)
    logger.info(f"Processed: {res} for {thresholds}")
    to_zarr(res, thresholds, "prob", f"{vname}_{toperator}_threshold.zarr")


def ens_prob(input_files, operator, threshold, vname):
    input_files = " ".join(input_files)
    opr = f"-{operator},{threshold}"
    opr_str = f"{operator}{threshold}"
    output = f"{vname}_{opr_str}.nc"
    logger.info(f"Processing: {output}")
    input = f" -ensmean [ {opr} : {input_files} ] "
    cdo_execute(input, output)
    logger.info(f"Processed: {output}")
    return output


def process_weekly(vname, preprocfiles, operator):
    try:
        flag = CONFIG["fields"][vname][f"weekly_{operator}"]
    except KeyError:
        return
    if not flag:
        return
    weekly_mean_files = to_weekly(preprocfiles)
    ens_stat(weekly_mean_files, "median", f"{vname}_weekly_mean")


def to_ens_stat(inputfiles, operators, vname):
    cpu_count = os.cpu_count()
    nworkers = min(len(operators), cpu_count)
    threads_per_worker = int(cpu_count / nworkers)
    with LocalCluster(
        n_workers=nworkers, threads_per_worker=threads_per_worker
    ) as cluster, Client(cluster) as client:
        futures = client.map(
            ens_stat,
            [inputfiles] * len(operators),
            operators,
            [vname] * len(operators),
        )
        res = client.gather(futures)
    return res


def ens_stat(input_files, operator, vname):
    input_files = " ".join(input_files)
    opr_str = operator.replace(",", "")
    output = f"{vname}_ens{opr_str}.nc"
    logger.info(f"Processing: {output}")

    input = f" -ens{operator} [ {input_files} ] "
    cdo_execute(input, output)
    logger.info(f"Processed: {output}")
    return output


def process_percentiles(vname, preprocfiles):
    try:
        pecentiles = CONFIG["fields"][vname].get("percentiles", [])
    except KeyError:
        return
    if not pecentiles:
        return
    output = f"{vname}_pctl.zarr"
    if Path(f"{output}.zip").exists():
        logger.info(f"{output}.zip already exists; skipping")
    pctl = [f"pctl,{i}" for i in pecentiles]
    ens_stat_files = to_ens_stat(preprocfiles, pctl, vname)
    to_zarr(ens_stat_files, pecentiles, "pctl", output)


def process_to_zarr(vname, preprocfiles, members):
    try:
        flag = CONFIG["fields"][vname]["to_zarr"]
    except KeyError:
        return
    if not flag:
        return
    output = f"{vname}.zarr"
    if Path(f"{output}.zip").exists():
        logger.info(f"{output}.zip already exists; skipping")
        return
    stime = time.time()
    to_zarr(preprocfiles, members, "member", output)
    logger.info(f"Time for to_zarr: {time.time() - stime}")


@app.command()
def main(
    vname: Annotated[str, typer.Option(...)],
    inputfiles: Annotated[
        list[str],
        typer.Argument(help="WRF output file(s)"),
    ],
):

    members = get_member_coord(inputfiles)
    members, inputfiles = sort_by_coord(members, inputfiles)

    # create a lat-lon griddes file by infering the nominal resolution from XLAT, XLONG
    griddes_file, remap_wgt_file = remap_wgts(inputfiles)
    cdoOpt = f"-remap,{griddes_file},{remap_wgt_file}"

    cdoOpt = f"{process_rename_var(vname)} {cdoOpt}"
    cdoOpt = f"{process_unit_conversion(vname)} {cdoOpt}"
    preprocfiles = preproc(inputfiles, cdoOpt)

    process_to_zarr(vname, preprocfiles, members)

    process_percentiles(vname, preprocfiles)

    process_weekly(vname, preprocfiles, "mean")

    process_daily(vname, preprocfiles, "min")
    process_daily(vname, preprocfiles, "max")
    process_daily(vname, preprocfiles, "mean")
