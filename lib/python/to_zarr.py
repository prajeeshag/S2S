import glob
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Annotated

import typer
import xarray as xr
import zarr

app = typer.Typer()

# 7z a -tzip archive.zarr.zip archive.zarr/.


def process_and_write_to_zarr(zarr_store, nc_file, idx, dimname):
    print(f"Process ID: {os.getpid()} is processing file: {nc_file}")
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
    return f"Process ID: {os.getpid()} finised processing file: {nc_file}"


@app.command()
def main(
    ncfiles: Annotated[list[str], typer.Argument()],
    dimname: Annotated[str, typer.Option(...)],
    output: Annotated[str, typer.Option()],
    coord: Annotated[str, typer.Option()] = "",
):
    multiprocessing.set_start_method("fork", force=True)
    store = zarr.DirectoryStore(output)
    if coord:
        coordval = list(map(float, coord.split(",")))
    else:
        coordval = list(range(len(ncfiles)))

    if len(coordval) != len(ncfiles):
        raise ValueError(
            f"Number of coordinates ({len(coordval)}) does not match number of files ({len(ncfiles)})"
        )
    ds = xr.open_dataset(
        ncfiles[0],
        chunks={"lat": 10, "lon": 10},
    )
    ds = ds.expand_dims({dimname: len(ncfiles)}).chunk(
        {dimname: 1, "lat": 10, "lon": 10}
    )
    print(ds)
    ds.to_zarr(store, mode="w", compute=False)

    total_workers = os.cpu_count()
    print(f"Total workers: {total_workers}")
    workers = min(total_workers, len(ncfiles))
    print(f"Workers: {workers}")

    # Collect Future objects
    futures = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for idx, nc_file in enumerate(ncfiles):
            print(f"Submitting job for file: {nc_file} (index: {idx})")
            future = executor.submit(
                process_and_write_to_zarr, store, nc_file, idx, dimname
            )
            futures.append(future)

    # Wait for all futures to complete
    for future in as_completed(futures):
        try:
            # You can check the result if needed or just confirm completion
            result = future.result()
        except Exception as e:
            print(f"An error occurred: {e}")

    print("All jobs completed.")


if __name__ == "__main__":
    app()
