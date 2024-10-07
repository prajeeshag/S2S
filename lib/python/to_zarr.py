import glob
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

import xarray as xr
import zarr

# 7z a -tzip archive.zarr.zip archive.zarr/.

def process_and_write_to_zarr(zarr_store, nc_file, idx):
    print(f"Process ID: {os.getpid()} is processing file: {nc_file}")
    # Open the NetCDF file using xarray
    ds = xr.open_dataset(nc_file, chunks={"lat": 10, "lon": 10}).drop_vars(
        ["Times", "lon", "lat"]
    )
    ds = ds.expand_dims({"member": 1}).chunk({"member": 1, "lat": 10, "lon": 10})
    # Define the region where this process will write its data
    region = {"member": slice(idx, idx + 1)}
    # Write the data to the Zarr store
    ds.to_zarr(zarr_store, region=region)
    # Close the dataset
    ds.close()
    return f"Process ID: {os.getpid()} finised processing file: {nc_file}"


if __name__ == "__main__":
    multiprocessing.set_start_method("fork", force=True)

    idir = "ll_data/"
    paths = glob.glob(f"{idir}/surface_d01_T2_*")
    paths.sort()
    print(paths)
    ds = xr.open_dataset(
        paths[0],
        chunks={"lat": 10, "lon": 10},
    )
    ds = ds.expand_dims({"member": 51}).chunk({"member": 1, "lat": 10, "lon": 10})
    # store = zarr.ZipStore("test.zarr.zip")
    store = zarr.DirectoryStore("test.zarr")
    print(ds)
    ds.to_zarr(store, mode="w", compute=False)
    print("To zarr compute false completed")
    total_workers = os.cpu_count()
    print(f"Total workers: {total_workers}")
    workers = min(total_workers, len(paths))
    print(f"Workers: {workers}")
    # Use ProcessPoolExecutor to run the processing in parallel
    # Collect Future objects
    futures = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for idx, nc_file in enumerate(paths):
            print(f"Submitting job for file: {nc_file} (index: {idx})")
            future = executor.submit(process_and_write_to_zarr, store, nc_file, idx)
            futures.append(future)

    # Wait for all futures to complete
    for future in as_completed(futures):
        try:
            # You can check the result if needed or just confirm completion
            result = future.result()
        except Exception as e:
            print(f"An error occurred: {e}")

    print("All jobs completed.")
