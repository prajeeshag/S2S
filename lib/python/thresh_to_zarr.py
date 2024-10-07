import xarray as xr
import re
import zarr
import typer

app = typer.Typer()


def _get_pctl_from_filename(filename: str):
    # Define a regex pattern to extract the number between 'thresh' and '_'
    pattern = r"thresh(\d+)_"

    # Use re.search to find the pattern in the filename
    match = re.search(pattern, filename)

    if match:
        # Extract the matched value
        percentile_value = match.group(1)
        return percentile_value
    else:
        raise ValueError("Pattern not found in the filename.")

@app.command()
def to_zarr(input_files: list[str], output_path: str):
    pctls = [int(_get_pctl_from_filename(f)) for f in input_files]
    combined = sorted(zip(pctls, input_files), key=lambda x: x[0])
    sorted_pctls, sorted_input_files = zip(*combined)
    pctls = list(sorted_pctls)
    input_files = list(sorted_input_files)
    
    ds = xr.open_mfdataset(input_files, combine="nested", concat_dim="thresholds")
    ds = ds.assign_coords(pctl=("thresholds", pctls))
    ds = ds.chunk({'lon': 10, 'lat': 10})

    store = zarr.ZipStore(output_path)
    ds.to_zarr(store)