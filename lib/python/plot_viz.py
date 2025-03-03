import cartopy.crs as ccrs
import matplotlib.colors as mpc
import matplotlib.pyplot as plt
import typer
import xarray as xr

app = typer.Typer()


@app.command()
def plot_efi(file_name, var_name):
    """
    Plot the Extreme Forecast Index (EFI) with Shift Of Tails (SOT).

    Parameters:
    - Var: Variable to plot
    - EFI: Extreme Forecast Index data (2D array)
    - SOTp: Positive Shift Of Tails data (2D array)
    - SOTn: Negative Shift Of Tails data (2D array)
    - lons: Longitudes for the grid
    - lats: Latitudes for the grid
    - t: Time index (for labeling the week)
    - path: Path to additional data (like city locations)
    - out_dir: Output directory for saving the plot
    - valid: Formatted string for the base and valid time range
    """
    # Set projection and color settings
    cols = [
        "#ff02dd",
        "#0160ff",
        "#00ceff",
        "#04ffa4",
        "#94e301",
        "#c0ec6f",
        "#e0f9b9",
        "#ffffff",
        "#eae9d7",
        "#fbfba8",
        "#f1f202",
        "#fad403",
        "#fea500",
        "#ff6600",
        "#ff0100",
    ]
    lvls = [
        -1,
        -0.9,
        -0.8,
        -0.7,
        -0.6,
        -0.5,
        -0.3,
        -0.1,
        0.1,
        0.3,
        0.5,
        0.6,
        0.7,
        0.8,
        0.9,
        1,
    ]

    # Load forecast data
    efi_ = xr.open_dataset(f"{file_name}_efi.nc")[var_name]
    sotp_ = xr.open_dataset(f"{file_name}_sotp.nc")[var_name]
    sotn_ = xr.open_dataset(f"{file_name}_sotn.nc")[var_name]
    lons = efi_["lon"]
    lats = efi_["lat"]

    # Create colormap and normalization
    efi_cmap = mpc.ListedColormap(cols, "efi")
    norm = mpc.BoundaryNorm(lvls, efi_cmap.N)

    for t in range(efi_.shape[0]):
        print(f"ploting {var_name} for time {t}")
        # Plot setup
        proj = ccrs.PlateCarree()
        fig, ax = plt.subplots(figsize=(8, 6), subplot_kw={"projection": proj})

        # Plot the EFI data
        fill = ax.contourf(
            lons, lats, efi_[t, :, :], lvls, cmap=efi_cmap, norm=norm, transform=proj
        )

        # Add SOTp and SOTn contours
        ax.contour(
            lons, lats, sotp_[t, :, :], levels=[0.5], colors="black", linewidths=1.0
        )
        ax.contour(
            lons, lats, sotn_[t, :, :], levels=[-0.5], colors="black", linewidths=1.0
        )

        # Add colorbar and titles
        plt.colorbar(fill, ticks=lvls, ax=ax, shrink=0.8)
        plt.title(
            f"Extreme Forecast Index - Week {t+1}",
            loc="center",
            fontsize=14,
            fontweight="bold",
            pad=30,
        )

        # Save the plot
        plt.savefig(f"{file_name}_efi_t{t}.png", dpi=200, bbox_inches="tight")
        plt.close()


if __name__ == "__main__":
    app()
