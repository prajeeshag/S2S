import math
import time

import numpy as np
import scipy.stats as stats
import xarray as xr


def calc_efi(
    tf: np.ndarray, tc: np.ndarray, return_extra=False
) -> float | tuple[float, float, float]:
    tf = np.sort(tf)
    tc = np.sort(tc)
    Q = stats.percentileofscore(tc, tc)[:-1] / 100
    QfQ = stats.percentileofscore(tf, tc)[:-1] / 100

    # Calculate fraction and EFI
    frac = (Q - QfQ) / np.sqrt(Q * (1 - Q))
    efi = 2 / math.pi * np.trapezoid(frac, x=Q)
    if return_extra:
        return efi, Q, QfQ
    return efi


def calc_cdf(
    tf: np.ndarray, tc: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    tf = np.sort(tf)
    tc = np.sort(tc)
    # Calculate percentiles exclude the last element due to Q = 1 making infinity frac
    min_value = min(tf[0], tc[0]) - 1
    max_value = max(tf[-1], tc[-1]) + 1
    elements_lt_min_tc = tf[tf < tc[0]]
    elements_gt_max_tc = tf[tf > tc[-1]]
    Tplot = np.concatenate(
        [
            np.array([min_value]),
            elements_lt_min_tc,
            tc,
            elements_gt_max_tc,
            np.array([max_value]),
        ]
    )
    Qplot = stats.percentileofscore(tc, Tplot) / 100
    QfQplot = stats.percentileofscore(tf, Tplot) / 100
    return Tplot, Qplot, QfQplot


def get_cdf(
    tf_da: xr.DataArray,
    tc_da: xr.DataArray,
    time_index: int,
    lonlat: tuple[float, float] | None = None,
    index: tuple[int, int] | None = None,
) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    float,
]:
    """
    Calculate CDF for given forecast and climate data arrays.

    Args:
        tf_da: Forecast data, xarray data array
        tc_da: Reforecast data,  xarray data array
        time_index: Time index
        lonlat: Tuple of (lon, lat) coordinates
        index: Tuple of (x, y) indices

    Returns:
        (Tplot, Qplot, QfQplot, Tc, Tf, Q, QfQ, efi)
    """
    if lonlat:
        lon, lat = lonlat
        tc = tc_da.sel(lon=lon, lat=lat, method="nearest").isel(Times=time_index)
        tf = tf_da.sel(lon=lon, lat=lat, method="nearest").isel(Times=time_index)
    elif index:
        x, y = index
        tc = tc_da.isel(lon=x, lat=y, Times=time_index)
        tf = tf_da.isel(lon=x, lat=y, Times=time_index)
    else:
        raise ValueError("Either 'lonlat' or 'index' must be specified.")
    Tplot, Qplot, QfQplot = calc_cdf(tf, tc)
    efi, Q, QfQ = calc_efi(tf, tc, return_extra=True)
    return Tplot, Qplot, QfQplot, tc, tf, Q, QfQ, efi


if __name__ == "__main__":
    # Example CDF ploting

    import matplotlib.pyplot as plt

    clim = xr.open_zarr(
        "/scratch/athippp/cylc-run/S2SF/run1/share/20240919T0000Z/viz/T2/stage/T2_weekmean_ensmembers_rf.zarr.zip"
    )["T2"]
    fcst = xr.open_zarr(
        "/scratch/athippp/cylc-run/S2SF/run1/share/20240919T0000Z/viz/T2/stage/T2_weekmean_ensmembers.zarr.zip"
    )["T2"]
    print(clim)
    lon = 40.22
    lat = 20.66
    start_time = time.time()
    Tplot, Qplot, QfQplot, Tc, Tf, Q, QfQ, efi = get_cdf(
        fcst, clim, time_index=0, lonlat=(lon, lat)
    )
    print(f"Time for fetching data: {time.time() - start_time}")

    plt.plot(Tplot, Qplot, "-", color="black", label="M-Climate")
    plt.plot(Tplot, QfQplot, "-", color="red", label="ENS-Forecast")
    plt.text(
        Tc[len(Tc) // 2] - 0.2, Q[len(Q) // 2], "M-Climate", color="black", ha="right"
    )
    plt.text(Tc[-1] - 0.2, QfQ[-1], "ENS-Forecast", color="red", ha="right")

    # Add vertical lines
    plt.vlines(
        [Tc[0], Tc[-1]],
        ymin=0,
        ymax=[min(Q[0], QfQ[0]), min(Q[-1], QfQ[-1])],
        color="cornflowerblue",
        linestyle="-",
        label="Tc min / T max",
    )
    plt.vlines(
        [Tc[0], Tc[-1]],
        ymin=[min(Q[0], QfQ[0]), min(Q[-1], QfQ[-1])],
        ymax=[Q[0], Q[-1]],
        color="cornflowerblue",
        linestyle="--",
    )

    # Add text box with EFI value
    plt.text(
        0.05,
        0.95,
        f"EFI = {efi * 100:.1f} %",
        transform=plt.gca().transAxes,
        fontsize=12,
        verticalalignment="top",
        bbox=dict(facecolor="white", alpha=0.5),
    )

    # Add labels and title
    plt.xlabel("Temperature (°C)")
    plt.ylabel("Probability (%) not exceed threshold")
    plt.title(f"Cumulative Distribution Function (CDF) at {lon}E, {lat}N")
    plt.grid(True)
    plt.savefig("cdf.png")
