Temperature [Refer Slide-1]
----------------------------
Path: kw61469:/home/pammirp/data/ncm/subseasonal/temperature/stage

1. Weekly spatial (ensmedian Spatial) - T2_weekly_mean_ensmedian.nc
2. Hourly Time-series (Pecentile) - T2_enspctl.zarr.zip
3. Daily Time-series (median with range) - T2_daily_max.zarr.zip
4. Daily Time-series (Probability of air Temperature > threshold) - T2_daily_max_gtc_threshold.zarr.zip
5. Daily Table (Probability of air Temperature > threshold) - T2_daily_max_gtc_threshold.zarr.zip
6. Cumulative distribution function - T2_weekly.zarr.zip, reforecast/T2_weekly.zarr.zip, s2s.get_cdf()[s2s.py]
7. Weekly spatial (Extreme Forecast Index) - T2_weekly_mean_[efi,sotp,sotn].nc




Rainfall [Refer Slide-2]
------------------------
Path: kw61469:/home/pammirp/data/ncm/subseasonal/rainfall/stage

1. Weekly spatial (Probability > threshold) - RAIN_weekly_gtc_threshold.nc
2. Weekly spatial (ensmedian) - RAIN_weekly_ensmedian.nc
3. Daily time-series (Accumulated) - (Pending...)
4. Weekly Time-series (median with range) - (Pending...)
5. Weekly Time-series (Probability  > threshold) - RAIN_daily_max_gtc_threshold.zarr.zip
6. Weekly Table (Probability > threshold) - RAIN_daily_max_gtc_threshold.zarr.zip
7. Cumulative distribution function - RAIN_weekly.zarr.zip, reforecast/RAIN_weekly.zarr.zip, s2s.get_cdf()[s2s.py]
8. Weekly spatial (Extreme Forecast Index) - RAIN_weekly_mean_[efi,sotp,sotn].nc


NOTE: the above numbers are same as the figure numbers from the slides






(Extreme Forecast Index Spatial Map) [Slide-1<fig.7>, Slide-2<fig.8>]
------------------------------------

Data files:
    - <VAR>_weekly_mean_efi.nc
    - <VAR>_weekly_mean_sotn.nc
    - <VAR>_weekly_mean_sotp.nc

Description:
    The Extreme Forecast Index (EFI) is stored in "<VAR>_weekly_mean_efi.nc".
    We plot this index in a spartial map with shades. The colomap provided in the presentation.
    The Shift of Tails (SOT) is stored in "<VAR>_weekly_mean_sotn.nc" for negative values and "<VAR>_weekly_mean_sotp.nc" for positive values.
    We plot this index with contours overlay over the EFI map.
    Temperture should be plot with two contours levels (0.5 for positive and -0.5 for negative) while Precipitation needs only one contour (0.5).



CDF plot for one grid point [Slide-1<fig.6>, Slide-2<fig.7>]
------------------------------------------------------------
Date files:
    - <VAR>_weekly.zarr.zip
    - reforecast/<VAR>_weekly.zarr.zip

Retrival function:
    - from s2s.py import get_cdf

Description:
    Use the get_cdf function from s2s.py to retrive the data required for ploting the CDF.
    Please check the s2s.get_cdf docstring for usage and the ploting example in s2s.__main__


