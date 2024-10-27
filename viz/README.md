
## NOTE
- Refer the `S2S-visualization-catalog.pdf` for the figures
- The below numbering for each item are the same figure numbers from the slides
- Tick Marks
    - :heavy_check_mark: : done but yet to finalize
    - :white_check_mark: : finalized

# Visualization catalog
## Temperature [Refer Slide-1]

| No. | Description | Files | Data Status | Viz Status |
| :-- | :-- | :-- | :-- | :-- |
| 1. |  Weekly spatial (ensmedian Spatial) | T2_weekmean_ensmedian.nc | :heavy_check_mark: | 
| 2. |  Hourly Time-series (Pecentile) | T2_enspctl.zarr.zip | :heavy_check_mark: | 
| 3. |  Daily Time-series (daily maximum RH min,median,max) | T2_daymax_ensrange.zarr.zip | :heavy_check_mark: | 
| 4. |  Daily Time-series (Probability of daily maximum RH > threshold) | T2_daymax_ensprob_gtc.zarr.zip | :heavy_check_mark: | 
| 5. |  Daily Table (Probability of daily maximum RH > threshold) | T2_daymax_ensprob_gtc.zarr.zip | :heavy_check_mark: | 
| 3. |  Daily Time-series (daily minimum RH min,median,max) | T2_daymin_ensrange.zarr.zip | :heavy_check_mark: | 
| 4. |  Daily Time-series (Probability of daily minimum RH < threshold) | T2_daymin_ensprob_ltc.zarr.zip | :heavy_check_mark: | 
| 5. |  Daily Table (Probability of daily minimum RH < threshold) | T2_daymin_ensprob_ltc.zarr.zip | :heavy_check_mark: | 
| 6. |  Cumulative distribution function | T2_weekmean_ensmembers.zarr.zip, T2_weekmean_ensmembers_rf.zarr.zip, s2s.get_cdf()[s2s.py] | :heavy_check_mark: | 
| 7. |  Weekly spatial (Extreme Forecast Index) | T2_weekmean_efi.nc | :heavy_check_mark: | 

Data Path: kw61469:/home/athippp/data/ncm/subseasonal/T2/stage

## Rainfall [Refer Slide-2]
| No. | Description | Files | Data Status | Viz Status |
| :-- | :-- | :-- | :-- | :-- |
| 1. | Weekly spatial (Probability > threshold) | Rainfall_weeksum_ensprob_gtc.zarr.zip | :heavy_check_mark: | 
| 2. | Weekly spatial (ensmedian) | Rainfall_weeksum_ensmedian.nc | ✔️ | 
| 3. | Daily time-series (Accumulated) | Rainfall_daycumsum_ensrange.zarr.zip | ✔️ | 
| 4. | Weekly Time-series (median with range) | Rainfall_weeksum_ensrange.zarr.zip | ✔️ | 
| 5. | Weekly Time-series (Probability  > threshold) | Rain_weeksum_ensprob_gtc.zarr.zip | :heavy_check_mark: | 
| 6. | Weekly Table (Probability > threshold) | Rainfall_weeksum_ensprob_gtc.zarr.zip  | :heavy_check_mark: | 
| 7. | Cumulative distribution function | Rainfall_weeksum_ensmembers.zarr.zip, Rainfall_weeksum_ensmembers_rf.zarr.zip, s2s.get_cdf()[s2s.py] | :heavy_check_mark: | 
| 8. | Weekly spatial (Extreme Forecast Index) | Rainfall_weeksum_efi.nc | :heavy_check_mark: | 

Data Path: kw61469:/home/athippp/data/ncm/subseasonal/Rainfall/stage


## Relative Humidity [Similar to Temperature, refer Slide-1]

| No. | Description | Files | Data Status | Viz Status |
| :-- | :-- | :-- | :-- | :-- |
| 1. |  Weekly spatial (ensmedian Spatial) | RH2_weekmean_ensmedian.nc | :heavy_check_mark: | 
| 2. |  Hourly Time-series (Pecentile) | RH2_enspctl.zarr.zip | :heavy_check_mark: | 
| 3. |  Daily Time-series (daily maximum RH min,median,max) | RH2_daymax_ensrange.zarr.zip | :heavy_check_mark: | 
| 4. |  Daily Time-series (Probability of daily maximum RH > threshold) | RH2_daymax_ensprob_gtc.zarr.zip | :heavy_check_mark: | 
| 5. |  Daily Table (Probability of daily maximum RH > threshold) | RH2_daymax_ensprob_gtc.zarr.zip | :heavy_check_mark: | 
| 3. |  Daily Time-series (daily minimum RH min,median,max) | RH2_daymin_ensrange.zarr.zip | :heavy_check_mark: | 
| 4. |  Daily Time-series (Probability of daily minimum RH < threshold) | RH2_daymin_ensprob_ltc.zarr.zip | :heavy_check_mark: | 
| 5. |  Daily Table (Probability of daily minimum RH < threshold) | RH2_daymin_ensprob_ltc.zarr.zip | :heavy_check_mark: | 
| 6. |  Cumulative distribution function | RH2_weekmean_ensmembers.zarr.zip, RH2_weekmean_ensmembers_rf.zarr.zip, s2s.get_cdf()[s2s.py] | :heavy_check_mark: | 
| 7. |  Weekly spatial (Extreme Forecast Index) | RH2_weekmean_efi.nc | :heavy_check_mark: | 

Data Path: kw61469:/home/athippp/data/ncm/subseasonal/RH2/stage

# Python Code
- s2s.py


# Details

## Extreme Forecast Index Spatial Map [Slide-1<fig.7>, Slide-2<fig.8>]

Data files:

- `<VAR>_weekly_mean_efi.nc`
- `<VAR>_weekly_mean_sotn.nc`
- `<VAR>_weekly_mean_sotp.nc`

Description:The Extreme Forecast Index (EFI) is stored in `<VAR>_weekly_mean_efi.nc`.
We plot this index in a spartial map with shades. The colomap provided in the presentation.
The Shift of Tails (SOT) is stored in `<VAR>_weekly_mean_sotn.nc` for negative values and `<VAR>_weekly_mean_sotp.nc` for positive values.
We plot this index with contours overlay over the EFI map.
Temperture should be plot with two contours levels (0.5 for positive and -0.5 for negative) while Precipitation needs only one contour (0.5).



## CDF plot for one grid point [Slide-1<fig.6>, Slide-2<fig.7>]
Data files:

- `<VAR>_weekly.zarr.zip`
- `reforecast/<VAR>_weekly.zarr.zip`

Retrival function:`from s2s.py import get_cdf`

Description: Use the get_cdf function from s2s.py to retrive the data required for ploting the CDF.
    Please check the s2s.get_cdf docstring for usage and the ploting example in s2s.__main__


