#!/bin/bash
# shellcheck disable=SC2154,SC2086,SC1091,SC2034
set -e

source "$(dirname "$0")/koi"
koiname="$0"
koidescription="Extract a single ensemble member from the ECMWF S2S data"

__koimain() {
  __addarg "-h" "--help" "help" "optional" "" "$koidescription" ""
  __addarg "-i" "--input" "storevalue" "required" "" "Input grib data" ""
  __addarg "-m" "--member" "storevalue" "required" "" "Ensemble member" ""
  __addarg "-o" "--output" "storevalue" "optional" "" "Output grib data" ""
  __addarg "-n" "--nday" "storevalue" "optional" "46" "Forecast length in days" ""
  __addarg "" "--year" "storevalue" "optional" "" "Forecast year (used to extract reforecast)" ""
  __addarg "" "--lsminput" "storevalue" "optional" "" "Input grib data for land-sea mask (because LSM not available in Reforecast)" ""

  __parseargs "$@"

  set -u

  inv=${input}.inv

  if [[ ! -e $inv ]]; then
    inv=inventory
    wgrib -s "$input" >"$inv"
  fi

  if [[ "$member" -eq 0 ]]; then
    search_string="Control forecast 0:"
  else
    search_string="Perturbed forecast $member:"
  fi

  # Extract the year if needed
  year_search_string="d=${year: -2}"

  tmpfile=data.mem${member}
  grep "$search_string" <"$inv" | grep "$year_search_string" | wgrib -i "$input" -s -grib -o "$tmpfile" >/dev/null

  mkdir -p sorted
  wgrib -s $tmpfile >inv

  # Sort in time for ungrib compatibility
  grep ":anl:" <inv | wgrib -i $tmpfile -s -grib -o "${tmpfile}.0000" >/dev/null
  for ((k = 1; k <= $((nday * 2)); k++)); do
    j=$((k * 12))
    jj=$(printf "%04d\n" $j)
    grep ":${j}hr fcst:" <inv | wgrib -i "$tmpfile" -s -grib -o "${tmpfile}.${jj}" >/dev/null
  done
  cat "${tmpfile}".* >"sorted_${tmpfile}"
  rm inv "${tmpfile}".*

  # Get land-sea mask
  if [ -n "$lsminput" ]; then
    wgrib "$lsminput" | grep LSM | wgrib -s -grib -i "${lsminput}" -o lsm.grb
  else
    grep LSM <"$inv" | wgrib -s -grib -i "${input}" -o lsm.grb
  fi

  # Patching SST with buffer zone so that metgrid interplotes it correctly
  cdo -s -fillmiss2 -ifthen -ltc,0.01 lsm.grb -selvar,var34 "sorted_$tmpfile" "SST_$tmpfile"

  cdo -s -replace "sorted_$tmpfile" "SST_$tmpfile" "$output"
}

__koirun "$@"
