#!/bin/bash
# shellcheck disable=SC2154,SC2086,SC1091,SC2034
set -e
set -o pipefail

source "$(dirname "$0")/koi"
koiname="$0"
koidescription="Merge in times and split in variables. Fixes time coordinates."

__koimain() {
  __addarg "-h" "--help" "help" "optional" "" "$koidescription" ""
  __addarg "-o" "--obase" "storevalue" "required" "" "Output filename base" ""
  __addarg "-r" "--remove" "flag" "optional" "" "Remove input files" ""
  __addarg "" "inputs" "positionalarray" "required" "" "WRF output file(s) to be merged" ""
  __parseargs "$@"

  set -u

  incHour=""
  cdoInput=""
  for input in "${inputs[@]}"; do
    date1=$(ncks -v Times -d Time,0 $input | grep -A 2 'data:' | sed -n 's/.*"\(.*\)".*/\1/p')
    sdate=${date1:0:10}
    stime=${date1:11:8}

    if [ -z $incHour ]; then
      date2=$(ncks -v Times -d Time,1 $input | grep -A 2 'data:' | sed -n 's/.*"\(.*\)".*/\1/p')
      if [ -z $date2 ]; then
        incHour=1hour
      else
        hour=$(isodatetime --parse-format "%Y-%m-%d_%H:%M:%S" $date1 $date2 --as-total H)
        incHour="$(printf "%.0f" $hour)"hour
      fi
    fi

    cdoInput="$cdoInput -settaxis,$sdate,$stime,$incHour $input"
  done

  CDO_FILE_SUFFIX=".nc"

  echo cdo -splitvar -mergetime $cdoInput ${obase}
  cdo -r -splitvar -mergetime $cdoInput ${obase}

  if [ "$remove" -eq 1 ]; then
    rm -f "${inputs[@]}"
  fi
}

__koirun "$@"
