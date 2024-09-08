#!/bin/bash
# shellcheck disable=SC2154,SC2086,SC1091,SC2034
set -e
set -o pipefail

source "$(dirname "$0")/koi"
koiname="$0"
koidescription="Calculate weekly mean from time merged WRF output"

function __verify_weekstart {
    # verify that $1 is a valid HTTP method
    if [[ "$1" -lt 0 ]] || [[ "$1" -gt 6 ]]; then 
        __errortext "$koiname: err: invalid weekstart: $1"
        return 1
    fi
}

# Function to calculate days to skip for the nearest desired week start day
cal_skip_days() {
    local current_day=$1      # Current weekday (0-6)
    local desired_start=$2    # Desired week start day (0-6)

    # Calculate the number of days to skip
    local skip_days=$(( (desired_start - current_day + 7) % 7 ))

    echo $skip_days
}


__koimain() {
  __addarg "-h" "--help" "help" "optional" "" "$koidescription" ""
  __addarg "-w" "--weekstart" "storevalue" "optional" "1" "Starting day of the week, Sun=0, Mon=1, ..." "__verify_weekstart"
  __addarg "" "inputs" "positionalarray" "required" "" "Time merged WRF output file(s)" ""
  __parseargs "$@"

  set -u

  incHour=""
  cdoInput=""
  for input in "${inputs[@]}"; do

    # get the start datetime
    date1=$(cdo --silent -showtimestamp -seltimestep,1 $input | tr -d " ")
    date2=$(cdo --silent -showtimestamp -seltimestep,-1 $input | tr -d " ")

    # get the day of the week, Sunday=0
    weekday=$(isodatetime ${date1:0:10} --print-format %w)
    offset=P$(cal_skip_days $weekday $weekstart)D
    start_date=$(isodatetime ${date1:0:10} --offset $offset)
    end_date=${date2:0:10}

    filename=$(basename "$input")

    # calculate weekly mean
    set -x 
    cdo -timselmean,7 -daymean -seldate,${start_date:0:10},${end_date} ${input} ${filename}
    set +x
  done
}

__koirun "$@"
