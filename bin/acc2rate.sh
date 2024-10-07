#!/bin/bash
# shellcheck disable=SC2154,SC2086,SC1091,SC2034
set -e
set -o pipefail

source "$(dirname "$0")/koi"
koiname="$0"
koidescription="Accumulated fields to rate"

__koimain() {
  __addarg "-h" "--help" "help" "optional" "" "$koidescription" ""
  __addarg "" "input" "positionalvalue" "required" "" "WRF output containing accumulated fields" ""
  __addarg "" "output" "positionalvalue" "required" "" "Output file" ""
  __parseargs "$@"

  set -ux

  date1=$(cdo -showtimestamp -seltimestep,1 $input | tr -d " ")
  date2=$(cdo -showtimestamp -seltimestep,2 $input | tr -d " ")

  interval_seconds=$(isodatetime --parse-format "%Y-%m-%dT%H:%M:%S" $date1 $date2 --as-total S)

  cdo -divc,$interval_seconds -sub -seltimestep,2/-1 $input -seltimestep,1/-2 $input $output
}

__koirun "$@"
