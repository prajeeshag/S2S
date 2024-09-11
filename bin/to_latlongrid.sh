#!/bin/bash
# shellcheck disable=SC2154,SC2086,SC1091,SC2034
set -e
set -o pipefail

source "$(dirname "$0")/koi"
koiname="$0"
koidescription="Remap WRF outputs to lat/lon grid"

create_grid_file() {
  resolution=$2

  if [ -f griddes.txt ]; then
    return 0
  fi

  xlat=$(ncdump -v XLAT $1 | sed -n '/XLAT =/, $p' | grep -o '[0-9.-]\+' | sort -n)
  xlon=$(ncdump -v XLONG $1 | sed -n '/XLONG =/, $p' | grep -o '[0-9.-]\+' | sort -n)

  slat=$(echo $xlat | cut -d ' ' -f 1)
  elat=$(echo $xlat | rev | cut -d ' ' -f 1 | rev)
  latsize=$(echo "scale=10; (($elat - $slat + $resolution)/$resolution)" | bc)
  # Get the integer part
  latsize=$(printf "%.0f" $latsize)

  slon=$(echo $xlon | cut -d ' ' -f 1)
  elon=$(echo $xlon | rev | cut -d ' ' -f 1 | rev)
  lonsize=$(echo "scale=10; (($elon - $slon + $resolution)/$resolution)" | bc)
  lonsize=$(printf "%.0f" $lonsize)

  cat <<EOF >griddes.txt
        gridtype='lonlat'
        xsize=$lonsize
        ysize=$latsize
        xfirst=$slon
        yfirst=$slat
        xinc=$resolution
        yinc=$resolution
EOF
}

__koimain() {
  __addarg "-h" "--help" "help" "optional" "" "$koidescription" ""
  __addarg "-r" "--resolution" "storevalue" "required" "" "Resolution of the output grid in degrees" ""
  __addarg "-o" "--output_dir" "storevalue" "optional" "./" "Ouput directory" ""
  __addarg "" "--remap_method" "storevalue" "optional" "remapnn" "CDO remap methods, e.g. remapnn" ""
  __addarg "" "inputs" "positionalarray" "required" "" "WRF output file(s), should contain XLAT and XLONG" ""
  __parseargs "$@"

  set -u

  for input in "${inputs[@]}"; do

    create_grid_file "$input" "${resolution}"

    filename=$(basename "$input")

    # calculate weekly mean
    set -x
    cdo -${remap_method},griddes.txt ${input} $output_dir/${filename}
    set +x
  done
}

__koirun "$@"
