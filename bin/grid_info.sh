#!/bin/bash
# shellcheck disable=SC2154,SC2086,SC1091,SC2034

# Exports the grid information from geo_em file, currently exports 'dx, dy, e_we, e_sn'"

geoem=$1
set -x
export dx=$(ncdump -h $geoem | grep "DX = " | awk '{print $3}' | sed 's/f//g')
export dy=$(ncdump -h $geoem | grep "DY = " | awk '{print $3}' | sed 's/f//g')
export e_sn=$(ncdump -h $geoem | grep "j_parent_end = " | awk '{print $3}' | sed 's/f//g')
export e_we=$(ncdump -h $geoem | grep "i_parent_end = " | awk '{print $3}' | sed 's/f//g')
set +x
