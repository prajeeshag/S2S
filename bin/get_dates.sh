#!/bin/bash

set -ex
input_file=$MEM_SHARE_DIR/ungrib_input.grib
sdate=$(cdo -showtimestamp -seltimestep,1 $input_file | xargs)
edate=$(cylc cycle-point --offset=${FCSTDURATION} ${sdate})
export syyyy=${sdate:0:4}
export smm=${sdate:5:2}
export sdd=${sdate:8:2}
export shh=${sdate:11:2}
export eyyyy=${edate:0:4}
export emm=${edate:5:2}
export edd=${edate:8:2}
export ehh=${edate:11:2}
set +x
