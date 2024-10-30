import logging
from enum import Enum
from pathlib import Path
from typing import Annotated

import yaml
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

# setup logger to use stdout
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class VizConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EnsStatOpr(str, Enum):
    min = "min"
    median = "median"
    max = "max"
    sum = "sum"
    mean = "mean"
    pctl = "pctl"
    prob_ltc = "prob_ltc"
    prob_gtc = "prob_gtc"
    efi = "efi"
    sotp = "sotp"
    sotn = "sotn"
    members = "members"
    rfmembers = "rfmembers"


class EnsStat(VizConfig):
    opr: EnsStatOpr
    values: list[float] = []

    @property
    def is_cdo_opr(self):
        return self.opr not in [
            EnsStatOpr.efi,
            EnsStatOpr.sotn,
            EnsStatOpr.sotp,
            EnsStatOpr.members,
            EnsStatOpr.rfmembers,
        ]

    def get_cdo_opr(self, files: list[str]):
        if not self.is_cdo_opr:
            return None
        files_str = " ".join(files)
        if self.opr not in [EnsStatOpr.pctl, EnsStatOpr.prob_ltc, EnsStatOpr.prob_gtc]:
            yield f"-ens{self.opr.value} [ {files_str} ]"
            return

        for value in self.values:
            if self.opr == EnsStatOpr.pctl:
                yield f"-enspctl,{value} [ {files_str} ]"
            elif self.opr == EnsStatOpr.prob_gtc:
                yield f" -ensmean [ -gtc,{value} : {files_str} ] "
            elif self.opr == EnsStatOpr.prob_ltc:
                yield f" -ensmean [ -ltc,{value} : {files_str} ] "

    def get_ens_stat_coord_values(self):
        return self.values

    @model_validator(mode="after")
    def check_opr_args(self):
        if self.opr not in [EnsStatOpr.pctl, EnsStatOpr.prob_ltc, EnsStatOpr.prob_gtc]:
            self.values = []
        self.values = list(set(self.values))
        return self


class RemapMethod(str, Enum):
    nearestneighbor = "nn"
    bilinear = "bil"
    conservative = "con"


class TimeAggregation(str, Enum):
    daymean = "daymean"
    daysum = "daysum"
    daycumsum = "daycumsum"
    daymin = "daymin"
    daymax = "daymax"
    weekmean = "weekmean"
    weeksum = "weeksum"
    weekmean_daymax = "weekmean_daymax"
    weekmean_daymin = "weekmean_daymin"

    def get_cdo_opr(self):
        if self == TimeAggregation.daymean:
            return "-daymean"
        elif self == TimeAggregation.daysum:
            return "-daysum"
        elif self == TimeAggregation.daycumsum:
            return "-timcumsum -daysum"
        elif self == TimeAggregation.daymin:
            return "-daymin"
        elif self == TimeAggregation.daymax:
            return "-daymax"
        elif self == TimeAggregation.weekmean:
            return "-timselmean,7 -daymean"
        elif self == TimeAggregation.weeksum:
            return "-timselsum,7 -daysum"
        elif self == TimeAggregation.weekcumsum:
            return "-timcumsum -timselsum,7 -daysum"
        elif self == TimeAggregation.weekmean_daymax:
            return "-timselmean,7 -daymax"
        elif self == TimeAggregation.weekmean_daymin:
            return "-timselmean,7 -daymin"
        else:
            raise ValueError(f"Invalid time coarsen: {self}")


class WeekDay(Enum):
    monday = 0
    tuesday = 1
    wednesday = 2
    thursday = 3
    friday = 4
    saturday = 5
    sunday = 6

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            for member in cls:
                if member.name.lower() == value.lower():
                    return member
        return super()._missing_(value)


class Rename(VizConfig):
    from_: Annotated[str, Field(..., alias="from")]


class UnitConversion(VizConfig):

    mulc: float = 1.0
    addc: float = 0.0
    to_units: str = ""


class Remap(VizConfig):
    method: str
    res: float


class PreProccess(VizConfig):
    remap: Remap | None = None
    unit_conversion: UnitConversion | None = None
    rename: Rename | None = None
    week_start: WeekDay = WeekDay.monday


class FileType(Enum):
    zarr = "zarr"
    nc = "nc"
    zarr_and_nc = "zarr_and_nc"


class TimeStat(VizConfig):
    time_aggregation: TimeAggregation | None = None
    ens_stats: dict[str, EnsStat]
    file_type: dict[str, FileType]

    @model_validator(mode="after")
    def check_ens_stats_keys_in_file_type(self):
        ens_stats_keys = set(self.ens_stats.keys())
        file_type_keys = set(self.file_type.keys())

        if not ens_stats_keys.issubset(file_type_keys):
            missing_keys = ens_stats_keys - file_type_keys
            raise ValueError(
                f"The following keys in 'ens_stats' are missing in 'file_type': {missing_keys}"
            )
        return self

    def is_zarr(self, stat_name: str):
        return self.file_type[stat_name] in [FileType.zarr, FileType.zarr_and_nc]

    def is_nc(self, stat_name: str):
        return self.file_type[stat_name] in [FileType.nc, FileType.zarr_and_nc]

    @property
    def reforecast_needed(self):
        reforecast_stats = [
            EnsStatOpr.rfmembers,
            EnsStatOpr.efi,
            EnsStatOpr.sotn,
            EnsStatOpr.sotp,
        ]
        for x in self.ens_stats.values():
            if x.opr in reforecast_stats:
                return True
        return False


class Field(VizConfig):
    preprocess: PreProccess | None = None
    stat: list[TimeStat] = []


class Config(BaseModel):
    fields: dict[str, Field] = {}


def get_config():
    current_dir = Path(__file__).parent
    yaml_file_path = current_dir / "viz.yaml"
    with open(yaml_file_path, "r") as file:
        data = yaml.safe_load(file)

    return Config.model_validate(data)


if __name__ == "__main__":
    print(get_config())
