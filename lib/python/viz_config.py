import logging
from enum import Enum
from pathlib import Path
from typing import Annotated

import yaml
from pydantic import AfterValidator, BaseModel, Field

# setup logger to use stdout
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class EnsStatOpr(str, Enum):
    min = "min"
    median = "median"
    max = "max"
    sum = "sum"
    mean = "mean"
    pctl = "pctl"
    prob_ltc = "prob_ltc"
    prob_gtc = "prob_gtc"
    sotn = "sotn"
    sotp = "sotp"
    efi = "efi"
    members = "members"
    rfmembers = "rfmembers"


class EnsStat(BaseModel):
    opr: EnsStatOpr
    arg: float = 0

    def get_cdo_opr(self, files: list[str]):
        files_str = " ".join(files)
        if self.opr == EnsStatOpr.pctl:
            return f"-enspctl,{self.arg} [ {files_str} ]"
        elif self.opr == EnsStatOpr.prob_gtc:
            return f" -ensmean [ -gtc,{self.arg} : {files_str} ] "
        elif self.opr == EnsStatOpr.prob_ltc:
            return f" -ensmean [ -ltc,{self.arg} : {files_str} ] "
        elif self.opr == [
            EnsStatOpr.min,
            EnsStatOpr.max,
            EnsStatOpr.mean,
            EnsStatOpr.sum,
            EnsStatOpr.median,
        ]:
            return f"-ens{self.opr.value} {files_str}"
        else:
            return None


class RemapMethod(str, Enum):
    nearestneighbor = "nn"
    bilinear = "bil"
    conservative = "con"


class TimeCoarsen(str, Enum):
    daymean = "daymean"
    daysum = "daysum"
    daymin = "daymin"
    daymax = "daymax"
    weekmean = "weekmean"
    weeksum = "weeksum"
    weekmean_daymax = "weekmean_daymax"
    weekmean_daymin = "weekmean_daymin"

    def get_cdo_opr(self):
        if self == TimeCoarsen.daymean:
            return "-daymean"
        elif self == TimeCoarsen.daysum:
            return "-daysum"
        elif self == TimeCoarsen.daymin:
            return "-daymin"
        elif self == TimeCoarsen.daymax:
            return "-daymax"
        elif self == TimeCoarsen.weekmean:
            return "-timselmean,7 -daymean"
        elif self == TimeCoarsen.weeksum:
            return "-timselsum,7 -daysum"
        elif self == TimeCoarsen.weekmean_daymax:
            return "-timselmean,7 -daymax"
        elif self == TimeCoarsen.weekmean_daymin:
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


class Rename(BaseModel):
    from_: Annotated[str, Field(..., alias="from")]


class UnitConversion(BaseModel):
    mulc: float = 1.0
    addc: float = 0.0
    to_units: str = ""


class Remap(BaseModel):
    method: str
    res: float


class PreProccess(BaseModel):
    remap: Remap | None = None
    unit_conversion: UnitConversion | None = None
    rename: Rename | None = None
    week_start: WeekDay = WeekDay.monday


class FileType(Enum):
    zarr = "zarr"
    nc = "nc"
    zarr_and_nc = "zarr_and_nc"


def ens_stats_validator(v: dict[str, list[EnsStat]]):
    v_validated = {}
    for name, ens_stat_list in v.items():
        v_validated[name] = ens_stat_list_validator(ens_stat_list)
    return v_validated


def ens_stat_list_validator(v: list[EnsStat]):

    # Validate unique list of EnsStat
    stats_list = []
    for stat in v:
        arg = 0
        if stat.opr in [EnsStatOpr.pctl, EnsStatOpr.prob_ltc, EnsStatOpr.prob_gtc]:
            arg = stat.arg
        stats_list.append((stat.opr, arg))

    stats_set = set(stats_list)
    if len(stats_set) != len(v):
        raise ValueError("Invalid ens_stat: duplicate entries not allowed")

    oprs_set = list(set([stat.opr for stat in v]))

    if len(oprs_set) == 1:
        return v

    for opr in [
        EnsStatOpr.pctl,
        EnsStatOpr.prob_ltc,
        EnsStatOpr.prob_gtc,
        EnsStatOpr.members,
        EnsStatOpr.rfmembers,
    ]:
        if opr in oprs_set:
            raise ValueError(
                f"Invalid ens_stat: cannot combine {opr} with other operators"
            )

    for opr in oprs_set:
        if opr in [EnsStatOpr.members, EnsStatOpr.rfmembers] and len(v) > 1:
            raise ValueError(
                "Invalid ens_stat: for operators 'members' and 'rfmembers' only one operator is allowed"
            )

    allowed_groups = [
        [
            EnsStatOpr.min,
            EnsStatOpr.max,
            EnsStatOpr.mean,
            EnsStatOpr.sum,
            EnsStatOpr.median,
        ],
        [
            EnsStatOpr.efi,
            EnsStatOpr.sotn,
            EnsStatOpr.sotp,
        ],
    ]

    for group in allowed_groups:
        if any([opr in group for opr in oprs_set]):
            if all([opr in group for opr in oprs_set]):
                return v
            else:
                raise ValueError(
                    f"Invalid ens_stat: allowed groups are: {allowed_groups}"
                )
    raise ValueError(f"Invalid ens_stat: {oprs_set}")


class TimeStat(BaseModel):
    time_coarsen: TimeCoarsen | None = None
    ens_stats: Annotated[
        dict[str, list[EnsStat]],
        AfterValidator(ens_stats_validator),
    ]
    file_type: dict[str, FileType]

    @property
    def reforecast_needed(self):
        return any(
            [
                stat.opr
                in [
                    EnsStatOpr.rfmembers,
                    EnsStatOpr.efi,
                    EnsStatOpr.sotn,
                    EnsStatOpr.sotp,
                ]
                for stat in self.ens_stats.values()
            ]
        )

    def get_ens_stat_coord_values(self, stat_name: str):
        ens_stat = self.ens_stats[stat_name]
        if ens_stat[0].opr in [
            EnsStatOpr.pctl,
            EnsStatOpr.prob_ltc,
            EnsStatOpr.prob_gtc,
        ]:
            return [stat.arg for stat in ens_stat]
        return [stat.opr.value for stat in ens_stat]


class Field(BaseModel):
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
