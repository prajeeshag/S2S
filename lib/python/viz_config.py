import logging
from enum import Enum
from pathlib import Path
from typing import Annotated

import yaml
from pydantic import BaseModel, Field

# setup logger to use stdout
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class StatsOpr(str, Enum):
    min = "min"
    median = "median"
    max = "max"
    sum = "sum"
    cumsum = "sum"
    mean = "mean"
    pctl = "pctl"
    prob_ltc = "prob_ltc"
    prob_gtc = "prob_gtc"


class TimeCoarsen(str, Enum):
    daymean = "daymean"
    daysum = "daysum"
    daymin = "daymin"
    daymax = "daymax"
    weekmean = "weekmean"
    weeksum = "weeksum"
    weekmean_daymax = "weekmean_daymax"
    weekmean_daymin = "weekmean_daymin"


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


class Stats(BaseModel):
    opr: tuple[StatsOpr, float] | StatsOpr


class FileType(Enum):
    zarr = "zarr"
    nc = "nc"
    zarr_and_nc = "zarr_and_nc"


class TimeStat(BaseModel):
    time_coarsen: TimeCoarsen | None = None
    ens_stats: dict[str, set[tuple[StatsOpr, float | None]]]
    file_type: dict[str, FileType]


class Field(BaseModel):
    preprocess: PreProccess | None = None
    stat: list[TimeStat] = []


class Config(BaseModel):
    fields: dict[str, Field] = {}
    week_start: int = Field(default=0, ge=0, le=6)


def get_config():
    current_dir = Path(__file__).parent
    yaml_file_path = current_dir / "viz.yaml"
    with open(yaml_file_path, "r") as file:
        data = yaml.safe_load(file)

    return Config.model_validate(data)


if __name__ == "__main__":
    print(get_config())
