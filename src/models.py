from dataclasses import dataclass
from enum import Enum


class Variable(Enum):
    PPT = "ppt"
    TMAX = "tmax"
    TMIN = "tmin"


class TimeStep(Enum):
    DAILY = "daily"
    MONTHLY = "monthly"
