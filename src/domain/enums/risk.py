from enum import StrEnum


class RiskMode(StrEnum):
    NORMAL = "NORMAL"
    REDUCED = "REDUCED"
    FROZEN = "FROZEN"
    DEFENSIVE_UNWIND = "DEFENSIVE_UNWIND"
    FLATTEN = "FLATTEN"
    SHUTDOWN = "SHUTDOWN"
