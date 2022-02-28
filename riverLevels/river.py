import dataclasses


@dataclasses.dataclass
class River:
    name: str
    low_water: float
    high_water: float
    description: str = ""
