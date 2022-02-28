from decimal import *
from datetime import datetime


class Level:
    def __init__(self, time: datetime = 0, level: Decimal = 1):
        self.time = time
        self.level = level

    def __repr__(self):
        return f"{self.level}, {self.time}"
