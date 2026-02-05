import numpy as np
from datetime import date, timedelta
from enum import Enum
from scipy.special import comb


class Granularity(Enum):
    ONE_MINUTE = 1
    FIVE_MINUTES = 5
    FIFTEEN_MINUTES = 15
    THIRTY_MINUTES = 30
    HOURLY = 60


def simple_return(cur_price: np.float32, old_price: np.float32) -> np.float32:

    return (cur_price-old_price)/old_price

def brownian_bridge(number_of_items: np.int32, kurtosis: np.float32, bias: np.float32) -> np.ndarray:
    ## implement
    return np.array([])


def create_fake_stock_data(ticker: str, duration: timedelta, start: date, end: date, granularity: Granularity) -> np.ndarray:
    return np.array([])