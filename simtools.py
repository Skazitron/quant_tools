import numpy as np
from numba import jit
from datetime import date, timedelta
import holidays



print(nyse_holidays)

@jit
def calculate_rsi(prev: np.float32, cur: np.float32, start_date: date, end_date: date):
    """
    Docstring for calculate_rsi
    
    :param prev: the price of the security in the backwards date
    :type prev: np.float32
    :param cur: the present price
    :type cur: np.float32
    :param duration: the duration days
    :type duration: np.float32
    """
    end_date = end_date + timedelta(days=1)
    delta = (end_date - start_date).days

    # adjust for market holidays and weekends

    return (cur-prev)/delta

# @jit(nopython=True)
# def simulate_price_by_duration(sec_price: np.float32, volume: np.int64, \
#                           open: np.float32, close: np.float32, high: np.float32, \
#                           low: np.float32, duration: np.int64,) -> np.ndarray:
#     # browninan bridge in numpy

#     return np.array([])
