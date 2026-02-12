import numpy as np
from numba import guvectorize, jit
import polars as pl


@jit(nopython=True)
def fibo_array(n: np.int32) -> np.ndarray:
    arr = np.ones(n+1)
    if len(arr) <= 0:
        raise ValueError("Number has to be greater than 0")
    arr[0] = 1
    arr[1] = 1
    for i in range(1, n+1):
        arr[i] = arr[i-1] + arr[i-2]

    return arr


# testing
print(fibo_array(np.int32(13)))