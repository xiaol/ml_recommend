# -*- coding: utf-8 -*-


import numpy as np
from memory_profiler import profile

@profile
def test_numpy_array():
    a = [1]*10000000
    ar = np.array(a, copy=False)


test_numpy_array()
print '--------hold on---------------'
