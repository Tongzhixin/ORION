import ray
import numpy as np
r1 = ray.put(np.ones(10000000))
r2 = ray.get(r1)
cycle = {"x": r2}
cycle["y"] = cycle
# ray.internal.free([r1])
del r1
del r2
del cycle

# import gc
# gc.collect()  # This line is needed to avoid the leak.

import time
time.sleep(999)