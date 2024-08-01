import ray,time
import pyarrow as pa
from ray.util.state import get_actor, get_objects

ray.init()

@ray.remote
def output():
    print("start output")
    value=ray.get(setting.remote())
    value = value+1
    return value
@ray.remote
def setting():
    c=10
    print("start")
    time.sleep(2)
    return c
time1=time.time()
ref = output.remote()
print("main start")
time.sleep(2)
print(ray.get(ref))
print(f"time use{time.time()-time1}")