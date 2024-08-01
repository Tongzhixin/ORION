import ray
import time

ray.init()
@ray.remote(num_cpus=1)
class ThreadedActor():
    def task_1(self,index): 
        time.sleep(1)
        for _ in range(10):
            self.task_2()
        print(f"I'm running in {index} thread!")
        return index
    def task_2(self): 
        time.sleep(1)
        print("I'm running in another thread!")
@ray.remote(num_cpus=1)
class th2():
    def task_3(self,index):
        time.sleep(1)
        print(f"I'm running in class 2 {index} thread!")
        

    


time_start = time.time()
a = ThreadedActor.options(max_concurrency=10).remote()
b = th2.options(max_concurrency=20).remote()
refs = []
for i in range(10):
    ref = a.task_1.remote(i)
    # ref2 = b.task_3.remote(ref)
    refs.append(ref)
# ray.get([a.task_1.remote(), a.task_2.remote()])
ray.get(refs)
print(f"time use{time.time()-time_start}")