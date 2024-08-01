import ray
from ray.util.queue import Queue
import time

# 初始化Ray
ray.init()

@ray.remote
class Producer:
    def __init__(self, queue):
        self.queue = queue

    def produce(self, num_items):
        for i in range(num_items):
            time.sleep(0.01)  # 模拟生产数据的时间
            item = f"item_{i}"
            print(f"Producing: {item}")
            self.queue.put(item)
        # 在生产结束后向队列中放入None作为结束标志
        self.queue.put(None)

@ray.remote
class Consumer:
    def __init__(self, queue):
        self.queue = queue

    def consume(self):
        while True:
            item = self.queue.get()
            if item is None:  # 使用None作为结束标志
                break
            print(f"Consuming: {item}")
            time.sleep(0.05)  # 模拟消费数据的时间

# 创建一个队列
queue = Queue()

# 创建生产者和消费者actor
producer = Producer.remote(queue)
consumer = Consumer.remote(queue)

# 启动生产者和消费者
producer_task = producer.produce.remote(100)
consumer_task = consumer.consume.remote()

# 等待生产者和消费者完成
ray.get(producer_task)
ray.get(consumer_task)


