import numpy as np
import json
import time
import os
import sys
import dask
import dask.array as da
from distributed import Client, Queue, Variable#, Future, get_client, secede, rejoin, as_completed

nb_workers = int(sys.argv[1])
scheduler_file_name = str(sys.argv[2])
n = int(sys.argv[3])
nt = int(sys.argv[4])
t = int(sys.argv[5])

try:
    client = Client(scheduler_file='scheduler.json')
except Exception as _:
    print("retrying ...", flush=True)
    client = Client(scheduler_file='scheduler.json')
    
workers = list(client.scheduler_info()["workers"].keys())
while (len(workers) != nb_workers):
    workers = list(client.scheduler_info()["workers"].keys())
    time.sleep(1)
print('Simulation client connected')
print(workers, flush=True)


shared_data = [n, nt, t]

Variable("shared").set(shared_data)
q = Queue("rank-0")
time.sleep(3)

for i in range(nt):

#    ts = time.time()
    print("\nMatrix for task " + str(i) + " : size : " + str(n) + "x" + str(n))
    matrix = np.random.rand(n,n)
#    te = time.time()
#    print(f"time matrix rand: {te-ts}")

#    ts = time.time()
    f = client.scatter(matrix, direct=True)
#    te = time.time()
#    print(f"time scatter: {te-ts}")

    q.put(f)
    time.sleep(t)

client.close()
