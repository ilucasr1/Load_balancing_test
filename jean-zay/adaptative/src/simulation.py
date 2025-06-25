import numpy as np
import json
import time
import os
import sys
import dask
import dask.array as da
from distributed import Client, Queue, Variable#, Future, get_client, secede, rejoin, as_completed
from dask_jobqueue import SLURMCluster
from dask_management import client_start

scheduler_addr = str(sys.argv[1])
nt = int(sys.argv[2])
t = int(sys.argv[3])

#workers = list(client.scheduler_info()["workers"].keys())
#while (len(workers) == 0):
#    workers = list(client.scheduler_info()["workers"].keys())
#    time.sleep(1)

client = dm.client_start(10)
print('Simulation client started')
print(workers, flush=True)

shared_data = ["adaptative", nt, t]

Variable("shared").set(shared_data)
q = Queue("rank-0")

sizes = [1000, 2000, 3000, 4000, 5000, 7000, 8000, 9000, 10000, 5000, 2000]
#sizes = [9000, 10000, 12000, 14000, 16000, 18000, 16000, 14000, 12000, 12000]
#sizes = [20000]
cpt = 0
for i in range(nt):
    if i > 0 and i % 3 == 0 and i < nt-1:
        cpt = cpt + 1
    print("\nMatrix for task "  + str(i) + " : size : " \
                                + str(sizes[cpt]) + "x" \
                                + str(sizes[cpt]), flush=True)
#    ts = time.time()
    matrix = np.random.rand(sizes[cpt],sizes[cpt])
#    te = time.time()
#    print(f"time matrix rand: {te-ts}")

#    ts = time.time()
    f = client.scatter(matrix, direct=True)
#    te = time.time()
#    print(f"time scatter: {te-ts}")

    q.put(f)
    time.sleep(t)

client.close()
