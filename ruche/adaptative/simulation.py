import numpy as np
import json
import time
import os
import sys
import dask
import dask.array as da
from distributed import Client, Queue, Variable#, Future, get_client, secede, rejoin, as_completed
from dask_jobqueue import SLURMCluster

print("path simu:", os.getcwd() ,flush=True)

print("start simu file", flush=True)
nb_workers = int(sys.argv[1])
scheduler_addr = str(sys.argv[2])
#n = int(sys.argv[3])
nt = int(sys.argv[3])
t = int(sys.argv[4])

print("start simu client connect", flush=True)
try:
    client = Client(scheduler_addr)
except Exception as _:
    print("retrying ...", flush=True)
    client = Client(scheduler_addr)

print("simu client connected", flush=True)

print("\n dashboard link :" + str(client.dashboard_link),flush=True)
#while not "workers" in client.scheduler_info(n_workers=-1).keys():
#	print("Analytics : no workers yet", flush=True)

workers = list(client.scheduler_info()["workers"].keys())
while (len(workers) != nb_workers):
    workers = list(client.scheduler_info()["workers"].keys())
    time.sleep(1)
print('Simulation client connected')
print(workers, flush=True)


shared_data = ["adaptative", nt, t]

Variable("shared").set(shared_data)
q = Queue("rank-0")
time.sleep(3)

#sizes = [1000, 2000, 3000, 4000, 5000, 7000, 8000, 9000, 10000, 5000, 2000]
#sizes = [9000, 10000, 12000, 14000, 16000, 18000, 16000, 14000, 12000, 12000]
sizes = [20000]
cpt = 0
for i in range(nt):
#    if i > 0 and i % 4 == 0:
#        cpt = cpt + 1
#    ts = time.time()
    print("\nMatrix for task " + str(i) + " : size : " + str(sizes[cpt]) + "x" + str(sizes[cpt]), flush=True)
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
