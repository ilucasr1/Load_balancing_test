import numpy as np
import json
import time
import os
import gc
import sys
import dask
import dask.array as da
from distributed import Client, Queue, Variable, performance_report, fire_and_forget#, Future, get_client, secede, rejoin, as_completed
from distributed.diagnostics import MemorySampler
import matplotlib.pyplot as plt

nb_workers = str(sys.argv[1])
scheduler_file_name = str(sys.argv[2])

try:
    client = Client(scheduler_file='scheduler.json')
except Exception as _:
    print("retrying ...", flush=True)
    client = Client(scheduler_file='scheduler.json')
    
workers = list(client.scheduler_info(n_workers=-1)["workers"].keys())
while (len(workers) != int(nb_workers)):
    time.sleep(1)
    workers = list(client.scheduler_info(n_workers=-1)["workers"].keys())
print('Analytics client connected')
print(workers, flush=True)


def func(mat, it):
#    for i in range((it+1)*3):
    for i in range(3):
        #r = mat@mat.T
        #mat = mat + (r % 1)
        mat = mat+mat
        mat = mat/2
    return mat

try:
    shared_data = Variable("shared").get(timeout=10)
except Exception as e:
    shared_data = Variable("shared").get(timeout=10)

n, nt, t = shared_data

print("Dimension of array : ", str(n))
print("Number of timesteps : ", str(nt))
print("Length simulated timestep : ",  str(t))

ms_1 = MemorySampler()    
ms_2 = MemorySampler()    
ms_3 = MemorySampler()    
ms_4 = MemorySampler()    

Futures = []
cancel_list = []
#with performance_report(filename="dask-report.html"), dask.config.set(array_optimize=None), ms.sample("collection 1"):
with performance_report(filename="dask-report.html"),\
        ms_1.sample("Worker memory"),\
        ms_2.sample("Managed", measure="managed"),\
        ms_3.sample("Unmanaged", measure="unmanaged"),\
        ms_4.sample("Spilled", measure="spilled"):

    for i in range(nt-1):
        while Queue("rank-0").qsize() == 0:
            time.sleep(0.01)
        data_f = Queue("rank-0").get(timeout=5)
        
        fire_and_forget(client.submit(func, data_f, i))
        client.cancel(data_f)
        print("Task ", str(i), " submitted", flush=True)
    
    while Queue("rank-0").qsize() == 0:
            time.sleep(0.01)
    data_f = Queue("rank-0").get(timeout=5)
    Future = client.submit(func, data_f, nt-1)
    print("Task ", str(nt-1), " submitted", flush=True)
    value = client.gather(Future)
    del Future
    del data_f

    time.sleep(10)

nworkers = len(list(client.scheduler_info(n_workers=-1)["workers"].keys()))


fig, axes = plt.subplots(nrows=4, ncols=nworkers, figsize=(15,10))
plt.subplots_adjust(hspace=0.5)

ms_1.plot(ax=axes[0], align=True)
ms_2.plot(ax=axes[1], align=True)
ms_3.plot(ax=axes[2], align=True)
ms_4.plot(ax=axes[3], align=True)
#if nworkers == 1:
#    ms_1.plot(ax=axes[0], align=True)
#    ms_2.plot(ax=axes[1], align=True)
#else:
#    for i in range(nworkers):
#        ms_1.plot(ax=axes[i,0], align=True)
#        ms_2.plot(ax=axes[i,1], align=True)

#res = ms.plot(align=True)
#if isinstance(res, plt.Axes):
#    res = res.get_figure()

plotfile = "plots/plot_n" + str(n) + "_nt" + str(nt) +  "_t" + str(t) + ".png"
fig.savefig(plotfile)

print("\nFinished\n", flush=True)

client.close()

