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
from dask_jobqueue import SLURMCluster
from dask_management import *

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

if __name__ == "__main__":
	
    scheduler_addr= str(sys.argv[1])
    client = client_start(10)


#    while not "workers" in client.scheduler_info(n_workers=-1).keys():
#        print("Analytics : no workers yet", flush=True)

    #workers = list(client.scheduler_info(n_workers=-1)["workers"].keys())
    #while (len(workers) != int(nb_workers)):
    #    time.sleep(1)
    #    workers = list(client.scheduler_info(n_workers=-1)["workers"].keys())
    print('\nAnalytics client connected')
    print(workers, flush=True)

    memory_stats = client.run_on_scheduler(memory_info)
    print("\nmemory_stats : \n" + str(memory_stats))

    try:
        shared_data = Variable("shared").get(timeout=10)
    except Exception as e:
        shared_data = Variable("shared").get(timeout=10)

    n, nt, t = shared_data

    print("\nDimension of array : ", str(n))
    print("Number of timesteps : ", str(nt))
    print("Length simulated timestep : ",  str(t))

    ms_1 = MemorySampler()    
    ms_2 = MemorySampler()    
    ms_3 = MemorySampler()    
    ms_4 = MemorySampler()    

    print(client.scheduler_info(n_workers=-1)["workers"])

    memory_limit = client.scheduler_info(nworkers=-1)["workers"][workers[0]]["memory_limit"]
    print("\nmemory limit : ", memory_limit)

    #with performance_report(filename="dask-report.html"), dask.config.set(array_optimize=None), ms.sample("collection 1"):
    with performance_report(filename="dask-report.html"),\
            ms_1.sample("Worker memory"),\
            ms_2.sample("Managed", measure="managed"),\
            ms_3.sample("Unmanaged", measure="unmanaged"),\
            ms_4.sample("Spilled", measure="spilled"):
        
        client.run_on_scheduler(scheduler_monitor)
#        client.run_on_scheduler(scheduler_adaptor)

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


 #   client.run_on_scheduler(stop_adaptor)

    monitor_data = client.run_on_scheduler(get_monitor_results)
    times, event_loop_intervals, memory_lists = monitor_data["times"], \
                                                monitor_data["event_loop_intervals"], \
                                                monitor_data["memory_lists"]
    client.run_on_scheduler(stop_monitor)

    nworkers = len(list(client.scheduler_info(n_workers=-1)["workers"].keys()))

    dir_name = str(nt) + "_" + str(t)

    try:
        os.mkdir("../plots/" + dir_name)
        print(f"Directory '{dir_name}' created successfully.")
    except FileExistsError:
        print(f"Directory '{dir_name}' already exists.")

    os.system("rm -rf ../plots/" + dir_name + "/*")


    filepath = "../plots/" + dir_name

    fig, axes = plt.subplots(nrows=4, ncols=1, figsize=(15,10))
    plt.subplots_adjust(hspace=0.5)

    ms_1.plot(ax=axes[0], align=True)
    ms_2.plot(ax=axes[1], align=True)
    ms_3.plot(ax=axes[2], align=True)
    ms_4.plot(ax=axes[3], align=True)
        
    if n == "adaptative":
        plotfile = filepath + "/plot_adaptative.png"
    else:
        plotfile = filepath + "/plot.png"
    fig.savefig(plotfile)
    plt.clf()


    #times = [datetime.fromtimestamp(ts).strftime('%H:%M:%S') for ts in times]
    times = [datetime.fromtimestamp(ts) for ts in times]
    short_times = []
    for i in range(nworkers):
        short_times.append([])
        short_times[i] = [times[-len(memory_lists[i][0])], times[-1]]
    memory_limits=   [memory_limit, memory_limit]
    managed_spill_limit = [0.6*memory_limit, 0.6*memory_limit]
    process_spill_limit = [0.7*memory_limit, 0.7*memory_limit]
    new_worker_limit = [0.55*memory_limit, 0.55*memory_limit]

    plt.plot(times, event_loop_intervals, label="event_loop_interval")
    finalize_plot()
    plt.savefig(filepath + "/plot_event_loop_intervals.png")
    plt.clf()


    memory_labels = ["process", "managed", "un_old", "un_recent", "spilled"]

    for i in range(nworkers):
        name_worker = "worker_" + str(i) 
        filepath = "../plots/" + dir_name + "/" + name_worker

        try:
            os.mkdir(filepath)
            print(f"Directory '{name_worker}' created successfully.")
        except FileExistsError:
            print(f"Directory '{name_worker}' already exists.")

        for j in range(len(memory_lists[i])):
            plt.plot(times[-len(memory_lists[i][j]):], memory_lists[i][j], label=memory_labels[j])
        plt.plot(short_times[i], memory_limits, label="max_alloc_mem")
        plt.plot(short_times[i], managed_spill_limit, label="max_managed_b4_spill")
        plt.plot(short_times[i], process_spill_limit, label="max_process_b4_spill")
        plt.plot(short_times[i], new_worker_limit, label="new_worker_limit")
        plt.title("Worker " + str(i))
        
        finalize_plot()
        plt.savefig(filepath + "/plot_memory_global.png")
        plt.clf()


        for j in range(len(memory_lists[i])):
            plt.plot(times[-len(memory_lists[i][j]):], memory_lists[i][j], label=memory_labels[j])
            plt.title("Worker " + str(i))

            finalize_plot()
            plt.savefig(filepath + "/plot_memory_" + memory_labels[j]  + ".png")
            
            plt.plot(short_times[i], memory_limits, label="max_alloc_mem")
            if memory_labels[j] == "managed":
                plt.plot(short_times[i], managed_spill_limit, label="max_managed_b4_spill")
                plt.plot(short_times[i], new_worker_limit, label="new_worker_limit")
                finalize_plot()
                plt.savefig(filepath + "/plot_memory_" + memory_labels[j]  + "_w_lim.png")
            if memory_labels[j] == "process":
                plt.plot(short_times[i], process_spill_limit, label="max_process_b4_spill")
                plt.plot(short_times[i], new_worker_limit, label="new_worker_limit")
                finalize_plot()
                plt.savefig(filepath + "/plot_memory_" + memory_labels[j]  + "_w_lim.png")
            plt.clf()

        
        for j in [0, 1, -1]:
            plt.plot(times[-len(memory_lists[i][j]):], memory_lists[i][j], label=memory_labels[j])
        plt.plot(short_times[i], memory_limits, label="max_alloc_mem")
        plt.plot(short_times[i], managed_spill_limit, label="max_managed_b4_spill")
        plt.plot(short_times[i], process_spill_limit, label="max_process_b4_spill")
        plt.plot(short_times[i], new_worker_limit, label="new_worker_limit")
        plt.title("Worker " + str(i))
        
        finalize_plot()
        plt.savefig(filepath + "/plot_memory_when_spill.png")
        plt.clf()


    print("\nFinished\n", flush=True)

    client.close()

