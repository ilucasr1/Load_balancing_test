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
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

#def list_workerstate_fields(dask_scheduler):
#    ws = next(iter(dask_scheduler.workers.values()))  # get one WorkerState
#    return {
#        'WorkerState_fields': dir(ws),
#        'MemoryManager_fields': dir(ws.memory)
#    }
#
#fields = client.run_on_scheduler(list_workerstate_fields)
#print("\nfields :\n" + str(fields) + "\n")

def memory_info(dask_scheduler):
    info = {}
    for worker, ws in dask_scheduler.workers.items():
        memory = ws.memory
        info[worker] = {
            'managed_in_memory': memory.managed,
            'managed_spilled': memory.spilled,
            'unmanaged_old': memory.unmanaged_old,
            'unmanaged_recent': memory.unmanaged_recent,
            'process': memory.process,  # total memory used by the worker process
        }
    return info

def scheduler_monitor(dask_scheduler):
    import asyncio
    import time

    dask_scheduler._event_loop_intervals = []
    dask_scheduler._memory_samples = [[[], [], [], [], []], [[], [], [], [], []]] 
    dask_scheduler._timestamps = []
    dask_scheduler.should_stop_monitor = asyncio.Event()
    
    async def monitor():
        print("Scheduler monitor started", flush=True)
        
        while not dask_scheduler.should_stop_monitor.is_set():
            try:
                dask_scheduler._timestamps.append(time.time())
                # Collect memory + event loop info for one worker (first)
                for i, ws in enumerate(list(dask_scheduler.workers.values())): 
                    if i == 0:
                        loop_interval = ws.metrics["event_loop_interval"]
                        dask_scheduler._event_loop_intervals.append(loop_interval)
                    
                    mem = ws.memory
                    dask_scheduler._memory_samples[i][0].append(mem.process)
                    dask_scheduler._memory_samples[i][1].append(mem.managed)
                    dask_scheduler._memory_samples[i][2].append(mem.unmanaged_old)
                    dask_scheduler._memory_samples[i][3].append(mem.unmanaged_recent)
                    dask_scheduler._memory_samples[i][4].append(mem.spilled)
            except Exception as e:
                dask_scheduler.log_event("monitor", f"Monitoring error: {e}")
            await asyncio.sleep(0.01)
        print("Scheduler monitor stopped", flush=True)

    dask_scheduler.loop.add_callback(monitor)

def scheduler_adaptor(dask_scheduler):
    import asyncio
    import subprocess
    
    dask_scheduler.should_stop_adaptor = asyncio.Event()
    
    async def adaptor():
        print("Scheduler adaptor started", flush=True)
        while not dask_scheduler.should_stop_adaptor.is_set():
            try:
                worker = next(iter(dask_scheduler.workers))
                ws = dask_scheduler.workers[worker]
                mem_limit = ws.memory_limit
                mem = ws.memory.process                
                ratio = mem/mem_limit
                
                nb_workers = len(dask_scheduler.workers)
                if ratio > 0.55 and nb_workers < 2:
                    print("trying", flush=True)
                    subprocess.Popen([ "dask", "worker", dask_scheduler.address, \
                            "--local-directory", "/tmp", "--nthreads", "1", \
                            "--nworkers", "1"]) 
                    print("\tNew worker launched", flush=True)
                    while len(dask_scheduler.workers) == nb_workers:
                        await asyncio.sleep(1)
                    print("\tWorker ready",flush=True)
            except Exception as e:
                dask_scheduler.log_event("adaptor", f"Adapting error: {e}")
            await asyncio.sleep(0.01)
        print("Scheduler adaptor stopped", flush=True)
    
    dask_scheduler.loop.add_callback(adaptor)

def get_monitor_results(dask_scheduler):
    return {
        "times": dask_scheduler._timestamps,
        "event_loop_intervals": dask_scheduler._event_loop_intervals,
        "memory_lists": dask_scheduler._memory_samples,
    }

def stop_monitor(dask_scheduler):
    if hasattr(dask_scheduler, "should_stop_monitor"):
        dask_scheduler.should_stop_monitor.set()
        return "Monitor stop signal sent"
    return "No monitor running"

def stop_adaptor(dask_scheduler):
    if hasattr(dask_scheduler, "should_stop_adaptor"):
        dask_scheduler.should_stop_adaptor.set()
        return "Adaptor stop signal sent"
    return "No adaptor running"

def get_memory(event_loop_intervals, times, memory_lists):
    event_loop_intervals.append(client.scheduler_info(n_workers=-1)["workers"][workers[0]]["metrics"]["event_loop_interval"])
    times.append(time.time())
        
    memory_stats = client.run_on_scheduler(memory_info)[workers[0]]
        
    memory_lists[0].append(memory_stats["process"])
    memory_lists[1].append(memory_stats["managed_in_memory"])
    memory_lists[2].append(memory_stats["unmanaged_old"])
    memory_lists[3].append(memory_stats["unmanaged_recent"])
    memory_lists[4].append(memory_stats["managed_spilled"])


def func(mat, it):
    for i in range(10):
        mat = mat+mat
        mat = mat/2
    return mat
    
def finalize_plot():
    plt.gcf().autofmt_xdate(rotation=90)
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.SecondLocator(interval=4))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    plt.legend()



if __name__ == "__main__":
	
    nb_workers = int(sys.argv[1])
    scheduler_addr= str(sys.argv[2])
#    print("file name : " + scheduler_file_name + "\n")

    try:
        client = Client(scheduler_addr)
    except Exception as _:
        print("retrying ...", flush=True)
        client = Client(scheduler_addr)

#    while not "workers" in client.scheduler_info(n_workers=-1).keys():
#        print("Analytics : no workers yet", flush=True)

    workers = list(client.scheduler_info(n_workers=-1)["workers"].keys())
    while (len(workers) != int(nb_workers)):
        time.sleep(1)
        workers = list(client.scheduler_info(n_workers=-1)["workers"].keys())
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

    artificial_limit = 0.65
    memory_limit = client.scheduler_info(nworkers=-1)["workers"][workers[0]]["memory_limit"]
    #ram_limit = client.scheduler_info(nworkers=-1)["workers"][workers[0]]["memory_limit"]
    ram_limit = 0
    print("\nmemory limit : ", memory_limit)



    #with performance_report(filename="dask-report.html"), dask.config.set(array_optimize=None), ms.sample("collection 1"):
    with performance_report(filename="dask-report.html"),\
            ms_1.sample("Worker memory"),\
            ms_2.sample("Managed", measure="managed"),\
            ms_3.sample("Unmanaged", measure="unmanaged"),\
            ms_4.sample("Spilled", measure="spilled"):
        
        client.run_on_scheduler(scheduler_monitor)
        client.run_on_scheduler(scheduler_adaptor)

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


    client.run_on_scheduler(stop_adaptor)

    monitor_data = client.run_on_scheduler(get_monitor_results)
    times, event_loop_intervals, memory_lists = monitor_data["times"], \
                                                monitor_data["event_loop_intervals"], \
                                                monitor_data["memory_lists"]
    client.run_on_scheduler(stop_monitor)

    nworkers = len(list(client.scheduler_info(n_workers=-1)["workers"].keys()))

    dir_name = str(nt) + "_" + str(t)

    try:
        os.mkdir("plots/" + dir_name)
        print(f"Directory '{dir_name}' created successfully.")
    except FileExistsError:
        print(f"Directory '{dir_name}' already exists.")

    os.system("rm -rf plots/" + dir_name + "/*")


    filepath = "plots/" + dir_name

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
        filepath = "plots/" + dir_name + "/" + name_worker

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

