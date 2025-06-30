import os
import sys
import time
import dask
from distributed  import Client

def client_start(scheduler_addr, max_try=5, current_try=0):
    try:
        client = Client(scheduler_addr)
    except Exception as e:
        current_try += 1
        if tries < max_try:
            print("retrying ...", flush=True)
            time.sleep(0.1)
            client_start(scheduler_addr, max_try=max_try, current_try=current_try)
        else:
            print("Didn't manage to start the simulation client", flush=True)
            os.exit(e)
    return client

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
#    dask_scheduler._memory_samples = [[[], [], [], [], []], [[], [], [], [], []]]
    dask_scheduler._memory_samples = dict()
#    dask_scheduler._timestamps = []
    dask_scheduler.should_stop_monitor = asyncio.Event()
    dask_scheduler._global_times = []
    async def monitor():
        print("Scheduler monitor started", flush=True)

        while not dask_scheduler.should_stop_monitor.is_set():
            dask_scheduler._global_times.append(time.time())
            try:

                for i, (wid, ws) in enumerate(dask_scheduler.workers.items()):
                    if wid not in dask_scheduler._memory_samples:
                        dask_scheduler._memory_samples[wid] = dict()
                        labels = ["times", "event_loop", "process", "managed", "unmanaged_old", "unmanaged_recent", "spilled"]
                        for j in range(len(labels)):
                            dask_scheduler._memory_samples[wid][labels[j]] = []

                    mem = ws.memory
                    dask_scheduler._memory_samples[wid]["times"].append(time.time())
                    dask_scheduler._memory_samples[wid]["event_loop"].append(ws.metrics["event_loop_interval"])
                    dask_scheduler._memory_samples[wid]["process"].append(mem.process)
                    dask_scheduler._memory_samples[wid]["managed"].append(mem.managed)
                    dask_scheduler._memory_samples[wid]["unmanaged_old"].append(mem.unmanaged_old)
                    dask_scheduler._memory_samples[wid]["unmanaged_recent"].append(mem.unmanaged_recent)
                    dask_scheduler._memory_samples[wid]["spilled"].append(mem.spilled)
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
    return (dask_scheduler._memory_samples, dask_scheduler._global_times)

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

