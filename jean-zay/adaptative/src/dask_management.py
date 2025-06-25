import stdlib
import os
import sys
import time
import dask
from distributed  import Client

def client_start(max_try=5):
    tries = 0
    try:
        client = Client(scheduler_addr)
    except Exception as e:
        tries += 1
        if tries < max_try:
            print("retrying ...", flush=True)
            client_start(max_try)
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
    dask_scheduler._memory_samples = [[[], [], [], [], []], [[], [], [], [], []]]
    dask_scheduler._timestamps = []
    dask_scheduler.should_stop_monitor = asyncio.Event()

    async def monitor():
        print("Scheduler monitor started", flush=True)

        while not dask_scheduler.should_stop_monitor.is_set():
            try:
                dask_scheduler._timestamps.append(time.time())

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
