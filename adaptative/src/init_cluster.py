import sys
import dask
import time
from distributed import Client, Queue, Variable#, Future, get_client, secede, rejoin, as_completed
from dask_jobqueue import SLURMCluster

cluster = SLURMCluster(cores=1, processes=1, memory='4GB', local_directory='/tmp', job_extra_directives={ '-p cpu_med', '--output=output/output_job_%j.out', '--error=output/error_job_%j.out' }, scheduler_options={ 'dashboard_address' : '8888'})

print(cluster.job_script())

cluster.adapt(minimum=0, maximum=3)

with open("output/scheduler_addr.txt", "w") as f:
	f.write(cluster.scheduler.address)

while True:
	time.sleep(10)
