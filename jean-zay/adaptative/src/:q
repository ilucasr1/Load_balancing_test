import sys
import dask
import time
from distributed import Client, Queue, Variable#, Future, get_client, secede, rejoin, as_completed
from dask_jobqueue import SLURMCluster

cluster = SLURMCluster(cores=1, memory='10GB', processes=1, scheduler_options={ 'dashboard_address' : '8787'})

with open("../scheduler_addr.txt", "w") as f:
	f.write(cluster.scheduler.address)

while True:
	time.sleep(10)
