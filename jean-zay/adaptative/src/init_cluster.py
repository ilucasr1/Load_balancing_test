import sys
import dask
import time
from distributed import Client, Queue, Variable#, Future, get_client, secede, rejoin, as_completed
from dask_jobqueue import SLURMCluster

cluster = SLURMCluster(cores=1, processes=1, memory='4GB',  account='jyd@cpu' ,scheduler_options={ 'dashboard_address' : '8787'})

cluster.adapt(minimum=0, maximum=2)

with open("output/scheduler_addr.txt", "w") as f:
	f.write(cluster.scheduler.address)

while True:
	time.sleep(10)
