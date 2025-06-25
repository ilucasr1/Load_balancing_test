#!/bin/bash

#SBATCH --job-name=adaptative_overload
#SBATCH --output=output.out
#SBATCH --time=00:30:00
#SBATCH --ntasks-per-node=1
#SBATCH --ntasks=5
#SBATCH --cpus-per-task=20
#SBATCH --threads-per-core=1
#SBATCH --partition=cpu_short

                                                
BASE_DIR=${HOME}/overload_test/adaptative

SCHEFILE=${BASE_DIR}/scheduler.json
DASK_WORKERS=1
THREADS_PER_WORKER=1

DEFAULT_NB_TIMESTEPS=10
DEFAULT_TIMESTEPS=10
NB_TIMESTEPS="${1:-$DEFAULT_NB_TIMESTEPS}"
TIMESTEPS="${2:-$DEFAULT_TIMESTEPS}"

echo "SCHEFILE=$SCHEFILE"
echo "DASK_WORKERS=$DASK_WORKERS"
echo "THREADS_PER_WORKER=$THREADS_PER_WORKER"

source ${HOME}/jupyter/bin/activate

#jupyter lab --no-browser --port=8888 --ip=* &

source ${WORKDIR}/bench-in-situ/working_dir/pdi/share/pdi/env.sh

if [ -f "client.txt" ]; then   	
	rm client.txt
fi
if [ -f "dask_scheduler.txt" ]; then   	
	rm dask_scheduler.txt
fi
if [ -f "dask_worker.txt" ]; then 	
	rm dask_worker.txt
fi
if [ -f "analytics.txt" ]; then 	
	rm analytics.txt
fi
if [ -f "simulation.txt" ]; then 	
	rm simulation.txt
fi

#dask scheduler
#srun -N 1 -n 1 -c 1 dask scheduler --dashboard-address 10000 --scheduler-file=$SCHEFILE & #>> dask_scheduler.txt &
#sched_pid=$!

#while ! [ -f ${SCHEFILE} ]; do
#  sleep 3
#done

#timeout 30 bash -c 'until nc -z localhost 10000; do sleep 1; done'

#sync

if [ -f "scheduler_addr.txt" ]; then
	rm scheduler_addr.txt
fi

#Start SLURMCluster
srun -N $DASK_WORKERS -n $DASK_WORKERS -c $DASK_WORKERS  python3 init_cluster.py &
init_pid=$!

#wait $init_pid

while ! [ -f "scheduler_addr.txt" ]; do
	sleep 0.1
done
echo "cluster init"

SCHE_ADDR=$(cat scheduler_addr.txt)
echo "address scheduler : $SCHE_ADDR"

export MALLOC_TRIM_THRESHOLD_=0
echo "MALLOC_TRIM_THRESHOLD_=$MALLOC_TRIM_THRESHOLD_"

#dask workers
srun -N $DASK_WORKERS -n $DASK_WORKERS -c $DASK_WORKERS dask worker $SCHE_ADDR --local-directory /tmp  & # >> dask_worker.txt &


#in situ analytics
srun -N $DASK_WORKERS -n $DASK_WORKERS -c $DASK_WORKERS python3 analytics.py $DASK_WORKERS $SCHE_ADDR &
analytics_pid=$!

#simulation
srun -N $DASK_WORKERS -n $DASK_WORKERS -c $DASK_WORKERS python3 simulation.py $DASK_WORKERS $SCHE_ADDR $NB_TIMESTEPS $TIMESTEPS &
simu_pid=$!

wait $analytics_pid
wait $simu_pid

kill -2 $init_pid

deactivate

