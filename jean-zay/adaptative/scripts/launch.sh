#!/bin/bash

#SBATCH --job-name=adaptative_overload
#SBATCH --output=output/output_job_%j.out
#SBATCH --error=output/error_job_%j.out
#SBATCH --time=00:05:00
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --partition=cpu_p1
#SBATCH --account=jyd@cpu
                                                
BASE_DIR=${WORK}/overload_test/adaptative
SCRIPT_DIR=${BASE_DIR}/scripts
SRC_DIR=${BASE_DIR}/src
OUTPUT_DIR=${BASE_DIR}/output

DASK_WORKERS=1
THREADS_PER_WORKER=1

DEFAULT_NB_TIMESTEPS=10
DEFAULT_TIMESTEPS=10
NB_TIMESTEPS="${1:-$DEFAULT_NB_TIMESTEPS}"
TIMESTEPS="${2:-$DEFAULT_TIMESTEPS}"

echo "DASK_WORKERS=$DASK_WORKERS"
echo "THREADS_PER_WORKER=$THREADS_PER_WORKER"

source ${SCRIPT_DIR}/env.sh

#jupyter lab --no-browser --port=8888 --ip=* &

#source ${WORKDIR}/bench-in-situ/working_dir/pdi/share/pdi/env.sh

if [ -f "${OUTPUT_DIR}/client.txt" ]; then   	
	rm ${OUTPUT_DIR}/client.txt
fi
#if [ -f "${OUTPUT_DIR}/dask_scheduler.txt" ]; then   	
#	rm ${OUTPUT_DIR}/dask_scheduler.txt
#fi
if [ -f "${OUTPUT_DIR}/dask_worker.txt" ]; then 	
	rm ${OUTPUT_DIR}/dask_worker.txt
fi
if [ -f "${OUTPUT_DIR}/analytics.txt" ]; then 	
	rm ${OUTPUT_DIR}/analytics.txt
fi
if [ -f "${OUTPUT_DIR}/simulation.txt" ]; then 	
	rm ${OUTPUT_DIR}/simulation.txt
fi

#dask scheduler
#srun -N 1 -n 1 -c 1 dask scheduler --dashboard-address 10000 --scheduler-file=$SCHEFILE & #>> dask_scheduler.txt &
#sched_pid=$!

#while ! [ -f ${SCHEFILE} ]; do
#  sleep 3
#done

#timeout 30 bash -c 'until nc -z localhost 10000; do sleep 1; done'

#sync

if [ -f "${OUTPUT_DIR}/scheduler_addr.txt" ]; then
	rm ${OUTPUT_DIR}/scheduler_addr.txt
fi

#Start SLURMCluster
srun -N $DASK_WORKERS -n $DASK_WORKERS -c $DASK_WORKERS  python3 ${SRC_DIR}/init_cluster.py &
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
srun -N $DASK_WORKERS -n $DASK_WORKERS -c $DASK_WORKERS dask worker $SCHE_ADDR --local-directory ${SCRATCH}/dask_worker & # >> ${OUTPUT_DIR}/dask_worker.txt &


#in situ analytics
srun -N 1 -n 1 -c 1  python3 ${SRC_DIR}/analytics.py $SCHE_ADDR &
analytics_pid=$!

#simulation
srun -N 1 -n 1 -c 1 python3 ${SRC_DIR}/simulation.py $SCHE_ADDR $NB_TIMESTEPS $TIMESTEPS &
simu_pid=$!

wait $analytics_pid
wait $simu_pid

kill -2 $init_pid

conda deactivate

