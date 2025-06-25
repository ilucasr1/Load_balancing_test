#!/bin/bash

BASE_DIR=${HOME}/overload_test/adaptative

SCHEFILE=scheduler.json
DASK_WORKERS=1
THREADS_PER_WORKER=1

DEFAULT_MATRIX_DIM=5000
DEFAULT_NB_TIMESTEPS=10
DEFAULT_TIMESTEPS=10
MATRIX_DIM="${1:-$DEFAULT_MATRIX_DIM}"
NB_TIMESTEPS="${2:-$DEFAULT_NB_TIMESTEPS}"
TIMESTEPS="${3:-$DEFAULT_TIMESTEPS}"

echo "SCHEFILE=$SCHEFILE"
echo "DASK_WORKERS=$DASK_WORKERS"
echo "THREADS_PER_WORKER=$THREADS_PER_WORKER"

source ${HOME}/jupyter/bin/activate

if [ -f "dask_scheduler.txt" ]; then   	
	rm dask_scheduler.txt
fi
if [ -f "dask_worker.txt" ]; then 	
	rm dask_worker.txt
fi

#dask scheduler
dask scheduler --interface=lo --scheduler-file=$SCHEFILE --preload log_filter.py >> dask_scheduler.txt &
sched_pid=$!

# Wait for the SCHEFILE to be created
while ! [ -f ${SCHEFILE} ]; do
  sleep 3
done
sync
export MALLOC_TRIM_THRESHOLD_=0
echo "MALLOC_TRIM_THRESHOLD_=$MALLOC_TRIM_THRESHOLD_"

#dask workers
dask worker --interface lo --local-directory /tmp --nworkers=$DASK_WORKERS --nthreads=$THREADS_PER_WORKER --scheduler-file=$SCHEFILE --preload log_filter.py >> dask_worker.txt &

#in situ analytics
python3 analytics.py $DASK_WORKERS $SCHEFILE &
analytics_pid=$!

#simulation
python3 simulation.py $DASK_WORKERS $SCHEFILE $MATRIX_DIM $NB_TIMESTEPS $TIMESTEPS &
simu_pid=$!

wait $analytics_pid
wait $simu_pid

sleep 5

kill -2 $sched_pid

deactivate

