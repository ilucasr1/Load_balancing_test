#!/bin/bash

NB_TIMESTEPS="${1:-}"
TIMESTEPS="${2:-}"
WORKER="${3:-}"

if [ -z "${TIMESTEPS}" ]; then	
	BASE_PLOTDIR="${HOME}/Load_balancing_test/ruche/adaptative/plots/default"
else
	BASE_PLOTDIR="${HOME}/Load_balancing_test/ruche/adaptative/plots/${NB_TIMESTEPS}_${TIMESTEPS}"
	if [ -z "${WORKER}" ]; then
		echo ${BASE_PLOTDIR}/plot_adaptative.png
		xdg-open ${BASE_PLOTDIR}/plot_adaptative.png
		echo ${BASE_PLOTDIR}/plot_nb_workers.png
		xdg-open ${BASE_PLOTDIR}/plot_nb_workers.png	
	else
		PLOTDIR="${BASE_PLOTDIR}/worker_${WORKER}"
		find "$PLOTDIR" -type f | while read -r plot; do
			echo "$plot"
			xdg-open "$plot"
		done
	fi
fi



