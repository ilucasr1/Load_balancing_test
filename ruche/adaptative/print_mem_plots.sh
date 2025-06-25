#!/bin/bash

NB_TIMESTEPS="${1:-}"
TIMESTEPS="${2:-}"

if [ -z "${TIMESTEPS}" ]; then	
	PLOTDIR="${HOME}/overload_test/ruche/adaptative/plots/default"
else
	PLOTDIR="${HOME}/overload_test/ruche/adaptative/plots/${NB_TIMESTEPS}_${TIMESTEPS}"
fi

find "$PLOTDIR" -type f | while read -r plot; do
    echo "$plot"
    xdg-open "$plot"
done

