#!/bin/bash

MATRIX_DIM="${1:-}"
NB_TIMESTEPS="${2:-}"
TIMESTEPS="${3:-}"

if [ -z "${TIMESTEPS}" ]; then	
	PLOTDIR="${HOME}/overload_test/normal/plots/default"
else
	PLOTDIR="${HOME}/overload_test/normal/plots/${MATRIX_DIM}_${NB_TIMESTEPS}_${TIMESTEPS}"
fi

find "$PLOTDIR" -type f | while read -r plot; do
    echo "$plot"
    xdg-open "$plot"
done

