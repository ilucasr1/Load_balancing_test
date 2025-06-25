#!/bin/bash

for plot in plots/plot_n8000_nt${1}_*
do
	xdg-open $plot
done
