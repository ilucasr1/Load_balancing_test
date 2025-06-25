#!/bin/bash

./launch.sh 8000 1 0 	
launch=$!
wait $launch

for i in {2..5}
do
	for j in {0..5}
	do
		./launch.sh 8000 $i $j 	
		launch=$!
		wait $launch
	done
#	./launch.sh 8000 $i 0 	
#	launch=$!
#	wait $launch
done

