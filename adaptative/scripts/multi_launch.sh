#!/bin/bash


for size in 5000 6000 7000 8000
do
	./launch.sh $size 1 0 	
	launch=$!
	wait $launch
	for i in 5 10 15 20
	do
		for j in 0 2 4
		do
			./launch.sh $size $i $j 	
			launch=$!
			wait $launch
		done
	#	./launch.sh 8000 $i 0 	
	#	launch=$!
	#	wait $launch
	done
done
