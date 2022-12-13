#!/bin/bash

./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1
./stop.sh >/dev/null 2>&1

score=0

mkdir chfs1 >/dev/null 2>&1
raft_test_case() {
	i=1
    # echo "Testing " $1"."$2;
	while(( $i<=100 ))
	do
		# shellcheck disable=SC2260
		./raft_test $1 $2 > output.log
		cat output.log | grep "Pass ("$1"."$2")";
		if [ $? -ne 0 ];
		then
            echo "Fail " $1"."$2;
			return
		fi;
		let "i++"
		echo "Passed " $1"."$2;
	done
	score=$((score+$3))
}
raft_test_case part3 persist1 5
