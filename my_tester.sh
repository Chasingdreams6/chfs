#!/bin/bash

score=0

mkdir chfs1 >/dev/null 2>&1

./start.sh

test_if_has_mount(){
	mount | grep -q "chfs_client"
	if [ $? -ne 0 ];
	then
			echo "FATAL: Your ChFS client has failed to mount its filesystem!"
			exit
	fi;
}
test_if_has_mount

##################################################

# run test 1
./test-lab1-part2-a.pl chfs1 | grep -q "Passed all"
if [ $? -ne 0 ];
then
        echo "Failed test-A"
        #exit
else

	ps -e | grep -q "chfs_client"
	if [ $? -ne 0 ];
	then
			echo "FATAL: chfs_client DIED!"
			exit
	else
		score=$((score+20))
		#echo $score
		echo "Passed A"
	fi

fi
test_if_has_mount

# ##################################################

# ./test-lab1-part2-b.pl chfs1 | grep -q "Passed all"
# if [ $? -ne 0 ];
# then
#         echo "Failed test-B"
#         #exit
# else
	
# 	ps -e | grep -q "chfs_client"
# 	if [ $? -ne 0 ];
# 	then
# 			echo "FATAL: chfs_client DIED!"
# 			exit
# 	else
# 		score=$((score+20))
# 		#echo $score
# 		echo "Passed B"
# 	fi

# fi
# test_if_has_mount

##################################################
echo "start test-c"
echo "here finish b part -------" >> ./chfs_client1.log
rm clog.out
./test-lab1-part2-c.pl chfs1  >> clog.out | grep -q "Passed all"
if [ $? -ne 0 ];
then
        echo "Failed test-c"
        #exit
else

	ps -e | grep -q "chfs_client"
	if [ $? -ne 0 ];
	then
			echo "FATAL: chfs_client DIED!"
			exit
	else
		score=$((score+20))
		#echo $score
		echo "Passed C"
	fi

fi
test_if_has_mount

##################################################
./stop.sh