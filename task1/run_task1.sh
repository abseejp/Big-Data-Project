# Setup:
# Change scratch location NETID to individual using it
# Make sure to update location of file list
# File was run on NYU DUMBO HPC

# To Use:
# Comment out the line labeled "2" and uncomment the two lines labeled "1". Running these will get all of the files and put them on the Hadoop server
# Comment out the two lines labeled "1" and uncomment the line labeled "2". Running this will loop through a list of files and run them with spark.

# Note: once the datasets are put on hadoop server, they can be physically deleted from scratch



########## 1 ##########
# /usr/bin/hadoop fs -get "/user/hm74/NYCOpenData" "/scratch/mva271"
#######################

input="/scratch/mva271/NYCOpenData/files.txt"
while IFS= read -r line
do
	########## 1 ##########
	# /usr/bin/hadoop fs -put "$line"
	#######################

	# echo "$line"
	# read -p 'skip this file (y/n)?: ' skipvar
	# if [ "$skipvar" = "n" ]
	# then

	########## 2 ##########
	spark-submit --conf spark.executor.memoryOverhead=3G --executor-memory 6G spark.pyspark.python=$PYSPARK_PYTHON profiling.py "$line"
    #######################

    # fi
done < "$input"