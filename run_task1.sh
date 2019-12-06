# TO USE:
# Change scratch location USER to individual using it
# File was run on NYU DUMBO HPC

# /usr/bin/hadoop fs -get "/user/hm74/NYCOpenData" "/scratch/mva271"

input="/scratch/mva271/NYCOpenData/files.txt"
while IFS= read -r line
do
	# /usr/bin/hadoop fs -put "$line"
	# echo "$line"
	# read -p 'skip this file (y/n)?: ' skipvar
	# if [ "$skipvar" = "n" ]
	# then
	spark-submit --conf spark.pyspark.python=$PYSPARK_PYTHON profiling.py "$line"
    # fi
done < "$input"
