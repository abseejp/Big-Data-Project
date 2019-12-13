# Setup:
# Change scratch location NETID to individual using it
# Make sure to update location of file list
# File was run on NYU DUMBO HPC

# Note: once the datasets are put on hadoop server, they can be physically deleted from scratch

module load python/gnu/3.6.5
module load spark/2.4.0
export PYSPARK_PYTHON='/share/apps/python/3.6.5/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.6.5/bin/python'

########## 1 ##########
/usr/bin/hadoop fs -get "/user/hm74/NYCColumns" "/scratch/mva271"
#######################

input="/scratch/mva271/NYCColumns/files.txt"
while IFS= read -r line
do
	########## 1 ##########
	/usr/bin/hadoop fs -put "/scratch/mva271/NYCColumns/$line"
	#######################

done < "$input"

# Run semantic profiling
spark-submit --conf spark.pyspark.python=$PYSPARK_PYTHON task2.py "cluster1.txt"

# Merge all of the json files together into a single "task2.json"
python merge_jsons.py