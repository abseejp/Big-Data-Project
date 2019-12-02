# /usr/bin/hadoop fs -get "/user/hm74/NYCOpenData" "/scratch/mva271"

for filename in /scratch/mva271/NYCOpenData/*.gz; do
	# /usr/bin/hadoop fs -put "$filename"
    spark-submit --conf spark.pyspark.python=$PYSPARK_PYTHON profiling.py "${filename##*/}"
done