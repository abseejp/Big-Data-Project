#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
import string
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import *


if __name__ == "__main__":

	sc = SparkContext()

	spark = SparkSession \
		.builder \
		.appName("profiling") \
		.config("spark.some.config.option", "some-value") \
		.getOrCreate()

	sqlContext = SQLContext(sparkContext = spark.sparkContext, sparkSession = spark)

	# get command-line arguments
	inFile = sys.argv[1]

	print ("Executing data profiling with input from " + inFile)

	dataset = sqlContext.read.format('csv').options(header='true', inferschema='true', delimiter='\t', ignoreLeadingWhiteSpace='true', ignoreTrailingWhiteSpace='true').load(inFile)
	dataset.createOrReplaceTempView("dataset")
	sqlContext.cacheTable("dataset")

	attributes = dataset.columns
	dataset.printSchema()

#==================== DataFrame Operations =====================

	# Attribute Data Type array
	attribute_types = dict(dataset.dtypes)

	# Frequent Itemsets
	# need to retrieve sets of size 2, 3, and 4
	itemSets = dataset.freqItems(dataset.columns)

#=================== Storage Data Structures ====================

	numData = dict() # stores INT data: max, min, mean, stddev
	strData = dict() # stores TEXT data: top-5 max length, top-5 min length, avg length
	dateData = dict() # stores DATE data: earliest date, latest date
	prim_key = [] # stores suspected primary key(s) for tsv file

#================== Loop through every column ===================

	for attr in attributes:
		print(attr)
		
		# Count all values for a column
		num_col_values = dataset.select(attr).count()
		print("num_col_values:", num_col_values)
		
		# Count number of distinct values
		num_distinct_col_values = dataset.agg(countDistinct(col(attr)).alias("count_distinct")).collect()[0]["count_distinct"]
		print("num_distinct_col_values:",num_distinct_col_values)
		
		# Finding potential primary keys
		if(num_distinct_col_values >= num_col_values*0.9):
			prim_key.append(attr)

		# Find data types of all values in the column
		# def findType(x):
		# 	return str(type(x))

		# udftype = udf(findType, StringType())
		# column_types = dataset.select(attr).rdd.map(lambda x: (x, udftype(x))).toDF()
		# column_types.show()
		# type_counts = column_types.groupBy("_2").count().alias("count_types")
		# type_counts.show()
		dtype = attribute_types[attr]
		
		if(dtype == 'int'):
			
			stats = dataset.agg(max(col(attr)).alias("max"), min(col(attr)).alias("min"), mean(col(attr)).alias("mean"), stddev(col(attr)).alias("stddev"))
			col_max = stats.collect()[0]["max"]
			col_min = stats.collect()[0]["min"]
			col_mean = stats.collect()[0]["mean"]
			col_stddev = stats.collect()[0]["stddev"]

			numData[attr] = [col_max, col_min, col_mean, col_stddev]

		elif(dtype == 'date'):
			# Format date attributes to same structure
			continue
		
		elif(dtype == 'string'):
			
			# Find top-5 max and min string lengths
			text_lengths = dataset.select(length(attr).alias('length'))
			
			text_lengths = text_lengths.orderBy(text_lengths.length.desc())
			max_5 = text_lengths.limit(5).collect()
			max_5 = [row["length"] for row in max_5]
			print(max_5)
			
			text_lengths = text_lengths.orderBy(text_lengths.length.asc())
			min_5 = text_lengths.limit(5).collect()
			min_5 = [row["length"] for row in min_5]
			print(min_5)

			# Find average string length
			avg = dataset.agg(mean(col(attr)).alias("mean")).collect()[0]["mean"]
			
	#================== Saving as JSON file =====================

	# need to verify that this works
	# df.write.format('json').save(path=os.getcwd())