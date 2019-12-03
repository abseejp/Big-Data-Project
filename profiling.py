#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
import string
import os
import json

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

	dataset = sqlContext.read.format('csv').options(header='true', inferschema='true', delimiter='\t', ignoreLeadingWhiteSpace='true', ignoreTrailingWhiteSpace='true', emptyValues='Null').load(inFile)
	dataset.createOrReplaceTempView("dataset")
	sqlContext.cacheTable("dataset")

	end = len(inFile)
	inFile = inFile[:end-7]

	dataset.printSchema()

	dataset = dataset.select([col(c).alias(c.replace(".", "").replace("`", "")) for c in ["`" + x + "`" for x in dataset.columns]]) # pyspark cannot handle '.' in headers
	attributes = dataset.columns

#=================== General DF Operations ====================

	# Count all values for a column
	num_col_values = dataset.count()
	print("num_col_values:", num_col_values)

	# Attribute Data Type array
	attribute_types = dict(dataset.dtypes)

	# Frequent Itemsets
	# need to retrieve sets of size 2, 3, and 4
	itemSets = dataset.na.drop() # need to remove Null values to avoid error
	itemSets = itemSets.freqItems(dataset.columns, support=(4 / num_col_values))

#=================== Storage Data Structures ====================

	dataset_dict = {
		"dataset_name": inFile,
		"columns": [],
		"key_column_candidates": []
	}

#================== Loop through every column ===================

	for attr in attributes:
		print("")
		print(attr)
		dtype = attribute_types[attr] # fetch datatype of the current column

		#================== Metadata Profiling ===================

		val_count = dataset.groupBy(attr).count()
		
		# Count all empty values for a column
		# num_col_empty = val_count.filter(col(attr).isNull() | col(attr) == 'No Data' | col(attr) == '*' | col(attr) == 'NA').collect()
		num_col_empty = val_count.filter(col(attr).rlike('[^\\*]|^$|No Data|NA|N\\A|None')).collect()
		if(len(num_col_empty) > 0):
			num_col_empty = num_col_empty[0]["count"]
		else:
			num_col_empty = 0
		print("num_col_empty:", num_col_empty)

		# Count all non-empty values for a column
		num_col_notempty = num_col_values - num_col_empty
		print("num_col_notempty:", num_col_notempty)

		# ****** Remove junk from dataset ******
		cleaned_dataset = dataset.exceptAll(dataset.filter(col(attr).isNull())) # drop the entire row if any cells are empty in it
		cleaned_dataset = cleaned_dataset.exceptAll(cleaned_dataset.filter(cleaned_dataset[attr].rlike('No Data|NA|N\\A|None'))) # remove entries with 'No Data', 'NA'
		# **************************************
		
		if(dtype == 'string'):
			# ignore any characters in string that are not UTF-8 encoded
			def fix_encoding(x):
				return x.encode("ascii", "ignore").decode("ascii")
			udfencode = udf(fix_encoding, StringType())
			cleaned_dataset = cleaned_dataset.withColumn(attr, udfencode(attr))	
			cleaned_dataset = cleaned_dataset.exceptAll(cleaned_dataset.filter(cleaned_dataset[attr].rlike('[\\*]|^$|%'))) # remove garbage values from conversion

		# Count number of distinct values
		num_distinct_col_values = cleaned_dataset.agg(countDistinct(col(attr)).alias("count_distinct")).collect()[0]["count_distinct"]
		print("num_distinct_col_values:", num_distinct_col_values)
		
		# Finding potential primary keys
		if(num_distinct_col_values >= num_col_values*0.9):
			dataset_dict["key_column_candidates"].append(attr)
		# Top 5 most frequent values
		cleaned_count = cleaned_dataset.groupBy(attr).count()
		top_5_frequent = cleaned_count.orderBy(cleaned_count["count"].desc())
		top_5_frequent = top_5_frequent.limit(5).collect()
		top_5_frequent = [row[attr] for row in top_5_frequent]
		print("top 5 frequent values:", top_5_frequent)		

		# Find number of frequent values
		arrlen_udf = udf(lambda s: len(s), IntegerType())
		num_freq_items = itemSets.withColumn("size", arrlen_udf(itemSets[attr + "_freqItems"])).collect()[0]["size"]
		print("num_freq_items:", num_freq_items)

		#====================== Data Typing =======================

		# Find data types of all values in the column
		# def findType(x):
		# 	return str(type(x))

		# udftype = udf(findType, StringType())
		# column_types = dataset.rdd.map(lambda x: (x[attr], udftype(x[attr]))).toDF()
		# column_types.show()
		# type_counts = column_types.groupBy("_2").count().alias("count_types")
		# type_counts.show()

		# Drop all empty cells from dataset
		# cleaned_dataset = dataset.dropna(how='any') # drop the entire row if any cells are NaN in it



		# be wary of entry 's', 'R' ...


		#================== Column-Type Profiling ===================
		
		column = {
			"column_name": attr,
			"number_non_empty_cells": num_col_notempty,
			"number_empty_cells": num_col_empty,
			"number_distinct_values": num_distinct_col_values,
			"frequent_values": num_freq_items,
			"data_types": []
			}


		# work with list of dtypes per columns, check " 'int' in dtypes ", filter columns for these specific types
		# Fixed misclassified datatypes
		if(dtype == 'string'):

			# Check if string column contains numeric values casted as strings:
			num_non_ints = cleaned_dataset.select(attr, col(attr).cast("float").isNotNull().alias("Value")).filter(col("Value") == False).count() # count number of non INT entries
			
			if(num_non_ints > 0): # at least one entry is not a pure number
				if(num_non_ints / num_col_values <= 0.1): # if it appears in more than 10% of the columns, it is most likely not an INT column
					cleaned_dataset = cleaned_dataset.select(attr, col(attr).cast("float").isNotNull().alias("Value")).filter(col("Value") == True) # remove the non INT entries
					dtype = 'int'
			else:
				dtype = 'int'

		# classifying numeric columns representing dates as 'date'
		# if(('year' in attr.lower() and len(attr) == 4) or ('day' in attr.lower() and len(attr) == 3) or ('month' in attr.lower() and len(attr) == 5) or 'period' in attr.lower() or ('week' in attr.lower() and len(attr) == 4)):
		if('year' in attr.lower() or 'day' in attr.lower() or 'month' in attr.lower() or 'period' in attr.lower() or 'week' in attr.lower()):
			dtype = 'date'


		print(dtype)

		# Profile the column based on datatype
		if(dtype == 'int' or dtype == 'double' or dtype == 'float' or dtype == 'long'):
			
			stats = cleaned_dataset.agg(max(col(attr)).alias("max"), min(col(attr)).alias("min"), mean(col(attr)).alias("mean"), stddev(col(attr)).alias("stddev"))
			col_max = stats.collect()[0]["max"]
			col_min = stats.collect()[0]["min"]
			col_mean = stats.collect()[0]["mean"]
			col_stddev = stats.collect()[0]["stddev"]

			# Add column to JSON
			column["data_types"].append({
					"type": "INTEGER (LONG)",
					"count": 0, ################ UPDATE THIS #############################
					"max_value": col_max,
					"min_value": col_min,
					"mean": col_mean,
					"stddev": col_stddev
				})

		elif(dtype == 'date'):
			# Fetch the earliest and latest dates

			if('year' in attr.lower() or 'day' in attr.lower() or 'month' in attr.lower() or 'period' in attr.lower() or 'week' in attr.lower()):

				def findDate(x):
					months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']
					for month in months:
						lower_x = x.lower()
						x_month_index = lower_x.find(month)
						
						if(x_month_index != -1):
							date = str(months.index(month)) + " " + x[x_month_index+len(month):] # replace string month with its numerical equivalent (e.g. Jan -> 1, Feb -> 2, ...)
							return date

					return x # if string month is not present, just return original date string

				udfdatefind = udf(findDate, StringType())
				cleaned_dataset = cleaned_dataset.withColumn(attr, udfdatefind(attr))

				stats = cleaned_dataset.agg(max(col(attr)).alias("max"), min(col(attr)).alias("min"))
				col_max = stats.collect()[0]["max"]
				col_min = stats.collect()[0]["min"]			
			else:
				col_max = 0
				col_min = 0
				# TRYING TO BREAK ON A TRUE DATE COLUMN FOR DEBUGGING
				text_lengths = text_lengths.orderBy(text_lengths.cars.desc())

			# Add column to JSON
			column["data_types"].append({
					"type": "DATE/TIME",
					"count": 0, ################ UPDATE THIS #############################
					"max_value": col_max,
					"min_value": col_min
				})

		elif(dtype == 'string'):
			
			# Find top-5 longest strings
			text_lengths = cleaned_dataset.withColumn("length", length(attr))			
			text_lengths = text_lengths.orderBy(text_lengths.length.desc())
			max_5 = text_lengths.limit(5).collect()

			max_5_length = [row[attr] for row in max_5] # save the string values

			# Find the top-5 shortest words
			text_lengths = text_lengths.orderBy(text_lengths.length.asc())
			min_5 = text_lengths.limit(5).collect()

			min_5_length = [row[attr] for row in min_5]

			# Find average string length
			avg_length = text_lengths.agg(mean(col("length")).alias("mean")).collect()[0]["mean"]
			
			# Add column to JSON
			column["data_types"].append({
				"type": "TEXT",
				"count": 0, ################ UPDATE THIS #############################
				"shortest_values": min_5_length,
				"longest_values": max_5_length,
				"average_length": avg_length
				})
		
		else:
			# Add column to JSON
			column["data_types"].append({
				"semantic_type": dtype,
				"count": 0
			})

		dataset_dict["columns"].append(column)

	#================== Saving as JSON file =====================

	json_filename = inFile + ".json"
	with open(json_filename,"w") as f:
		json.dump(dataset_dict, f)