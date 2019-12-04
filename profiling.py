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

	dataset = sqlContext.read.format('csv').options(header='true', inferschema='true', delimiter='\t', ignoreLeadingWhiteSpace='true', ignoreTrailingWhiteSpace='true').load(inFile)
	dataset.createOrReplaceTempView("dataset")
	sqlContext.cacheTable("dataset")

	end = len(inFile)
	inFile = inFile[:end-7]

	dataset.printSchema()

	dataset = dataset.select([col(c).alias(c.replace(".", "").replace("`", "")) for c in ["`" + x + "`" for x in dataset.columns]]) # pyspark cannot handle '.' in headers
	attributes = dataset.columns

	# Count all values for a column
	num_col_values = dataset.count()
	print("num_col_values:", num_col_values)

	# Header data type dictionary
	header_types = dict(dataset.dtypes)
	

#===================== Storage Dictionary ======================

	dataset_dict = {
		"dataset_name": inFile,
		"columns": [],
		"key_column_candidates": []
	}

#================== Loop through every column ===================

	for attr in attributes:
		print("")
		print(attr)
		header_dtype = header_types[attr] # fetch header datatype of the current column

		#================== Metadata Profiling ===================

		val_count = dataset.groupBy(attr).agg(count(col(attr)).alias("val_count"))
		
		# Count all empty values for a column
		num_col_labeled_empty = val_count.filter(col(attr).rlike('\\*|^$|No Data|NA|N\\A|None|null')).collect()
		if(len(num_col_labeled_empty) > 0):
			num_col_labeled_empty = num_col_labeled_empty[0]["val_count"]
		else:
			num_col_labeled_empty = 0

		num_col_null = dataset.select(col(attr).isNull()).count()
		num_col_empty = num_col_null + num_col_labeled_empty
		print("num_col_empty:", num_col_empty)

		# Count all non-empty values for a column
		num_col_notempty = (num_col_values - num_col_empty)
		if(num_col_notempty <= 0):
			num_col_notempty = 0
		print("num_col_notempty:", num_col_notempty)
		
		# ************ Remove junk from dataset ************

		cleaned_dataset = dataset.exceptAll(dataset.filter(col(attr).isNull())) # drop the entire row if any cells are empty in it
		cleaned_dataset = cleaned_dataset.exceptAll(cleaned_dataset.filter(cleaned_dataset[attr].rlike('^$|No Data|NA|N\\A|None|%|null'))) # remove entries with 'No Data', 'NA', 'None'
		
		# **************************************************
		
		"""
		# convert string encodings into ascii if special characters appear
		if(header_dtype == 'string'):
			def fix_encoding(x):
				return x.encode("ascii", "ignore").decode("ascii")
			udfencode = udf(fix_encoding, StringType())
			cleaned_dataset = cleaned_dataset.withColumn(attr, udfencode(attr))	
			cleaned_dataset = cleaned_dataset.exceptAll(cleaned_dataset.filter(cleaned_dataset[attr].rlike('\\*|^$|%'))) # remove garbage values from conversion
		"""

		# Count number of distinct values
		num_distinct_col_values = cleaned_dataset.agg(countDistinct(col(attr)).alias("count_distinct")).collect()[0]["count_distinct"]
		print("num_distinct_col_values:", num_distinct_col_values)
		
		# Finding potential primary keys
		if(num_distinct_col_values >= num_col_values*0.9 and num_col_empty == 0):
			dataset_dict["key_column_candidates"].append(attr)
		# Top 5 most frequent values
		cleaned_count = cleaned_dataset.groupBy(attr).agg(count(col(attr)).alias("val_count"))
		top_5_frequent = cleaned_count.orderBy(cleaned_count["val_count"].desc())
		top_5_frequent = top_5_frequent.limit(5).collect()
		top_5_frequent = [row[attr] for row in top_5_frequent]
		print("top 5 frequent values:", top_5_frequent)		

		# Find number of frequent values
		# need to retrieve sets of size 2, 3, and 4
		# itemSets = cleaned_dataset.na.drop() # need to remove Null values to avoid error
		# itemSets = itemSets.freqItems([attr], support=(0.1))

		# arrlen_udf = udf(lambda s: len(s), IntegerType())
		# num_freq_items = itemSets.withColumn("size", arrlen_udf(itemSets[attr + "_freqItems"])).collect()[0]["size"]
		# print("num_freq_items:", num_freq_items)

		#====================== Data Typing =======================

		# array of possible column datatypes
		dtypes = [header_dtype]

		column = {
			"column_name": attr,
			"number_non_empty_cells": num_col_notempty,
			"number_empty_cells": num_col_empty,
			"number_distinct_values": num_distinct_col_values,
			"frequent_values": top_5_frequent,
			"data_types": []
			}

		# Fixed misclassified datatypes
		if(header_dtype == 'string'):

			# Check if string column contains numeric values casted as strings:
			num_ints = cleaned_dataset.select(attr, col(attr).cast("int").isNotNull().alias("isINT")).filter(col("isINT") == True).count() # count number of INT entries
			if(num_ints > 0): # at least one entry is a number
				dtypes.append('int')

		# classifying numeric columns representing dates as 'date'
		attr_ = attr + " " # verifying label is actually for a date (avoids cases when "yearly" or "years" etc.)
		if('year ' in attr_.lower() or 'day ' in attr_.lower() or 'month ' in attr_.lower() or 'period' in attr_.lower() or 'week ' in attr_.lower() or 'date ' in attr_.lower()):
			dtypes = ['date']


		print(dtypes)

		#================== Column-Type Profiling ===================

		for dtype in dtypes:

			if(dtype == 'int' or dtype == 'double' or dtype == 'float' or dtype == 'long'):
				cleaned_dataset_ints = cleaned_dataset.select(attr, col(attr).cast("int").isNotNull().alias("isINT")).filter(col("isINT") == True) # remove possible non INT entries
				int_count = cleaned_dataset_ints.count()
				
				cleaned_dataset_ints = cleaned_dataset_ints.withColumn(attr, col(attr).cast("int"))
				stats = cleaned_dataset_ints.agg(max(col(attr)).alias("max"), min(col(attr)).alias("min"), mean(col(attr)).alias("mean"), stddev(col(attr)).alias("stddev"))
				col_max = stats.collect()[0]["max"]
				col_min = stats.collect()[0]["min"]
				col_mean = stats.collect()[0]["mean"]
				col_stddev = stats.collect()[0]["stddev"]

				# Add column to JSON
				column["data_types"].append({
						"type": "INTEGER (LONG)",
						"count": int_count,
						"max_value": col_max,
						"min_value": col_min,
						"mean": col_mean,
						"stddev": col_stddev
					})

			elif(dtype == 'date'):
				
				# Fetch the earliest and latest dates

				# For non date formats (just years, months, etc.)
				if('year' in attr.lower() or 'day' in attr.lower() or 'month' in attr.lower() or 'period' in attr.lower() or 'week' in attr.lower()):

					if(header_dtype == 'string'):

						# replace string month with its numerical equivalent (e.g. Jan -> 1, Feb -> 2, ...)
						def findDate(x):
							months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']
							for month in months:
								lower_x = x.lower()
								x_month_index = lower_x.find(month)
								
								if(x_month_index != -1):
									date = str(months.index(month)) + " " + x[x_month_index+len(month):]
									return date

							return x # if string month is not present, just return original date string

						udfdatefind = udf(findDate, StringType())
						cleaned_dataset = cleaned_dataset.withColumn(attr, udfdatefind(attr))

					# compute column max and min values
					stats = cleaned_dataset.agg(max(col(attr)).alias("max"), min(col(attr)).alias("min"), count(col(attr)).alias("date_count")).collect()
					col_max = stats[0]["max"]
					col_min = stats[0]["min"]
					date_count = stats[0]["date_count"]

				else: # for formatted data

					if(header_dtype == 'string'): # stored as strings
						dates = cleaned_dataset.select(col(attr), 
							when(to_date(col(attr),"yyyy-MM-dd").isNotNull(), to_date(col(attr),"yyyy-MM-dd"))
							.when(to_date(col(attr),"yyyy MM dd").isNotNull(), to_date(col(attr),"yyyy MM dd"))
							.when(to_date(col(attr),"MM/dd/yyyy").isNotNull(), to_date(col(attr),"MM/dd/yyyy"))
							.when(to_date(col(attr),"yyyy MMMM dd").isNotNull(), to_date(col(attr),"yyyy MMMM dd"))
							.when(to_date(col(attr),"yyyy MMMM dd E").isNotNull(), to_date(col(attr),"yyyy MMMM dd E"))
							.otherwise("Unknown Format").alias("Formatted Date"))

						dates = dates.filter(col("Formatted Date") != "Unknown Format") # ignore dates that could not be formatted
						sorted_dates = dates.select("Formatted Date").orderBy(dates["Formatted Date"].desc())
						stats = sorted_dates.select(first("Formatted Date").alias("latest_date"), last("Formatted Date").alias("earliest_date"), count(col("Formatted Date")).alias("date_count")).collect()
						col_max = stats[0]["latest_date"]
						col_min = stats[0]["earliest_date"]
						date_count = stats[0]["date_count"]

					else: # stored in timestamp format
						sorted_dates = cleaned_dataset.select(attr).orderBy(cleaned_dataset[attr].desc())
						stats = sorted_dates.select(first(attr).alias("latest_date"), last(attr).alias("earliest_date"), count(col(attr)).alias("date_count")).collect()
						col_max = stats[0]["latest_date"]
						col_min = stats[0]["earliest_date"]
						date_count = stats[0]["date_count"]

				# Add column to JSON
				column["data_types"].append({
						"type": "DATE/TIME",
						"count": date_count,
						"max_value": col_max,
						"min_value": col_min
					})

			elif(dtype == 'string'):
				cleaned_dataset_string = cleaned_dataset.select(attr, col(attr).cast("float").isNotNull().alias("isINT")).filter(col("isINT") == False) # remove possible INT entries
				
				text_lengths = cleaned_dataset_string.withColumn("length", length(attr))
				
				# Find top-5 longest strings						
				text_lengths = text_lengths.orderBy(text_lengths.length.desc())
				max_5 = text_lengths.limit(5).collect()

				max_5_length = [row[attr] for row in max_5] # save the string values

				# Find the top-5 shortest words
				text_lengths = text_lengths.orderBy(text_lengths.length.asc())
				min_5 = text_lengths.limit(5).collect()

				min_5_length = [row[attr] for row in min_5]

				# Find average string length
				avg_length = text_lengths.agg(mean(col("length")).alias("mean")).collect()[0]["mean"]
				
				# Count number of string entries
				text_count  = cleaned_dataset_string.count()

				# Add column to JSON
				column["data_types"].append({
					"type": "TEXT",
					"count": text_count,
					"shortest_values": min_5_length,
					"longest_values": max_5_length,
					"average_length": avg_length
					})

		# add the column to the dataset dictionary
		dataset_dict["columns"].append(column)



	#================== Saving as JSON file =====================

	json_filename = inFile + ".json"
	with open(json_filename,"w") as f:
		json.dump(dataset_dict, f)