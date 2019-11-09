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

    sqlContext = SQLContext(spark)

    # get command-line arguments
    inFile = sys.argv[1]

    print ("Executing data profiling with input from " + inFile)

    dataset = sqlContext.read.format('csv').options(header='true', inferschema='true', delimiter='\t', ignoreLeadingWhiteSpace='true', ignoreTrailingWhiteSpace='true').load(inFile)
    dataset.createOrReplaceTempView("dataset")
    sqlContext.cacheTable("dataset")
    spark.sql("select COUNT(*) from dataset").show()

    attributes = dataset.columns
    dataset.printSchema()

    #================= DataFrame Operations ==================

    # Count all rows
    count = dataset.count()

    # Count distinct values
    distinct_count = dataset.distinct().count()

    # Attribute Data Type array
    attribute_types = dict(dataset.dtypes)

    # Frequent Itemsets
    # need to retrieve sets of size 2, 3, and 4
    itemSets = dataset.freqItems(dataset.columns)



    #================= Spark SQL Operations ===================

    # for INTEGER type attributes, compute max, min, mean, stdev
    numData = dict()
    for attr in attributes:

        # Finding potential primary keys
        num_distinct_col_values = dataset.agg(countDistinct(col(attr)).alias("count"))
        prim_key = []
        if(num_distinct_col_values >= count*0.9):
            prim_key.append(attr)


        # Agrregation on INTEGER column types
        dtype = attribute_types(attr)
        
        if(dtype == 'int'):

            query = """select max('""" + attr + """') as maxAttr, min('""" + attr + """') as minAttr, mean('""" + attr + """') as meanAttr,  std('""" + attr + """') as stdAttr from dataset"""
            # result = spark.sql(query).select(format_string('%.2f,%.2f,%.2f,%.2f', result.maxAttr, result.minAttr, result.meanAttr, result.stdAttr)).write.save("result.json",format="json")
            result = spark.sql(query)
            result.createOrReplaceTempView("result")
            # result = result.select(format_string('%.2f,%.2f,%.2f,%.2f', result.maxAttr, result.minAttr, result.meanAttr, result.stdAttr))
            spark.sql("select * from result").show()
            
            # numData[attr] = [result.maxAttr.val, result.minAttr.val, result.meanAttr.val, result.stdAttr.val]
            # print(attr, [result.maxAttr.val, result.minAttr.val, result.meanAttr.val, result.stdAttr.val])



    #================== Saving as JSON file =====================

    # need to verify that this works
    # df.write.format('json').save(path=os.getcwd())