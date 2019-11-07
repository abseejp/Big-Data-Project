#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
import string

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import *


# returns the data type for a particular attribute in a dataset
def get_dtype(df,colname):
    return [dtype for name, dtype in df.dtypes if name == colname][0]


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

    # for INTEGER type attributes, compute max, min, mean, stdev
    numData = dict()
    for attr in attributes:

        dtype = get_dtype(dataset, attr)
        
        if(dtype == 'int'):

            query = """select max('""" + attr + """') as maxAttr, min('""" + attr + """') as minAttr, mean('""" + attr + """') as meanAttr,  std('""" + attr + """') as stdAttr from dataset"""
            # result = spark.sql(query).select(format_string('%.2f,%.2f,%.2f,%.2f', result.maxAttr, result.minAttr, result.meanAttr, result.stdAttr)).write.save("result.json",format="json")
            result = spark.sql(query)
            result.createOrReplaceTempView("result")
            # result = result.select(format_string('%.2f,%.2f,%.2f,%.2f', result.maxAttr, result.minAttr, result.meanAttr, result.stdAttr))
            spark.sql("select * from result").show()
            
            # numData[attr] = [result.maxAttr.val, result.minAttr.val, result.meanAttr.val, result.stdAttr.val]
            # print(attr, [result.maxAttr.val, result.minAttr.val, result.meanAttr.val, result.stdAttr.val])