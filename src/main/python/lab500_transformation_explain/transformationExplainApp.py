"""
* Transformation explained.
*
* @author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (lit,col,concat,expr)
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Step 1 - Creates a session on a local master
spark = SparkSession.builder.appName("Analysing Catalyst's behavior") \
                    .master("local[*]").getOrCreate()

# Step 2 - Reads a CSV file with header, stores it in a dataframe
df = spark.read.csv(header=True, inferSchema=True,path=absolute_file_path)

df0 = df

# Step 3 - Build a bigger dataset
df = df.union(df0)

# Step 4 - Cleanup. preparation
df = df.withColumnRenamed("Lower Confidence Limit", "lcl") \
       .withColumnRenamed("Upper Confidence Limit", "ucl")

# Step 5 - Transformation
df =  df.withColumn("avg", expr("(lcl+ucl)/2")) \
        .withColumn("lcl2", col("lcl")) \
        .withColumn("ucl2", col("ucl"))

# Step 6 - explain
df.explain()

spark.stop()

