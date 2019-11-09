"""
* Transformation and action.
*
* @author rambabu.posa
"""
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (lit,col,concat,expr)
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

mode=""
t0 = int(round(time.time() * 1000))

# Step 1 - Creates a session on a local master
spark = SparkSession.builder.appName("Analysing Catalyst's behavior") \
    .master("local[*]").getOrCreate()

t1 = int(round(time.time() * 1000))

print("1. Creating a session ........... {}".format(t1 - t0))

# Step 2 - Reads a CSV file with header, stores it in a dataframe
df = spark.read.csv(header=True, inferSchema=True,path=absolute_file_path)

initalDf = df
t2 = int(round(time.time() * 1000))
print("2. Loading initial dataset ...... {}".format(t2 - t1))

# Step 3 - Build a bigger dataset
for x in range(60):
    df = df.union(initalDf)

t3 = int(round(time.time() * 1000))
print("3. Building full dataset ........ {}".format(t3 - t2))

# Step 4 - Cleanup. preparation
df = df.withColumnRenamed("Lower Confidence Limit", "lcl") \
       .withColumnRenamed("Upper Confidence Limit", "ucl")

t4 = int(round(time.time() * 1000))
print("4. Clean-up ..................... {}".format(t4 - t3))

# Step 5 - Transformation
if mode.lower != "noop":
    df =  df.withColumn("avg", expr("(lcl+ucl)/2")) \
            .withColumn("lcl2", col("lcl")) \
            .withColumn("ucl2", col("ucl"))
    if mode.lower == "full":
        df = df.drop("avg","lcl2","ucl2")


t5 = int(round(time.time() * 1000))
print("5. Transformations  ............. {}".format(t5 - t4))

# Step 6 - Action
df.collect()
t6 = int(round(time.time() * 1000))
print("6. Final action ................. {}".format(t6 - t5))

print("")
print("# of records .................... {}".format(df.count))
