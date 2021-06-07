The examples in this repository are support to the **[Spark in Action, 2nd edition](http://jgp.net/sia)** book by Jean-Georges Perrin and published by Manning. Find out more about the book on [Manning's website](http://jgp.ai/sia).

# Spark in Action, 2nd edition - chapter 4

Welcome to Spark in Action, 2nd edition, chapter 4. This chapter is about laziness.

The dataset used in this chapter is NCHS' teen birth rates for age group 15 to 19 in the United States, divided by County, from 2003 to 2015.

This code is designed to work with Apache Spark v3.1.2.

## Labs

Each chapter has one or more labs. Labs are examples used for teaching in the [book](https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp). You are encouraged to take ownership of the code and modify it, experiment with it, hence the use of the term **lab**.

### Lab \#200

The `TransformationAndActionApp` application does the following:

1.	It acquires a session (a `SparkSession`).
2.	It asks Spark to load (ingest) a dataset in CSV format.
3.	It demonstrates Spark's Transformation and action API.

### Lab \#500

The `TransformationExplainApp` builds on lab \#200 and shows the use of the dataframe `explain()` method, which is the same as the `EXPLAIN` keyword in Spark SQL. All Spark v3 explain options are used in the code: simple, extended, codegen, cost, and formatted.

## Running the labs

### Java

For information on running the Java lab, see chapter 4 in [Spark in Action, 2nd edition](http://jgp.net/sia).

### PySpark

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer [Appendix K: Installing Spark in production and a few tips](https://livebook.manning.com/book/spark-in-action-second-edition/appendix-k/)). 

Step by step direction for lab \#200. You will need to adapt some steps for the other labs.

1. Clone this project

    git clone https://github.com/jgperrin/net.jgp.books.spark.ch04

2. Go to the lab in the Python directory

    cd net.jgp.books.spark.ch04/src/main/python/lab200_transformation_and_action/

3. Execute the following spark-submit command to create a jar file to our this application

    spark-submit transformationActionApp.py

### Scala

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer [Appendix K: Installing Spark in production and a few tips](https://livebook.manning.com/book/spark-in-action-second-edition/appendix-k/)). 

Step by step direction for lab \#200. You will need to adapt some steps for the other labs.

1. Clone this project

    git clone https://github.com/jgperrin/net.jgp.books.spark.ch04

2. Go to the lab directory

    cd net.jgp.books.spark.ch04

3. Package application using the `sbt` command

    sbt clean assembly

4. Run Spark/Scala application using spark-submit command as shown below:

    spark-submit --class net.jgp.books.spark.ch04.lab200_transformation_and_action.TransformationAndActionScalaApp target/scala-2.12/SparkInAction2-Chapter04-assembly-1.0.0.jar

## News
 1. [2020-06-07] Updated the pom.xml to support Apache Spark v3.1.2. 
 1. [2020-06-07] As we celebrate the first anniversary of Spark in Action, 2nd edition is the best-rated Apache Spark book on [Amazon](https://amzn.to/2TPnmOv). 
 
## Notes
 1. [Java] Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10 (published in April 2019).
 1. [Scala, Python] As of MEAP v14, we have introduced Scala and Python examples (published in October 2019).
 1. With Spark v3.0.0 preview 2, the number of iterations in lab \#200 went from 60 to 10.
 1. The master branch contains the last version of the code running against the latest supported version of Apache Spark. Look in specifics branches for specific versions.
  
---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://www.facebook.com/SparkInAction/) or in [Manning's Live Book site](https://livebook.manning.com/book/spark-in-action-second-edition/about-this-book/?a_aid=jgp).
