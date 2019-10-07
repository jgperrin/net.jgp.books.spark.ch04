The examples in this repository are support to the **[Spark in Action, 2nd edition](http://jgp.net/sia)** book by Jean Georges Perrin and published by Manning. Find out more about the book on [Manning's website](http://jgp.net/sia).

# Spark in Action, 2nd edition - chapter 4

Welcome to Spark with Java, chapter 4. This chapter is about laziness.

The dataset used in this chapter is NCHS' teen birth rates for age group 15 to 19 in the United States, divided by County, from 2003 to 2015.

## Running PySpark

    1. Clone this project
    Assume that cloned this project to ${MY_HOME_DIR}

    2. cd ${MY_HOME_DIR}/src/main/python

    3. Execute the following spark-submit command to create a jar file to our this application
    ```
    spark-submit net/jgp/books/spark/ch04/lab200_transformation_and_action/transformationActionApp.py
    ```

## Running Scala

    1. Clone this project
       Assume that cloned this project to ${MY_HOME_DIR}

    2. cd ${MY_HOME_DIR}

    3. Create application jar file
       ```mvn clean package```

    4. Execute the following spark-submit command to create a jar file to our this application
    ```
    spark-submit --class net.jgp.books.spark.ch04.lab200_transformation_and_action.Transformation_ActionApp target/spark-chapter04-1.0.0-SNAPSHOT.jar
    ```

Notes:
 1. Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10.

---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://www.facebook.com/SparkWithJava/) or in [Manning's community site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).
