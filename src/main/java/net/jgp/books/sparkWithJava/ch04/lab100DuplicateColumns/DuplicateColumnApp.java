package net.jgp.books.sparkWithJava.ch04.lab100DuplicateColumns;

import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * CSV ingestion in a dataframe.
 * 
 * @author jgp
 */
public class DuplicateColumnApp {

  enum Mode {
    NOOP, CREATE_COLS, FULL
  }

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    DuplicateColumnApp app = new DuplicateColumnApp();
    app.start(Mode.NOOP);
    app.start(Mode.CREATE_COLS);
    app.start(Mode.FULL);
  }

  /**
   * The processing code.
   */
  private void start(Mode mode) {

    System.out.println("### Mode ... " + mode);

    long t0 = System.currentTimeMillis();

    // Step 1 - Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Analysing catalyst")
        .master("local")
        .config("spark.driver.maxResultSize", "4g")
        .getOrCreate();
    long t1 = System.currentTimeMillis();
    System.out.println("1. Creating a session ................. " + (t1 - t0));

    // Step 2 - Reads a CSV file with header, stores it in a dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .load(
            "data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv");
    long t2 = System.currentTimeMillis();
    System.out.println("2. Loading initial dataset ............ " + (t2 - t1));

    // Step 3 - Build a bigger dataset
    for (int i = 0; i < 7; i++) {
      df = df.union(df);
    }
    long t3 = System.currentTimeMillis();
    System.out.println("3. Building full dataset .............. " + (t3 - t2));

    // Step 4- Cleanup. preparation
    df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
    df = df.withColumnRenamed("Upper Confidence Limit", "ucl");
    long t4 = System.currentTimeMillis();
    System.out.println("4. Clean-up ........................... " + (t4 - t3));

    if (mode != Mode.NOOP) {
      df = df
          .withColumn("avg", expr("(lcl+ucl)/2"))
          .withColumn("lcl2", df.col("lcl"))
          .withColumn("ucl2", df.col("ucl"));
      if (mode == Mode.FULL) {
        df = df
            .drop(df.col("avg"))
            .drop(df.col("lcl2"))
            .drop(df.col("ucl2"));
      }
    }

    long t5 = System.currentTimeMillis();
    System.out.println("5. Transformations  ................... " + (t5 - t4));
    df.collect();
    long t6 = System.currentTimeMillis();
    System.out.println("6. Final action ....................... " + (t6 - t5));

    System.out.println("");
    System.out.println("# of records ....................... " + df.count());
  }
}
