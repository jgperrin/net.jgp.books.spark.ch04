package net.jgp.books.sparkWithJava.ch04.lab200TransformationExplain;

import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Transformation explained.
 * 
 * @author jgp
 */
public class TransformationExplainApp {

    /**
     * main() is your entry point to the application.
     * 
     * @param args
     */
    public static void main(String[] args) {
        TransformationExplainApp app = new TransformationExplainApp();
        String mode = "noop";
        if (args.length != 0) {
            mode = args[0];
        }
        app.start(mode);
    }

    /**
     * The processing code.
     */
    private void start(String mode) {

        // Step 1 - Creates a session on a local master
        SparkSession spark = SparkSession.builder().appName("Analysing Catalyst's behavior").master("local")
                .getOrCreate();

        // Step 2 - Reads a CSV file with header, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv").option("header", "true")
                .load("data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv");
        Dataset<Row> df0 = df;

        // Step 3 - Build a bigger dataset
        df = df.union(df0);

        // Step 4 - Cleanup. preparation
        df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
        df = df.withColumnRenamed("Upper Confidence Limit", "ucl");

        // Step 5 - Transformation
        df = df.withColumn("avg", expr("(lcl+ucl)/2")).withColumn("lcl2", df.col("lcl")).withColumn("ucl2",
                df.col("ucl"));

        // Step 6 - Action
        df.explain();

    }
}
