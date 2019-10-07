package net.jgp.books.spark.ch04.lab900_misc

/**
  * Hello world.
  *
  * @author rambabu.posa
  */
object Hello_WorldApp {

  /**
    * main() is your entry point to the application, let's display 50,000
    * hello world: it takes about the same time as creating 7.5m datapoints
    * using Spark, just for the fun of it...
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val t0 = System.currentTimeMillis
    for(i <- 0.to(50000)){
      println("Hello, world")
    }
    val t1 = System.currentTimeMillis
    println("1. Hello world ... " + (t1 - t0))
  }

}
