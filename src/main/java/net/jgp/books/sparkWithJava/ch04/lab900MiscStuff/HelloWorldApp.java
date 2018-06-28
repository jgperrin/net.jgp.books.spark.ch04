package net.jgp.books.sparkWithJava.ch04.lab900MiscStuff;

/**
 * Hello world.
 * 
 * @author jgp
 */
public class HelloWorldApp {

  /**
   * main() is your entry point to the application, let's display 50,000 hello
   * world: it takes about the same time as creating 7.5m datapoints using
   * Spark, just for the fun of it...
   * 
   * @param args
   */
  public static void main(String[] args) {
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < 50000; i++) {
      System.out.println("Hello, world");
    }
    long t1 = System.currentTimeMillis();
    System.out.println("1. Hello world ... " + (t1 - t0));
  }
}
