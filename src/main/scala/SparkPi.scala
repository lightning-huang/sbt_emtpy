
import org.apache.spark.{SparkConf, SparkContext}
import math._
import scala.util.Random
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length>0) args(0).toInt else 2
    val n = 100000 * slices

    val count = spark.parallelize(1 to n, slices).map{ i =>

      val x = random * 2 -1
      val y = random * 2 - 1
      if (x*x + y*y <1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly %f".format(4.0 * count / n))
    spark.stop()

  }
}
