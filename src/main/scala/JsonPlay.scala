import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import math._
import scala.util.Random
object JsonPlay {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\winutil")
    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val sqlContext = new SQLContext(spark)
    import sqlContext.implicits._

    sqlContext.read.json("abc.json")
  }
}
