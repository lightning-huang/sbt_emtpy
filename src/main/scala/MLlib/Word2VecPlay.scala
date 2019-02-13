package MLlib
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.Word2Vec
object Word2VecPlay {
  def main(args: Array[String]): Unit = {
    // http://dblab.xmu.edu.cn/blog/1450-2/
    // 设置运行环境
    System.setProperty("hadoop.home.dir", "C:\\winutil")
    val conf = new SparkConf().setAppName("Kmeans").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._
    val documentDF = spark.createDataFrame(Seq("Hi I heard about Spark".split(" "), "I wish Java could use case classes".split(" "), "Logistic regression models are neat".split(" ")).map(Tuple1.apply)).toDF("text")
    val word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(3).setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)
  }
}
