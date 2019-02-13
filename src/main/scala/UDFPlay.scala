import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

object UDFPlay {
  def main(args: Array[String]): Unit = {
    oldUdf
    oldDfUdf
  }

  def isAdult(age: Int) = {
    if (age < 18) {
      false
    } else {
      true
    }
  }

  def oldUdf() {
    //spark 初始化
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("oldUdf")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // 构造测试数据，有两个字段、名字和年龄
    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
    //创建测试df
    val userDF = sc.parallelize(userData).toDF("name", "age")
    // 注册一张user表
    userDF.registerTempTable("user")

    // 注册自定义函数（通过匿名函数）
    sqlContext.udf.register("strLen", (str: String) => str.length())

    sqlContext.udf.register("isAdult", isAdult _)
    // 使用自定义函数
    sqlContext.sql("select *,strLen(name)as name_len,isAdult(age) as isAdult from user").show
    //关闭
    sc.stop()
  }

  def oldDfUdf() {
    //spark 初始化
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("oldDfUdf")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // 构造测试数据，有两个字段、名字和年龄
    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
    //创建测试df
    val userDF = sc.parallelize(userData).toDF("name", "age")
    import org.apache.spark.sql.functions._
    //注册自定义函数（通过匿名函数）
    val strLen = udf((str: String) => str.length())
    //注册自定义函数（通过实名函数）
    val udf_isAdult = udf(isAdult _)

    //通过withColumn添加列
    userDF.withColumn("name_len", strLen(col("name"))).withColumn("isAdult", udf_isAdult(col("age"))).show
    //通过select添加列
    userDF.select(col("*"), strLen(col("name")) as "name_len", udf_isAdult(col("age")) as "isAdult").show

    //关闭
    sc.stop()
  }
}
