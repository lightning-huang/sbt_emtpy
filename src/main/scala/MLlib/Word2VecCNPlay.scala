package MLlib

import basic.AsciiUtil
import com.hankcs.hanlp.tokenizer.NLPTokenizer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object Word2VecCNPlay {
  def segment(sc:SparkContext): Unit = {
    //stop words
    val stopWordPath = "stopWords"
    val bcStopWords = sc.broadcast(sc.textFile(stopWordPath).collect().toSet)

    //content segment
    val inPath = "d:\\data\\news_shard.txt"
    val segmentRes = sc.textFile(inPath)
      .map(AsciiUtil.sbc2dbcCase) //全角转半角
      .mapPartitions(it =>{
      it.map(ct => {
        try {
          val nlpList = NLPTokenizer.segment(ct)
          import scala.collection.JavaConverters._
          nlpList.asScala.map(term => term.word)
            .filter(!bcStopWords.value.contains(_))
            .mkString(" ")
        } catch {
          case e: NullPointerException => println(ct);""
        }
      })
    })

    //save segment result
    segmentRes.saveAsTextFile("d:\\data\\cut_txt" )
    bcStopWords.unpersist()
  }

  def word2VecRun(sc:SparkContext) = {
    val spark = new SQLContext(sc)
    import spark.implicits._
    case class Text(text: Seq[String])
    val input = spark.read.text("分词结果路径").map(row => row.getString(0).split(" ").toSeq).map(Tuple1.apply).toDF(colNames="text")


    //model train
    val word2vec = new Word2Vec()
      .setInputCol("text")
      .setVectorSize(50)
      .setNumPartitions(64)

    val model = word2vec.fit(input)
    println("model word size: " + model.getVectors.count())

    //Save and load model
    model.save("word2vec模型路径")
    val local = model.getVectors.map{
      row => Seq(row.getString(0), row.getAs[Vector[Double]](1).mkString(" ")).mkString(":")
    }.collect()
    sc.parallelize(local).saveAsTextFile("word2vec词向量路径")

    //predict similar words
    val like = model.findSynonyms("中国", 40)
//    for ((item, cos) <- like) {
//      println(s"$item  $cos")
//    }

    //val sameModel = Word2VecModel.load(sc, "word2vec模型路径")
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\winutil")
    val conf = new SparkConf().setAppName("Word2VecCN").setMaster("local[4]")
    val sc = new SparkContext(conf)
    segment(sc)
    //val spark = new SQLContext(sc)
    //import spark.implicits._




  }
}
