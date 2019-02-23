package MLlib

import java.io.File

import basic.AsciiUtil
import com.hankcs.hanlp.tokenizer.NLPTokenizer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import parallex.ParellexIniReader

object Word2VecCNPlay {

  def segment(sc: SparkContext, stopWordsPath: String, corpus: String, segmentResFolder: String): Unit = {
    //stop words
    val bcStopWords = sc.broadcast(sc.textFile(stopWordsPath).collect().toSet)

    //content segment
    val inPath = corpus
    val segmentRes = sc.textFile(inPath)
      .map(AsciiUtil.sbc2dbcCase) //全角转半角
      .mapPartitions(it => {
      it.map(ct => {
        try {
          val nlpList = NLPTokenizer.segment(ct)
          import scala.collection.JavaConverters._
          nlpList.asScala.map(term => term.word)
            .filter(!bcStopWords.value.contains(_))
            .mkString(" ")
        } catch {
          case e: NullPointerException => println(ct); ""
        }
      })
    })

    //save segment result
    segmentRes.saveAsTextFile("d:\\data\\cut_txt")
    bcStopWords.unpersist()
  }

  case class Text(text: Seq[String])

  def word2VecRun(sc: SparkContext) = {
    val spark = new SQLContext(sc)
    import spark.implicits._

    val input = spark.read.text("d:\\data\\cut_txt\\part-00000", "d:\\data\\cut_txt\\part-00001").filter($"value".isNotNull && length(trim($"value")) > 0).map(row => Text(row.getAs[String]("value").split(" ").toSeq)).toDF()

    //model train
    val word2vec = new Word2Vec()
      .setInputCol("text")
      .setVectorSize(50)
      .setNumPartitions(64)

    val model = word2vec.fit(input)
    println("model word size: " + model.getVectors.count())

    //Save and load model
    model.save("d:\\data\\my_model")
    val local = model.getVectors.map {
      row => Seq(row.getString(0), row.getAs[DenseVector](1).toArray.mkString(" ")).mkString(":")
    }.collect()
    sc.parallelize(local).saveAsTextFile("d:\\data\\word2vec")

    //predict similar words
    //val like = model.findSynonyms("中国", 40)
    //    for ((item, cos) <- like) {
    //      println(s"$item  $cos")
    //    }

    //val sameModel = Word2VecModel.load(sc, "word2vec模型路径")
  }

  def main(args: Array[String]): Unit = {
    ParellexIniReader.init();
    val configuration = ParellexIniReader.DataPathConfiguration;
    val isLocalRun = configuration.getBoolean("localRun")
    if (isLocalRun) {
      System.setProperty("hadoop.home.dir", "d:\\winutil")
    }

    val stopWords = configuration.getProperty("stopWords")
    val corpus = configuration.getProperty("corpusPath")
    val segmentResDir = configuration.getProperty("segmentResDir")
    val modelDir = configuration.getProperty("modelDir")
    var conf = new SparkConf().setAppName("Word2VecCN")

    if(isLocalRun){
      conf = conf.setMaster("local[4]");
    }

    val sc = new SparkContext(conf)
    segment(sc, stopWords, corpus, segmentResDir)

  }
}
