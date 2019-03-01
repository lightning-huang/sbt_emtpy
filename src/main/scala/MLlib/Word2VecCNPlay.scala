package MLlib

import java.util.Properties

import basic.{AsciiUtil, OfCourseUtil}
import com.hankcs.hanlp.tokenizer.NLPTokenizer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.functions._
import parallax.ParallaxIniReader

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
    segmentRes.saveAsTextFile(segmentResFolder)
    bcStopWords.unpersist()
  }

  case class Text(text: Seq[String])

  def word2VecRun(spark: SQLContext, segmentResFolder: String, modelDir: String, word2VecDir: String) = {
    import spark.implicits._

    val input = spark.read.text(segmentResFolder + "/part-*").map({case Row(value:String) => value})
      .filter(line => !OfCourseUtil.isNullOrWhiteSpace(line)).map(line => Text(line.split(" ").toSeq)).toDF()

    //model train
    val word2vec = new Word2Vec()
      .setInputCol("text")
      .setVectorSize(50)
      .setNumPartitions(128)

    val model = word2vec.fit(input)
    println("model word size: " + model.getVectors.count())

    //Save and load model
    model.save(modelDir)
    model.getVectors.map {
      row => Seq(row.getString(0), row.getAs[DenseVector](1).toArray.mkString(" ")).mkString(":")
    }.write.text(word2VecDir)

    //predict similar words
    val like = model.findSynonyms("中国", 40)
    like.map {
      row => s"${row.getAs[String](0)} ${row.getAs[Double](1)}"
    }.show()

    val sameModel = Word2VecModel.load(modelDir)
  }

  def main(args: Array[String]): Unit = {
    ParallaxIniReader.init();
    val props = new Properties()
    props.load(this.getClass.getClassLoader.getResourceAsStream("hanlp.properties"))
    println("the read root=" + props.getProperty("root"))
    Class.forName("com.hankcs.hanlp.HanLP")
    val configuration = ParallaxIniReader.DataPathConfiguration;
    val isLocalRun = configuration.getBoolean("localRun")
    if (isLocalRun) {
      System.setProperty("hadoop.home.dir", "d:\\winutil")
    }

    val stopWords = configuration.getProperty("stopWords")
    val corpus = configuration.getProperty("corpusPath")
    val segmentResDir = configuration.getProperty("segmentResDir")
    val modelDir = configuration.getProperty("modelDir")
    val word2vecDir = configuration.getProperty("word2vecDir")

    var conf = new SparkConf().setAppName("Word2VecCN")

    if (isLocalRun) {
      conf = conf.setMaster("local[4]")
    }

    val spark = SparkSession.builder.config(conf).getOrCreate();
    val sc = spark.sparkContext;
    val sqlContext = spark.sqlContext;
    segment(sc, stopWords, corpus, segmentResDir)
    word2VecRun(sqlContext, segmentResDir, modelDir, word2vecDir);

  }
}
