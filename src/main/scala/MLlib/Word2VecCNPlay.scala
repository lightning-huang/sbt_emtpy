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
    val stopWordPath = "停用词路径"
    val bcStopWords = sc.broadcast(sc.textFile(stopWordPath).collect().toSet)

    //content segment
    val inPath = "训练语料路径"
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
    segmentRes.saveAsTextFile("分词结果路径")
    bcStopWords.unpersist()
  }

  def word2VecRun(sc:SparkContext) = {
    val spark = new SQLContext(sc)
    val input = sc.textFile("分词结果路径").map(line => line.split(" ").toSeq)
    val inputDF = spark.createDataFrame(input, Seq[String].getClass)

    //model train
    val word2vec = new Word2Vec()
      .setVectorSize(50)
      .setNumPartitions(64)

    val model = word2vec.fit(inputDF)
    println("model word size: " + model.getVectors.size)

    //Save and load model
    model.save(sc, "word2vec模型路径")
    val local = model.getVectors.map{
      case (word, vector) => Seq(word, vector.mkString(" ")).mkString(":")
    }.toArray
    sc.parallelize(local).saveAsTextFile("word2vec词向量路径")

    //predict similar words
    val like = model.findSynonyms("中国", 40)
    for ((item, cos) <- like) {
      println(s"$item  $cos")
    }

    //val sameModel = Word2VecModel.load(sc, "word2vec模型路径")
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\winutil")
    val conf = new SparkConf().setAppName("Word2VecCN").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._




  }
}
