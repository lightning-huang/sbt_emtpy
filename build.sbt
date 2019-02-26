name := "train_model"

version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.1.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion,
  "com.hankcs" % "hanlp" % "portable-1.2.8",
  "org.scalanlp" % "nak_2.11" % "1.3"
)