
name := "CapNLP"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.3.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.2.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-yarn_2.10" % "1.3.0" % "provided"

libraryDependencies += "org.apache.lucene" % "lucene-analyzers-common" % "5.0.0"

libraryDependencies += "com.hankcs" % "hanlp" % "portable-1.1.5"
