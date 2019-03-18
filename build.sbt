name := "geo-search-spark"

organization := "com.whiletruecurious"

version := "1.0"

scalaVersion := "2.10.5"

//val sparkVersion = "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
