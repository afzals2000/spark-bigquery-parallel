name := "spark-bigquery-parallel"
organization := "openlink"
scalaVersion := "2.11.11" 

spName := "openlink/spark-bigquery"
sparkVersion := "2.2.0"
sparkComponents := Seq("core", "sql")
spAppendScalaVersion := true
spIncludeMaven := true

libraryDependencies ++= Seq(
  "com.databricks" %% "spark-avro" % "4.0.0",
  "com.google.cloud.bigdataoss" % "bigquery-connector" % "0.10.4-hadoop2" exclude ("com.google.guava", "guava-jdk5"),
  "org.slf4j" % "slf4j-simple" % "1.7.21",
  "joda-time" % "joda-time" % "2.9.3",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("com", "databricks", "spark", "avro", xs @ _*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports")
