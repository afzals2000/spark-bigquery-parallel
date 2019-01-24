package openlink.spark.bigquery

import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.collection.JavaConverters._

object WriteMode extends Enumeration {
  val TRUNCATE, APPEND, UPDATE = Value
}

class BQDataSource(sql :SQLContext, bucket :String) {
  import WriteMode._
  val BUCKET_NAME = bucket
  val LOCATION = "EU"
  val bqpoolname ="spark-bigquery-pool"
  val timeoutInMins = 60
  def getBQContext(sqlContext: SQLContext): BigQuerySQLContext = {
    val bqSqlContext: BigQuerySQLContext = new BigQuerySQLContext(sqlContext)
    bqSqlContext.setBigQueryGcsBucket(BUCKET_NAME)
    bqSqlContext.setBigQueryDatasetLocation(LOCATION)
    bqSqlContext
  }

  def read(source: String): DataFrame = {
    val bqSqlContext = getBQContext(sql)
    bqSqlContext.bigQuerySelect(source)
  }

  def readParallel(sqlList: java.util.ArrayList[String]): java.util.ArrayList[DataFrame] = {
    val sqls = sqlList.asScala
    val ffs = Future.sequence(sqls.map(query => Future(read(query))))
    val dfs = Await.result(ffs, timeoutInMins.minutes)
    val dfsAsArrayList = new java.util.ArrayList[DataFrame](dfs.asJava)
    dfsAsArrayList
  }

  def write(dataFrame: DataFrame, tableName: String, writeMode: String): Unit = {
    import openlink.spark.bigquery.WriteDisposition._
//    val (modeStr, bqMode) :(String, Value) = ("append", WRITE_APPEND)
    val (modeStr, bqMode): (String, Value) = writeMode match {
      case "append" => ("append", WRITE_APPEND)
      case "truncate" => {
        val deleteQuery = truncateTable(tableName)
        val bqSqlContext = getBQContext(sql)
        bqSqlContext.bigQueryExec(deleteQuery)
        ("append", WRITE_APPEND)
      }
    }
    println(Calendar.getInstance().getTime().toString() + s" started writing to table $tableName with $modeStr !!! ")
    dataFrame.saveAsBigQueryTable(tableName, bqMode)
    println(Calendar.getInstance().getTime().toString() + s" finished writing to table $tableName with $modeStr !!! ")
  }

  def writeParallel(tableMap: java.util.HashMap[String, DataFrame]): Unit = {
    val outputTablesMap = tableMap.asScala
    sql.sparkContext.setLocalProperty("spark.scheduler.pool", bqpoolname)

    val ffs = Future.sequence(outputTablesMap.map { case (table, df) => Future(write(df, table, "append")) } )
    // must wait till the max timeout until all futures are completed
    // the job will fail fast for any missing object, syntax error etc.
    Await.result(ffs, timeoutInMins.minutes)
  }

  def truncateTable(tableName: String): String = {
    val truncateQuery = s"""DELETE FROM $tableName WHERE 1=1"""
    println(s"Truncate query for table $tableName", truncateQuery)
    truncateQuery
  }

}
