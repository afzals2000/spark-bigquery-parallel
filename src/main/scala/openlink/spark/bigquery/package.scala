package openlink.spark

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import scala.language.implicitConversions

package object bigquery {

  /**
    * Enhanced version of [[SQLContext]] with BigQuery support.
    *
    * @param sql Spark SQLContext
    * @return A SQLContext which represents the BiqQuerySQLContext
    */
  implicit def makebigQueryContext(sql: SQLContext): BigQuerySQLContext = new BigQuerySQLContext(sql)

  /**
    * Enhanced version of [[SQLContext]] with BigQuery support.
    *
    * @param sql Spark SQLContext
    * @return A SparkSession which represents the BQDataSource
    */
  implicit def makebigQueryDataSource(sql: SQLContext, bucket :String): BQDataSource = new BQDataSource(sql, bucket)

  /**
    * Enhanced version of [[org.apache.spark.sql.DataFrame]] with BigQuery support.
    *
    * @param df Spark dataframe
    * @return A dataframe which represents the BigQuery dataframe
    */
  implicit def makebigQueryDataFrame(df: DataFrame): BigQueryDataFrame = new BigQueryDataFrame(df)

}
