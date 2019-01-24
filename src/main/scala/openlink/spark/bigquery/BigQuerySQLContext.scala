package openlink.spark.bigquery

import com.databricks.spark.avro.SchemaConverters
import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import com.google.cloud.hadoop.io.bigquery._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Easier class to expose to python and java
  * @param sqlContext SQLContext
  */
class BigQuerySQLContext(sqlContext: SQLContext) {

  val sc: SparkContext = sqlContext.sparkContext
  val conf: Configuration = new Configuration(sc.hadoopConfiguration)
  lazy val bq: BigQueryClient = BigQueryClient.getInstance(conf)

  // Register GCS implementation
  if (conf.get("fs.gs.impl") == null) {
    conf.set("fs.gs.impl", classOf[GoogleHadoopFileSystem].getName)
  }

  /**
    * Set GCP project ID for BigQuery.
    */
  def setBigQueryProjectId(projectId: String): Unit = {
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)

    // Also set project ID for GCS connector
    if (conf.get("fs.gs.project.id") == null) {
      conf.set("fs.gs.project.id", projectId)
    }
  }

  /**
    * Set GCS bucket for temporary BigQuery files.
    */
  def setBigQueryGcsBucket(gcsBucket: String): Unit =
    conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, gcsBucket)

  /**
    * Set BigQuery dataset location, e.g. US, EU.
    */
  def setBigQueryDatasetLocation(location: String): Unit =
    conf.set(BigQueryClient.STAGING_DATASET_LOCATION, location)

  /**
    * Set GCP JSON key file.
    */
  def setGcpJsonKeyFile(jsonKeyFile: String): Unit = {
    conf.set("mapred.bq.auth.service.account.json.keyfile", jsonKeyFile)
    conf.set("fs.gs.auth.service.account.json.keyfile", jsonKeyFile)
  }

  /**
    * Set GCP pk12 key file.
    */
  def setGcpPk12KeyFile(pk12KeyFile: String): Unit = {
    conf.set("google.cloud.auth.service.account.keyfile", pk12KeyFile)
    conf.set("mapred.bq.auth.service.account.keyfile", pk12KeyFile)
    conf.set("fs.gs.auth.service.account.keyfile", pk12KeyFile)
  }

  /**
    * Perform a BigQuery SELECT query and load results as a [[DataFrame]].
    * @param sqlQuery SQL query in SQL-2011 dialect.
    */
  def bigQuerySelect(sqlQuery: String): DataFrame = bigQueryTable(bq.query(sqlQuery))

  /**
    * Load a BigQuery table as a [[DataFrame]].
    */
  def bigQueryTable(tableRef: TableReference): DataFrame = {
    conf.setClass(
      AbstractBigQueryInputFormat.INPUT_FORMAT_CLASS_KEY,
      classOf[AvroBigQueryInputFormat], classOf[InputFormat[LongWritable, GenericData.Record]])

    BigQueryConfiguration.configureBigQueryInput(
      conf, tableRef.getProjectId, tableRef.getDatasetId, tableRef.getTableId)

    val fClass = classOf[AvroBigQueryInputFormat]
    val kClass = classOf[LongWritable]
    val vClass = classOf[GenericData.Record]
    val rdd = sc
      .newAPIHadoopRDD(conf, fClass, kClass, vClass)
      .map(_._2)
    val schemaString = rdd.map(_.getSchema.toString).first()
    val schema = new Schema.Parser().parse(schemaString)

    val structType = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
    val converter = SchemaConverters.createConverterToSQL(schema, structType)
      .asInstanceOf[GenericData.Record => Row]
    sqlContext.createDataFrame(rdd.map(converter), structType)
  }

  /**
    * Load a BigQuery table as a [[DataFrame]].
    */
  def bigQueryTable(tableSpec: String): DataFrame = bigQueryTable(BigQueryStrings.parseTableReference(tableSpec))

  /**
    * SQL query to perform on bigquery.
    */
  def bigQueryExec(sqlDmlCommand: String): Unit = bq.execDml(sqlDmlCommand)
}