spark-bigquery-parallel
=======================

Google BigQuery support for Spark, SQL, and DataFrames. BigQuery Standard SQL is supported with default Staging BigQuery dataset region set to EU.  

 
## Usage:

In order to use this library in other projects which will make use of this library via SBT you will need to do the following:
```
1. Clone this repository to your local machine: git clone https://github.com/afzals2000/spark-bigquery-parallel
2. Navigate to the directory where you have stored the code
3. Issue the following command: sbt followed by enter
4. Issue the following command: publishLocal followed by enter   
``` 

The above will compile and publish the library into your local SBT cache on your local machine. In projects which require the use of this library, simply include the appropriate config line
in your build.sbt:


#### From Scala

```
// SparkSession
implicit val spark = SparkSession.builder().appName("spark-app").getOrCreate()

// Build List of SQL
val listOfSQL: List[String] = List("SELECT word, word_count FROM table1"
                                  ,"SELECT word, word_count FROM table2"
                                  ,"SELECT word, word_count FROM table3"
                                )

// Load SQL results as lists of Dataframe from BigQuery in parallel
val dataframeList = readParallel(listOfSQL)

//Build Dataframe to table map to be saved
val mapOfDataframeAndTables = Map{Dataframe1 -> "table1", Dataframe1 -> "table1"}

// Save data to a table in parallel
writeParallel(mapOfDataframeAndTables)
```

If you'd like to write nested records to BigQuery, be sure to specify an Avro Namespace.
BigQuery is unable to load Avro Namespaces with a leading dot (`.nestedColumn`) on nested records.


#### From Pyspark
```
    # List of SQL
    sql_list = [
           """SELECT col1, col2 FROM table"""
          ,"""SELECT col1, col2 FROM table1 t1, table2 t2 inner join t1.col1 = t2.col2"""
          ,"""SELECT col1, col2 FROM table3"""
          ,"""SELECT col1, col2 FROM table4"""
        ]

    # get spark and bq handle
    spark = SparkSession.builder.appName(jobName).getOrCreate()
    bqSource = spark._sc._jvm.uk.sky.spark.bigquery
    bq = bqSource.BQDataSource(spark._wrapped._jsqlContext, tmp_bucket_name)
    
    # get java dataframe for sql in parallel
    java_dataframe_list = bq.readParallel(sql_list)

    # convert java object to pyspark dataframe
    spark_dataframe_list = [DataFrame(df,spark) for df in java_dataframe_list]

```

Run Pyspark job on dataproc cluster
```
gcloud dataproc jobs submit pyspark \
    --project {project} --cluster {cluster} \
    --jars gs://path/to/spark-bigquery-parallel/jar/file/spark-bigquery-parallel-assembly-0.2.2.jar \
    --py-files gs://path/to/pyspark/file.py, gs://path/to/other/dependant/pyspark_file.py -- --d "demo_dataset"
```

Support for complex type like Array and Struct is not tested.


```
"openlink" %% "spark-bigquery-parallel" % "VERSION" // VERSION is dependant on the current version of the library which can be found in the version.sbt file 
```