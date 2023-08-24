package com.sparkbyexamples.spark

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object ParquetAWSExample extends App{


    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .config("spark.logConf", "true")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:22000/")
      .config("spark.hadoop.fs.s3a.access.key", "minio").config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
      .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
          "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
      .config("fs.s3a.readahead.range", "1048576")
      .config("fs.s3a.user.agent.prefix", "dail-job-aug-24-10-30")

     // .config("fs.s3a.committer.name", "magic")
      .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
      .config("spark.sql.parquet.output.committer.class",
          "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
  //.config("parquet.read.support.class", "parquet.avro.AvroReadSupport")

    .getOrCreate();
   // spark.sparkContext
     // .hadoopConfiguration.set("fs.s3a.path.style.access", "true")


  /*  df.write
      .parquet("s3a://sparkbyexamples/parquet/people.parquet")
*/

   //Non Streaming
     val parqDF = spark.read.option("mergeSchema", "true").parquet("s3a://test-bucket/prefix/userdata1.parquet")
       parqDF.show()
   //Non Streaming



  /* Streaming */

  /**
   * column#		column_name		hive_datatype
   * =====================================================
   * 1		registration_dttm 	timestamp
   * 2		id 			int
   * 3		first_name 		string
   * 4		last_name 		string
   * 5		email 			string
   * 6		gender 			string
   * 7		ip_address 		string
   * 8		cc 			string
   * 9		country 		string
   * 10		birthdate 		string
   * 11		salary 			double
   * 12		title 			string
   * 13		comments 		string
   */
/*

  val schema = StructType(
    List(
      StructField("id", IntegerType, true),
      StructField("first_name", StringType, true),
      StructField("last_name", StringType, true),
      StructField("email", StringType, true),
      StructField("gender", StringType, true),
      StructField("ip_address", StringType, true),
      StructField("country", StringType, true),
      StructField("birthdate", StringType, true),
      StructField("salary", DoubleType, true),
      StructField("title", StringType, true)
    )
  )

  val sdf = spark.readStream
    //.option("recursiveFileLookup", "false")
    //.option("basePath", "/")
    //.option("recursiveFileLookup", false)
    //.option("pathGlobFilterstr", false)
    //.option("mergeSchema", "true")
    .schema(schema)
    //.option("basePath", "s3a://test-bucket/")
    .parquet("s3a://test-bucket/prefix/userdata1.parquet")

  sdf.createOrReplaceTempView("stream_rdf")
  val query = spark.sql("select count(*) from stream_rdf  group by id")
    .writeStream
    .format("console")
    .outputMode("complete")
    .start()
    .awaitTermination()
*/

 /* sdf.createOrReplaceTempView("parquetFile")
  val namesDF = spark.sql("SELECT * FROM parquetFile")
  namesDF.writeStream
    .option("basePath", "/")
    .format("console")
    .option("basePath", "/")
    .option("checkpointLocation", "/tmp/check-point")
    .start()
    .awaitTermination()*/
  //namesDF.map(attributes => "Name: " + attributes(0)).show()
  // +------------+

/*
  sdf.select("id","title","gender").
    writeStream
    .option("basePath", "/")
    .format("console")
    .option("basePath", "/")
    .option("checkpointLocation",  "/tmp/check-point")
    .start()
    .awaitTermination()*/

  // Stop the Spark session
  spark.stop()
  spark.close()

  // parkSQL.show()

/*
  // Create a query to print the contents of the stream
  val query = sdf.writeStream
    .outputMode(OutputMode.Append()) // Choose the appropriate output mode
    .format("console") // Print to the console
    .start()

  // Start the query and wait for termination
  query.awaitTermination()*/


  // Process the stream// Process the stream

  /* Streaming */


}
