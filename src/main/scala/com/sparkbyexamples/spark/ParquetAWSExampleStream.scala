package com.sparkbyexamples.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object ParquetAWSExampleStream extends App{



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
     .config("fs.s3a.committer.name", "partitioned")
      .config("fs.s3a.committer.staging.conflict-mode", "replace")
      .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
      .config("spark.sql.parquet.output.committer.class",
          "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter")
      .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
  //.config("parquet.read.support.class", "parquet.avro.AvroReadSupport")

    .getOrCreate();



  /*val peopleDF = spark.read.parquet("/home/prakash/MinIO/GIT/trial-projects/SparkJobs/RCPExamples/data/userdata2.parquet")
  peopleDF.write.mode(SaveMode.Overwrite).partitionBy("gender").parquet("s3a://test-bucket/prefix-1/userdata2.parquet")
*/


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
      .option("maxFilesPerTrigger", 1)
      //.option("recursiveFileLookup", "false")
      //.option("basePath", "s3a://test-bucket/prefix-1/")
      //.option("recursiveFileLookup", false)
      //.option("pathGlobFilterstr", false)
      //.option("mergeSchema", "false")
      .schema(schema)
      .option("basePath", "s3a://test-bucket/prefix-1/")
      .parquet("s3a://test-bucket/prefix-1/userdata2.parquet")


    val wstream= sdf.writeStream
      .format("console")
     // .outputMode("complete")
      .start()
      .awaitTermination()


  spark.stop()
  spark.close()

}
