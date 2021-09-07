import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

//./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-20_2.12:7.13.1 /Users/dungcao/IdeaProjects/Optymizing/out/artifacts/Optymizing_jar/Optymizing.jar  13.212.213.41:9092 test_topic localhost 9200

object LPR {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("LPR")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  def main(args: Array[String]): Unit = {
//    val schema = new StructType()
//      .add("messageid", StringType)
//      .add("mdsversion", StringType)
//      .add("@timestamp", StringType)
//      .add("place", StringType)
//      .add("sensor", new StructType()
//        .add("id", StringType)
//        .add("type", StringType)
//        .add("description", StringType)
//        .add("location", new StructType()
//          .add("lat", DoubleType)
//          .add("lon", DoubleType)
//          .add("alt", DoubleType))
//        .add("coordinate", new StructType()
//          .add("x", DoubleType)
//          .add("y", DoubleType)
//          .add("z", DoubleType)))
//      .add("analyticsModule", StringType)
//      .add("object", new StructType()
//        .add("id", StringType)
//        .add("speed", DoubleType)
//        .add("direction", DoubleType)
//        .add("orientation", DoubleType)
//        .add("vehicle", new StructType()
//          .add("type", StringType)
//          .add("make", StringType)
//          .add("model", StringType)
//          .add("color", StringType)
//          .add("licenseState", StringType)
//          .add("license", StringType)
//          .add("confidence", DoubleType))
//        .add("bbox", new StructType()
  //        .add("topleftx", DoubleType)
  //        .add("toplefty", DoubleType)
  //        .add("bottomrightx", DoubleType)
  //        .add("bottomrighty", DoubleType)))
//      .add("location", new StructType()
//        .add("lat", DoubleType)
//        .add("lon", DoubleType)
//        .add("alt", DoubleType))
//      .add("coordinate", new StructType()
//        .add("x", DoubleType)
//        .add("y", DoubleType)
//        .add("z", DoubleType))
//      .add("event", new StructType()
//        .add("id", StringType)
//        .add("type", StringType))
//      .add("videoPath", StringType)

    if(args.length < 4){
      println()
      println("=================== MISSING ARG LIST ================================")
      println("ARGS: <kafka-conn-str> <topic> <es_host> <es_port>")
      println()
      sys.exit()
    }

    val kafka_conn_str = args(0)
    val kafka_topic = args(1)
    val es_host = args(2)
    val es_port = args(3)

    val df = spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", kafka_conn_str)  //13.212.213.41:9092")
              .option("subscribe", kafka_topic)   //"test_topic")
//              .option("startingOffsets", "earliest")
              .load()

    val data = df.selectExpr("CAST(value AS STRING)")

    val data_to_es = data
      .select(get_json_object(col("value"), "$.messageid").alias("messageid"),
        get_json_object(col("value"), "$.@timestamp").alias("timestamp"),
        get_json_object((col("value")),"$place").alias("place"),
        get_json_object(col("value"), "$.sensor").alias("sensor"),
        get_json_object(col("value"), "$.object.id").alias("track_id"),
        get_json_object(col("value"), "$.object.vehicle.type").alias("type"),
        get_json_object(col("value"), "$.object.vehicle.make").alias("make"),
        get_json_object(col("value"), "$.object.vehicle.model").alias("model"),
        get_json_object(col("value"), "$.object.vehicle.color").alias("color"),
        get_json_object(col("value"), "$.object.vehicle.license").alias("license"),
        get_json_object(col("value"), "$.object.vehicle.confidence").alias("confidence"),
        get_json_object(col("value"), "$.object.bbox").alias("bbox"),
        get_json_object(col("value"), "$.event.type").alias("event"),
        get_json_object(col("value"), "$.videoPath").alias("imagepath"))
      .filter("messageid is not null")
//      .withWatermark("timestamp", "10 minutes")
      .groupBy(
//        window($"timestamp", "10 minutes", "5 minutes"),
        $"license").agg(
//              first("messageid").as('msgid'),
              first("timestamp").as("timestamp"),
              first("place").as("place"),
              first("sensor").as("sensor"),
              first("track_id").as("trackid"),
              collect_set("type").as("type"),
              collect_set("make").as("make"),
              collect_set("color").as("color"),
              avg("confidence").as("confidence"),
              first("bbox").as("bbox"),
              first("event").as("event"),
              first("imagepath").as("filename"))

//    ========= TO ELASTICSEARCH ========
    data_to_es.writeStream
      .foreachBatch{(batchDF: DataFrame, batchId: Long) =>
      batchDF.write.format("org.elasticsearch.spark.sql")
        .option(ConfigurationOptions.ES_NODES, es_host)
        .option(ConfigurationOptions.ES_PORT, es_port)
        .mode("append")
        .save("optymizing/docs")
    }.outputMode("update")
      .start().awaitTermination()

//    ========== TO CONSOLE ============
//    data_to_es.writeStream
//        .format("console")
//        .outputMode("update")
//        .start().awaitTermination()

//    =========== TO HDFS ==========
//    data.writeStream
//      .format("parquet")
//      .outputMode("append")
//      .option("checkpointLocation", hdfs_checkpoint_dir) //"/Users/Storage/checkpoint")
//      .option("path", hdfs_storage_dir)  //"hdfs://localhost:9000/dungcao/lpr")
//      .start().awaitTermination()
  }
}
