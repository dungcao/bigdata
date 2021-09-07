import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

//./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-20_2.12:7.13.1 
// xxx.jar  kafka_host:9092 kafka_topic 
// es_host 9200

object LPR {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("LPR")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    if(args.length < 2){
      println()
      println("=================== MISSING ARG LIST ================================")
      println("ARGS: <kafka-conn-str> <topic> <es_host> <es_port>")
      println()
      sys.exit()
    }

    val kafka_conn_str = args(0)
    val kafka_topic = args(1)
//    val es_host = args(2)
//    val es_port = args(3)

    val df = spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", kafka_conn_str) 
              .option("subscribe", kafka_topic)   
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
      .groupBy(
        $"license").agg(
              first("messageid").as('msgid'),
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
//    data_to_es.writeStream
//      .foreachBatch{(batchDF: DataFrame, batchId: Long) =>
//      batchDF.write.format("org.elasticsearch.spark.sql")
//        .option(ConfigurationOptions.ES_NODES, es_host)
//        .option(ConfigurationOptions.ES_PORT, es_port)
//        .mode("append")
//        .save("optymizing/docs")
//    }.outputMode("update")
//      .start().awaitTermination()

//    ========== TO CONSOLE ============
    data_to_es.writeStream
        .format("console")
        .outputMode("update")
        .start().awaitTermination()

//    =========== TO HDFS ==========
//    data.writeStream
//      .format("parquet")
//      .outputMode("append")
//      .option("checkpointLocation", hdfs_checkpoint_dir) //"/Users/Storage/checkpoint")
//      .option("path", hdfs_storage_dir)  //"hdfs://localhost:9000/dungcao/lpr")
//      .start().awaitTermination()
  }
}
