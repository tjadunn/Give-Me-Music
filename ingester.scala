// Ingest messages from a Kafka topic into Structured Streaming

import org.apache.spark.sql.functions.{from_json,col}
import org.apache.spark.sql.{Row, DataFrame, ForeachWriter}
import org.apache.spark.sql.types.{MapType, StringType}

// Define an abstract class for any stream
abstract class StreamIngester {
  def run_stream(): Unit
}

// Define a side effect abstract class to perform some write action for each dataframe Row
abstract class foreach_row_writer extends ForeachWriter[Row] {
    // Define any writing here e.g to S3 or just print
    def process(value: Row): Unit

    // Any IO we have to do
    def open(paritionId: Long, epochId: Long): Boolean

    def close(errorOrNull: Throwable): Unit
}

class row_printer extends foreach_row_writer{
    override def process(value: Row): Unit
      = println(s"We are going to process this value soon {$value}")

    override def open(partitionId: Long, epochId: Long): Boolean = true

    override def close(errorOrNull: Throwable): Unit = {}
}

class KafkaStramIngester extends StreamIngester {

  val dataframe_kafka_read: DataFrame =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "musical_tweets")
      .load()

  val mapped_dataframe: DataFrame = dataframe_kafka_read
    .selectExpr("CAST(value AS STRING)")
    .withColumn("value",from_json(col("value"),MapType(StringType, StringType)))

  def run_stream(): Unit =
    mapped_dataframe
      .writeStream
      .foreach(new row_printer)
      .option("truncate", false)
      .start()
      .awaitTermination()

  def get_url_component(row: DataSet[Row], batchId: Long) => {
    // TODO
    //regex url from dataframe
    // send req to api
    // post back the song(s)
  }
}
