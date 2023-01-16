// Ingest messages from a Kafka topic into Structured Streaming

import org.apache.spark.sql.functions.{from_json,col}
import org.apache.spark.sql.{Row, DataFrame, ForeachWriter}
import org.apache.spark.sql.types.{MapType, StringType}

// Get streaming data
// map each row to a Map Type
// find out wether each row contains a url
// if so -> call API which returns resp with music info
//

trait StreamIngester {

  class for_each_writer extends ForeachWriter[Row] {
    def process(value: Row): Unit

    def open(paritionId: Long, epochId: Long): Boolean

    def close(errorOrNull: Throwable): Unit
  }
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

  class for_each_writer extends ForeachWriter[Row] {
    override def process(value: Row): Unit
      = println(s"We are going to process this value soon {$value}")

    override def open(partitionId: Long, epochId: Long): Boolean = true

    override def close(errorOrNull: Throwable): Unit = {}
  }

  mapped_dataframe.writeStream.foreach(new for_each_writer).option("truncate", false).start().awaitTermination()

  def get_url_component(row: DataSet[Row], batchId: Long) => {
    //regex url from dataframe
    // send req to api
    // post back the song(s)
  }
}
