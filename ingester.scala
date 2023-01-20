/* 
 * Ingest messages from a Kafka topic into Structured Streaming
 */

package ingestmusicstream

import scala.util.matching.Regex

import org.apache.spark.sql.functions.{from_json, to_json,col}
import org.apache.spark.sql.{Row, DataFrame, ForeachWriter, SparkSession}
import org.apache.spark.sql.types.{MapType, StringType, StructType}


// Define an abstract class for any stream
abstract class StreamIngester {
  def run_stream(): Unit
  def spark: SparkSession
}

// Define a side effect abstract class to perform some write action for each dataframe Row
abstract class ForeachRowWriter extends ForeachWriter[Row] {
    // Define any writing here e.g to S3 or just print
    def process(value: Row): Unit

    // Any IO we have to do
    def open(paritionId: Long, epochId: Long): Boolean

    def close(errorOrNull: Throwable): Unit
}

class RowPrinter extends ForeachRowWriter {
    /*
     * Side affect class which unpacks JSON DataFrame tweet data and prints it
     *
     * @param row the Row of the DataFrame which we are processing
     *  row: [Map("id" -> 1234, "text" ->"Hello this is a tweet")]
     *
     * @return Unit ,side affect class so we simply unpack and return Unit
     *
     */
    override def process(row: Row): Unit = {
      val row_value = row(0) match {
        // Unpack the map -> @ unchecked to suppress erasure elimination with Map[String, String]
        // Not ideal but we're always getting a string here so is safe
        case m: Map[String , String] @ unchecked => (m.get("text"), m.get("id")) match {
          case (Some(s), Some(id)) => println(s, id) // TODO Perform regex and api call here
          case (_ , _) =>
        }
        case _ =>
      }
    }
    override def open(partitionId: Long, epochId: Long): Boolean = true

    override def close(errorOrNull: Throwable): Unit = {}
}

class urlParser {
  def get_url_from_text(text: String): String = {
    val URL: Regex = """(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-])""".r
    URL.findFirstMatchIn(text) match {
      case Some(s) => s.toString
      case None => ""
    }
  }
}

class KafkaStramIngester extends StreamIngester {

  val spark = SparkSession
    .builder
    .appName("KafkaMusicStreamIngester")
    .getOrCreate()

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
      .foreach(new RowPrinter)
      .option("truncate", false)
      .start()
      .awaitTermination()

    // TODO
    // expand shortened URL
    // send req to api
    // post back the song(s)
}
