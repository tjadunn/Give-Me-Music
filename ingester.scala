// Ingest messages from a Kafka topic into Structured Streaming

import org.apache.spark.sql.functions.{from_json,col}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{MapType, StringType}

// Get streaming data
// map each row to a Map Type
// find out wether each row contains a url
// if so -> call API which returns resp with music info
//

val df: DataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "musical_tweets").load()
val df2: DataFrame = df.selectExpr("CAST(value AS STRING)").withColumn("value",from_json(col("value"),MapType(StringType, StringType)))
df2.writeStream.foreach(...).format("console").option("truncate", false).start().awaitTermination()

def get_url_component(row: DataSet[Row], batchId: Long) => {
  //regex url from dataframe
  // send req to api
  // post back the song(s)
}
