import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

object JobKafkaPhoenixStreaming {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Kafka + Phoenix Streaming Job")
      .getOrCreate()

    import spark.implicits._
    implicit val s = spark

    // Lecture batch des tables Phoenix
    val df1 = transformTable1(readPhoenix("table_phoenix_1"))
    val df2 = transformTable2(readPhoenix("table_phoenix_2"))

    // Lecture du topic Kafka en streaming
    val dfKafkaStream = transformKafka(
      readKafkaStream("mon-topic-kafka")
    )

    // Jointure streaming avec batch (clé: id)
    val joined = dfKafkaStream
      .join(df1, Seq("id"), "left")
      .join(df2, Seq("id"), "left")

    // Écriture en Phoenix avec foreachBatch
    val query = joined.writeStream
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        writePhoenix("table_resultat_streaming", batchDF)
      }
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }

  def readPhoenix(table: String)(implicit spark: SparkSession): DataFrame = {
    ??? // déjà implémentée
  }

  def readKafkaStream(topic: String)(implicit spark: SparkSession): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .load()
  }

  def transformTable1(df: DataFrame): DataFrame = {
    df.filter("status = 'active'")
      .withColumnRenamed("timestamp", "ts1")
  }

  def transformTable2(df: DataFrame): DataFrame = {
    df.select("id", "value")
      .withColumnRenamed("value", "val2")
  }

  def transformKafka(df: DataFrame): DataFrame = {
    df.selectExpr("CAST(key AS STRING) as id", "CAST(value AS STRING) as message")
  }

  def writePhoenix(table: String, df: DataFrame): Unit = {
    ??? // déjà implémentée
  }
}
