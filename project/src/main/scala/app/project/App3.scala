// src/main/scala/batch/BatchJob.scala
package batch

import org.apache.spark.sql.{SparkSession, DataFrame}

object BatchJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IndependentBatchJob")
      .config("spark.sql.shuffle.partitions", "8") // config spécifique
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    run(spark)

    spark.stop()
  }

  def run(spark: SparkSession): Unit = {
    val input: DataFrame = spark.read
      .format("parquet")
      .load("/data/input/")

    val result = input
      .filter("status = 'active'")
      .groupBy("category")
      .count()

    result.write
      .mode("overwrite")
      .format("parquet")
      .save("/data/output/")
  }
  /*
  spark-submit --class batch.BatchJob \
  --master yarn \
  --conf spark.executor.memory=4g \
  target/yourapp.jar
   */


  /*
  // src/main/scala/monitoring/QueryMonitor.scala
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress, StreamingQueryException}
import org.apache.spark.sql.SparkSession

object QueryMonitor {
  def register(spark: SparkSession): Unit = {
    val listener = new StreamingQueryListener {

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println(s"[STARTED] id: ${event.id}, name: ${event.name.getOrElse("unnamed")}")
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val progress: StreamingQueryProgress = event.progress
        println(s"[PROGRESS] id: ${progress.id}, name: ${progress.name}, batchId: ${progress.batchId}, inputRows: ${progress.numInputRows}")
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println(s"[TERMINATED] id: ${event.id}")
        event.exception.foreach(e => println(s"[ERROR] ${e}"))
      }
    }

    spark.streams.addListener(listener)
  }
}

   */

  /*
  ==>

  object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StreamingApp").getOrCreate()

    // Enregistre le monitoring
    QueryMonitor.register(spark)

    val classe1 = new Classe1(spark)
    val classe2 = new Classe2(spark)

    classe1.saveToSink1()
    classe2.saveToSink2()

    spark.streams.awaitAnyTermination()
  }
}


   */



}
