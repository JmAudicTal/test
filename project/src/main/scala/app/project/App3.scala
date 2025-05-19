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

  val captor = ArgumentCaptor.forClass(classOf[DataFrame])
  verify(mockWriter).write(captor.capture())

  val capturedDF = captor.getValue
  assert(capturedDF.columns.contains("name"))
  assert(capturedDF.columns.contains("age"))
  assert(capturedDF.count() == 2)


  /////////////

  def run(): Unit = {
    val df = writer.read("myTablePhoenix")
    val date = currentDate
    val newDF = df.calcFromDate(date)
    writer.write(newDF, "mySecondTablePhoenix")
  }

  protected def currentDate: LocalDate = LocalDate.now()



  test("should pass DataFrame with fixed date to writer.write") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val fixedDate = LocalDate.of(2024, 10, 1)
    val originalDF = Seq(("Alice", 10)).toDF("name", "age")

    // Extension implicite qui modifie légèrement le DataFrame
    implicit class DFExt(df: DataFrame) {
      def calcFromDate(date: LocalDate): DataFrame = {
        // Simule une transformation avec la date
        df.withColumn("run_date", lit(date.toString))
      }
    }

    val mockWriter = mock[PhoenixWriter]
    when(mockWriter.read("myTablePhoenix")).thenReturn(originalDF)

    // MyJob avec date mockée
    val job = new MyJob(mockWriter) {
      override protected def currentDate: LocalDate = fixedDate
    }

    job.run()

    val dfCaptor = ArgumentCaptor.forClass(classOf[DataFrame])
    verify(mockWriter).write(dfCaptor.capture(), eq("mySecondTablePhoenix"))

    val captured = dfCaptor.getValue
    val result = captured.collect().map(_.getAs[String]("run_date")).toSet

    assert(result == Set("2024-10-01"))
  }
}





}













}
