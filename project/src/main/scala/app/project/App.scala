package app.project

import scala.io.StdIn
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, rand, when}
import org.apache.spark.sql.types.StringType
import org.graphframes
import org.graphframes.GraphFrame

import scala.util.Random

object App {
  def main(args: Array[String]): Unit = {

    println("TEST")

    val spark = SparkSession.builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    //
    //Création aléatoire de graph
    //
    val numVertices = 30

    val seed = 1
    val random = new Random()
    random.setSeed(seed)

    val vertices = spark.range(numVertices)
      .coalesce(1)
      .withColumn("id", monotonically_increasing_id().cast(StringType))
      .withColumn("type", when(rand(seed) < 0.65, "PP")
        .when(rand(seed) < 0.79, "PM")
        .when(rand(seed) < 0.90 ,"GP")
        .otherwise("ET"))

    vertices.show(false)

    def weightedRandomConnections(probabilities: Map[Int, Double]): Int = {
      val sortedProbs = probabilities.toSeq.sortBy(_._1) // Trie par clé (nombre de connexions)
      val cumulativeProbs = sortedProbs.scanLeft((0, 0.0)) { case ((_, acc), (numConnections, prob)) =>
        (numConnections, acc + prob)
      }.tail

      val randValue = random.nextDouble()
      cumulativeProbs.find(_._2 >= randValue).map(_._1).getOrElse(sortedProbs.last._1)
    }

    def limitedConnections(srcType: String, dstType: String, prob:  Map[Int, Double]): Seq[(String, String)] = {
      val srcNodes = vertices.filter($"type" === srcType).collect().map(_.getString(0))
      val dstNodes = vertices.filter($"type" === dstType).collect().map(_.getString(0))

      srcNodes.flatMap { src =>
        random.shuffle(dstNodes.toSeq).take(weightedRandomConnections(prob)).map(dst => (src, dst))
      }.toSeq

    }

    val pmToPpEdges = limitedConnections("PM", "PP", Map(1->0.3,2->0.3,3->0.3,4->0.1)) // Chaque PM peut se lier à max 3 PP

    val gpToPpEdges = limitedConnections("GP", "PP", Map(0->0.5,1->0.45,2->0.05)) // Chaque GP peut se lier à max 2 PP
    val gpToPmEdges = limitedConnections("GP", "PM", Map(1->0.3,2->0.4,3->0.3)) // Chaque GP peut se lier à max 2 PM

    val etToPpEdges = limitedConnections("ET", "PP", Map(0->0.8,1->0.1,2->0.1)) // Chaque ET peut se lier à max 2 PP
    val etToPmEdges = limitedConnections("ET", "PM", Map(0->0.5,1->0.3,2->0.2)) // Chaque ET peut se lier à max 2 PM
    val etToGpEdges = limitedConnections("ET", "GP", Map(1->0.6,2->0.4)) // Chaque ET peut se lier à max 2 GP


    val edges = spark.createDataFrame(
      gpToPpEdges ++ gpToPmEdges ++ pmToPpEdges ++ etToPpEdges ++ etToPmEdges ++ etToGpEdges
    ).toDF("src", "dst")


    val graph = GraphFrame(vertices, edges)

    vertices.show()
    edges.show()

    val df1 = graph.find("(a)-[e]->(b)").persist()
    df1.show()
    df1.filter(col("b.type") === "PP").show(false)




    Thread.sleep(1000)
    //val input = StdIn.readLine()

  }
}
