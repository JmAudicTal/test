// src/main/scala/GraphPhoenixJob.scala
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

object GraphPhoenixJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GraphPhoenixConnectedComponents")
      .getOrCreate()

    import spark.implicits._

    // 1. Lecture du fichier HDFS contenant TimestampLast
    val dfTimestamp = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs:///path/to/file.csv")

    val TimestampLast = dfTimestamp
      .select($"TimestampLast")
      .as[java.sql.Timestamp]
      .head()

    // 2. Lecture des tables Phoenix
    val dfEdges = read("Edges")
    val dfVertex = read("Vertex")

    // 3. Filtrer sur Timestamp
    val dfEdgesFiltered = dfEdges.filter($"TIMESTAMPS" > lit(TimestampLast))
    val dfVertexFiltered = dfVertex.filter($"TIMESTAMPS" > lit(TimestampLast))

    // 4. Récupérer les IDGRAPH distincts
    val idGraphsEdges = dfEdgesFiltered.select("IDGRAPH").distinct()
    val idGraphsVertex = dfVertexFiltered.select("IDGRAPH").distinct()
    val idGraphs = idGraphsEdges.union(idGraphsVertex).distinct()

    // 5. Relire Edge et Vertex sur ces IDGRAPH
    val dfEdgesSub = dfEdges.join(idGraphs, Seq("IDGRAPH"), "inner")
    val dfVertexSub = dfVertex.join(idGraphs, Seq("IDGRAPH"), "inner")

    // 6. Construire le GraphFrame
    val g = GraphFrame(dfVertexSub, dfEdgesSub)

    // 7. Connected components avec graphx
    val resultVertices = g.connectedComponents.run()
      .withColumnRenamed("component", "IDGRAPH")

    // Mise à jour des sommets
    val updatedVertices = dfVertexSub
      .drop("IDGRAPH")
      .join(resultVertices.select("id", "IDGRAPH"), Seq("id"))

    // Mise à jour des arêtes
    val updatedEdges = dfEdgesSub
      .drop("IDGRAPH")
      .join(resultVertices.select($"id".as("SRC"), $"IDGRAPH"), Seq("SRC"))
      .join(resultVertices.select($"id".as("DST"), $"IDGRAPH"), Seq("DST"))

    // 8. Écrire vers Phoenix
    write("Vertex", updatedVertices)
    write("Edges", updatedEdges)

    spark.stop()
  }

  // Méthodes d'accès Phoenix
  def read(table: String)(implicit spark: SparkSession): DataFrame = {
    // Ton implémentation custom ici
    spark.emptyDataFrame
  }

  def write(table: String, df: DataFrame): Unit = {
    // Ton implémentation custom ici
    df.show(false)
  }
}
//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
/*
Spark pousse les filtres et les colonnes vers Phoenix quand c’est possible (predicate pushdown + column pruning).

  Cela veut dire :

  Si tu écris directement dfEdges.filter($"TIMESTAMPS" > TimestampLast), Spark traduira ça en WHERE TIMESTAMPS > ? dans la requête SQL envoyée à Phoenix.

  Si tu fais dfEdges.select("SRC", "DST"), Spark ne ramènera que ces colonnes, pas toute la table.

✅ Donc la sélection se fait bien côté Phoenix (serveur), pas en chargeant toute la table côté Spark, à condition que :

  Tes filtres soient exprimés de manière simple (comparaison, IN, etc.) et pas dans des UDF Spark.

  Tu n’appelles pas d’collect() ou de transformations trop complexes avant le filtre.

  🚨 Cas où ce n’est PAS pushdown

Si tu appliques un filter avec une UDF Spark : ça ramène tout côté Spark, puis applique en mémoire.

Si tu fais des join Spark avant d’appliquer le filtre.

  Si tu utilises des fonctions Spark qui ne peuvent pas être traduites en SQL Phoenix.

  💡 Bonnes pratiques

Exprimer les filtres au plus tôt et avec des expressions simples :

val dfEdges = spark.read
  .format("org.apache.phoenix.spark")
  .option("table", "EDGES")
  .option("zkUrl", "zkhost:2181")
  .load()
  .filter($"TIMESTAMPS" > lit(TimestampLast))
  .select("SRC", "DST", "IDGRAPH", "TIMESTAMPS")


→ ça pousse bien le WHERE et le SELECT dans Phoenix.

  Vérifier le plan d’exécution :

  dfEdges.explain(true)


Si le filtre est bien poussé, tu verras PushedFilters dans le plan logique.

 */

//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

!!! Test => calcComponent :> check id!!!

---------------------------------
---------------------------------
---------------------------------
---------------------------------
Si non ordoné comme mentionné ==>
// src/main/scala/StableIdGraphJob.scala
// Spark 3.3.x, GraphFrames 0.8.x, Phoenix connector (spark-phoenix)
// Objectif: IDGRAPH stable = plus petit "id" par composant (min lexical par défaut ou min numérique optionnel)

package app

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.graphframes.GraphFrame

object StableIdGraphJob {

  /** Configuration simple pour Phoenix. */
  final case class AppConfig(
                              zkUrl: String,                // ex: "zk-host1,zk-host2,zk-host3:2181"
                              edgesTable: String = "EDGES",
                              verticesTable: String = "VERTEX",
                              modeMinId: String = "lex"     // "lex" (par défaut) ou "num"
                            )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StableIdGraphJob")
      .getOrCreate()

    import spark.implicits._

    // -- Paramètres (adapter selon votre déploiement)
    val conf = AppConfig(
      zkUrl = sys.env.getOrElse("PHOENIX_ZK_URL", "localhost:2181"),
      edgesTable = sys.env.getOrElse("PHOENIX_EDGES", "EDGES"),
      verticesTable = sys.env.getOrElse("PHOENIX_VERTICES", "VERTEX"),
      modeMinId = sys.env.getOrElse("MIN_ID_MODE", "lex") // "lex" ou "num"
    )

    // -- Lecture Phoenix avec pushdown
    val dfEdgesRaw = readPhoenix(conf.edgesTable)(spark, conf)
    val dfVerticesRaw = readPhoenix(conf.verticesTable)(spark, conf)

    // Renommages pour GraphFrame (obligatoire: vertices.id, edges.src/dst)
    val dfVertices = dfVerticesRaw
      .withColumnRenamed("ID", "id")
      .withColumn("IDGRAPH_OLD", col("IDGRAPH")) // conserve l'ancien IDGRAPH pour repli

    val dfEdges = dfEdgesRaw
      .withColumnRenamed("SRC", "src")
      .withColumnRenamed("DST", "dst")
      .withColumn("IDGRAPH_OLD", col("IDGRAPH"))

    // -- GraphFrame & connected components (algo graphx)
    val g = GraphFrame(dfVertices, dfEdges)
    val resultVertices = g.connectedComponents.setAlgorithm("graphx").run() // cols: id, component, ...

    // -- Mapping component -> IDGRAPH stable (min id)
    val comp2Graph = computeComponentMinId(resultVertices, conf.modeMinId)(spark)
    // comp2Graph: component, IDGRAPH (même type que vertices.id)

    // -- Mise à jour des VERTEX
    val mappedVertices = resultVertices
      .select(col("id"), col("component"))
      .join(comp2Graph, Seq("component"), "left")
      .select(col("id"), col("IDGRAPH"))

    val updatedVertices = dfVertices
      .drop("IDGRAPH")
      .join(mappedVertices, Seq("id"), "left")
      .withColumn("IDGRAPH",
        coalesce(col("IDGRAPH"), col("IDGRAPH_OLD")) // repli si jamais pas de composant
      )
      .drop("IDGRAPH_OLD")

    // -- Mise à jour des EDGES (coalesce entre src et dst)
    val mappedSrc = mappedVertices.withColumnRenamed("id", "src").withColumnRenamed("IDGRAPH", "IDGRAPH_SRC")
    val mappedDst = mappedVertices.withColumnRenamed("id", "dst").withColumnRenamed("IDGRAPH", "IDGRAPH_DST")

    val edgesWithMaps = dfEdges
      .drop("IDGRAPH")
      .join(mappedSrc, Seq("src"), "left")
      .join(mappedDst, Seq("dst"), "left")
      .withColumn("IDGRAPH_NEW", coalesce(col("IDGRAPH_SRC"), col("IDGRAPH_DST")))

    // Contrôle d'intégrité (facultatif): edges dont les deux côtés mènent à des IDGRAPH différents
    val inconsistencies = edgesWithMaps
      .filter(col("IDGRAPH_SRC").isNotNull && col("IDGRAPH_DST").isNotNull && col("IDGRAPH_SRC") =!= col("IDGRAPH_DST"))
      .select("src", "dst", "IDGRAPH_SRC", "IDGRAPH_DST")

    if (!inconsistencies.isEmpty) {
      // Pourquoi: signale des anomalies de partitionnement (devrait être rare)
      inconsistencies.show(50, truncate = false)
    }

    val updatedEdges = edgesWithMaps
      .withColumn("IDGRAPH", coalesce(col("IDGRAPH_NEW"), col("IDGRAPH_OLD")))
      .drop("IDGRAPH_SRC", "IDGRAPH_DST", "IDGRAPH_NEW", "IDGRAPH_OLD")

    // -- Remettre les noms attendus par Phoenix si nécessaire
    val outVertices = updatedVertices
      .withColumnRenamed("id", "ID")

    val outEdges = updatedEdges
      .withColumnRenamed("src", "SRC")
      .withColumnRenamed("dst", "DST")

    // -- Upsert Phoenix
    writePhoenix(conf.verticesTable, outVertices)(spark, conf)
    writePhoenix(conf.edgesTable, outEdges)(spark, conf)

    spark.stop()
  }

  /** Calcule le plus petit id par composant et le nomme IDGRAPH.
   * @param resultVertices DataFrame issu de connectedComponents: colonnes (id, component, ...)
   * @param mode "lex" pour min lexicographique ; "num" pour min numérique basé sur les chiffres extraits
   */
  def computeComponentMinId(resultVertices: DataFrame, mode: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    mode.toLowerCase match {
      case "num" =>
        // Pourquoi: si vos ids sont de forme "AB12345" et vous voulez un ordre numérique sur 12345
        val withNum = resultVertices
          .withColumn("id_num", regexp_extract(col("id"), "(\\d+)", 1).cast("long"))
        val w = Window.partitionBy("component").orderBy(col("id_num").asc_nulls_last, col("id").asc)
        withNum
          .withColumn("rn", row_number().over(w))
          .filter(col("rn") === lit(1))
          .select(col("component"), col("id").as("IDGRAPH"))

      case _ =>
        // Min lexicographique natif Spark (StringType ou LongType)
        resultVertices
          .groupBy("component")
          .agg(min(col("id")).as("IDGRAPH"))
    }
  }

  // ---- Phoenix IO helpers ----
  def readPhoenix(table: String)(implicit spark: SparkSession, conf: AppConfig): DataFrame = {
    spark.read
      .format("org.apache.phoenix.spark")
      .option("table", table)
      .option("zkUrl", conf.zkUrl)
      .load()
  }

  def writePhoenix(table: String, df: DataFrame)(implicit spark: SparkSession, conf: AppConfig): Unit = {
    df.write
      .format("org.apache.phoenix.spark")
      .mode("upsert")
      .option("table", table)
      .option("zkUrl", conf.zkUrl)
      .save()
  }
}
