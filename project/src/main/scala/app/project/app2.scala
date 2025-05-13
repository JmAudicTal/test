import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

object JobKafkaPhoenixStreaming {

  def component_idBis(maxIterations : Int = 10):(DataFrame,DataFrame) = {

    //todo: voir si on se base sur vertex pour calcul ou uniquement edges
    val dfv = dfV.select("id").distinct()
    //ou
    val dfv_ = dfE.select(col("src")).union(dfE.select(col("dst"))).distinct().toDF("id")
    val dfvWithCurrentIdGraph = dfv_.withColumn("idGraphTmp", monotonically_increasing_id())

    val dfe = dfE.select("src","dst")
    val dfeFull = dfe.union(dfe.select(col("dst").as("src"), col("src").as("dst"))).distinct()

    def uniformizeIdGraph(dfeFull:DataFrame,verticesWithTmpIdGraph:DataFrame):(DataFrame,Boolean) = {

      val joined = dfeFull
        .join(verticesWithTmpIdGraph,  col("src") === col("id"))
        .select(col("dst"), col("idGraphTmp"))

      val newIdGraphs = joined
        .groupBy("dst")
        .agg(max("idGraphTmp").as("idGraphTmp"))
        .withColumnRenamed("dst", "id")

      val compare = verticesWithTmpIdGraph
        .join(newIdGraphs.withColumnRenamed("idGraphTmp", "new_idGraphTmp"), Seq("id"))

      val hasDiff = !compare.filter(col("idGraphTmp") =!= col("new_idGraphTmp")).isEmpty

      val updatedIdGraphs = compare
        .withColumn("idGraphTmp", greatest(col("idGraphTmp"), col("new_idGraphTmp")))
        .select(col("id"), col("idGraphTmp"))

      (updatedIdGraphs, !hasDiff)
    }

    val (finalIdGraphs, _) = (0 until maxIterations).foldLeft((dfvWithCurrentIdGraph, false)) {
      case ((currentIdGraphs, true), _) =>
        (currentIdGraphs, true) // ==> idGraphTmp === nouveaux idGraphTmp calculés

      case ((currentIdGraphs, false), i) =>
        println(s"--- ITERATION: $i ---")
        uniformizeIdGraph(dfeFull, currentIdGraphs)

    }

    val edgesWithIdGraph = dfE.drop("idGraph")
      .join(finalIdGraphs.withColumnRenamed("id", "src"), Seq("src"), "left")
      .withColumnRenamed("idGraphTmp", "idGraph")

    val verticesWithIdGraph = dfV.drop("idGraph")
      .join(finalIdGraphs, Seq("id"),"left")


    (verticesWithIdGraph,edgesWithIdGraph)
  }

  /*
  ////////

  import java.util.List;

@FunctionalInterface
public interface ListProcessor {
    List<Integer> process(List<Integer> list, Integer num);
}
//////////////////////
//////////////////////
//////////////////////

import java.util.List;

// Définition de la classe abstraite
public abstract class Check2 {

    public abstract List<Integer> processLists(List<Integer> list1, List<Integer> list2);

    public abstract List<Integer> processLists(List<Integer> list1, List<Integer> list2, ListProcessor processor);

    public void displayMessage() {
        System.out.println("Méthode concrète dans une classe abstraite 2.");
    }

}

///////////////////
///////////////////
///////////////////
///////////////////
import java.util.List;
import java.util.ArrayList;
import java.util.function.Function;

// Classe concrète qui étend la classe abstraite Check
public class Check2Implementation extends Check2 {

    // Implémentation de la méthode abstraite
    @Override
    public List<Integer> processLists(List<Integer> list1, List<Integer> list2, ListProcessor processor ) {
        // Vérifier que les deux listes ont la même taille
        if (list1.size() != list2.size()) {
            throw new IllegalArgumentException("Les deux listes doivent avoir la même taille.");
        }

        // Créer une troisième liste pour stocker les résultats
        List<Integer> resultList = new ArrayList<>();

        // Additionner les éléments correspondants des deux listes
        for (int i = 0; i < list1.size(); i++) {
            resultList.add(list1.get(i) + list2.get(i));
        }

        Integer number = 4;

        if (processor != null) {
            resultList = processor.process(resultList,number);
        }

        return resultList;
    }

    public List<Integer> processLists(List<Integer> list1, List<Integer> list2) {
        return processLists(list1, list2, null); // Appelle la version avec hook en mettant null par défaut
    }


}
/////////////////
/////////////////

        List<Integer> list1 = List.of(1, 2, 3);
        List<Integer> list2 = List.of(4, 5, 6);

        Check2 check2 = new Check2Implementation();
        check2.displayMessage();

        ListProcessor listProcessor = (list, num) -> {
            List<Integer> processedList = new ArrayList<>();
            for (Integer element : list) {
                processedList.add(element + num); // Ajoute 'num' à chaque élément de la liste.
            }
            return processedList;
        };

        List<Integer> resultList4 = check2.processLists(new ArrayList<>(list1), new ArrayList<>(list2),listProcessor);
        System.out.println("Liste résultante : " + resultList4);


   */

  /*
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
package com.example.hook;

import org.apache.spark.sql.Row;

@FunctionalInterface
public interface RowComparator {
    boolean compare(Row row1, Row row2);
}

XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

// Exemple de test
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import com.example.hook.RowComparator;

public class HookTest {
    public static void main(String[] args) {
        // Création de deux Row fictifs
        Row row1 = RowFactory.create(true, "data1");
        Row row2 = RowFactory.create(true, "data2");

        // Hook (lambda) : compare si les deux premiers champs booléens sont true
        RowComparator comparator = (r1, r2) -> {
            Boolean b1 = r1.getBoolean(0);
            Boolean b2 = r2.getBoolean(0);
            return b1 != null && b2 != null && b1 && b2;
        };

        boolean result = comparator.compare(row1, row2);
        System.out.println("Résultat de la comparaison : " + result);
    }
}


DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDd
DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDd
DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDd

// src/main/java/com/example/hook/RowComparator.java
package com.example.hook;

import org.apache.spark.sql.Row;

@FunctionalInterface
public interface RowComparator {
    boolean compare(Row row1, Row row2);
}

// src/main/java/com/example/processor/CustomJoiner.java
package com.example.processor;

import com.example.hook.RowComparator;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.util.*;
import static org.apache.spark.sql.functions.*;

public class CustomJoiner {

    private final SparkSession spark;

    public CustomJoiner(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> process(String tableA, String tableB, RowComparator comparator) {
        Dataset<Row> dfA = spark.table(tableA);
        Dataset<Row> dfB = spark.table(tableB);

        List<Row> rowsA = dfA.collectAsList();
        List<Row> rowsB = dfB.collectAsList();

        List<Row> matchedRows = new ArrayList<>();

        for (Row rowA : rowsA) {
            for (Row rowB : rowsB) {
                if (comparator.compare(rowA, rowB)) {
                    // Combine two rows (example: just keep both as one row with 2 columns)
                    matchedRows.add(RowFactory.create(rowA, rowB));
                }
            }
        }

        StructType schema = new StructType()
            .add("left_row", dfA.schema())
            .add("right_row", dfB.schema());

        return spark.createDataFrame(matchedRows, schema);
    }
}

// Exemple d'utilisation
// src/main/java/com/example/Main.java
package com.example;

import com.example.hook.RowComparator;
import com.example.processor.CustomJoiner;
import org.apache.spark.sql.*;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Row Comparator Join")
                .master("local[*]")
                .getOrCreate();

        // Supposons que les tables sont déjà enregistrées : spark.sql("CREATE OR REPLACE TEMP VIEW ...")

        RowComparator comparator = (row1, row2) -> {
            // Suppose que le premier champ est booléen
            return row1.getBoolean(0) && row2.getBoolean(0);
        };

        CustomJoiner joiner = new CustomJoiner(spark);
        Dataset<Row> result = joiner.process("tableA", "tableB", comparator);
        result.show(false);

        spark.stop();
    }
}

FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFf
FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFf
FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFf

// Struct interne : {flag: Boolean, count: Integer}
        StructType innerStruct = new StructType(new StructField[]{
            new StructField("flag", DataTypes.BooleanType, false, Metadata.empty()),
            new StructField("count", DataTypes.IntegerType, false, Metadata.empty())
        });

        // Struct externe : {value: Struct<flag:Boolean, count:Integer>}
        StructType outerSchema = new StructType(new StructField[]{
            new StructField("value", innerStruct, false, Metadata.empty())
        });

        // Création du Row imbriqué
        Row innerRow = RowFactory.create(true, 42);
        Row outerRow = RowFactory.create(innerRow);

        Dataset<Row> df = spark.createDataFrame(Collections.singletonList(outerRow), outerSchema);


Row row = df.first(); // ou .collectAsList().get(0)
Row nested = row.getStruct(0); // car "value" est à l’index 0
boolean flag = nested.getBoolean(0); // "flag" est à l’index 0 dans "value"


   */
}
