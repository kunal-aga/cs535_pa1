import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Citations2 {

    def main(args: Array[String]): Unit = {
 
        val spark = SparkSession.builder.appName("Citations2").getOrCreate()
        import spark.implicits._
        
        // Read Citations from HDFS
        // var cit = spark.read.textFile("hdfs:///pa1/citations.txt")
        var cit = spark.read.textFile("hdfs:///pa1/test_data.txt")
        cit = cit.filter(!$"value".contains("#"))
        val citcleaned = cit.withColumn("a", split(col("value"), "\t").getItem(0).cast("int"))
            .withColumn("b", split(col("value"), "\t").getItem(1).cast("int"))
            .drop("value")
            // .persist()
        citcleaned.createOrReplaceTempView("citations_all")

        // Read published-dates from HDFS
        var pd = spark.read.textFile("hdfs:///pa1/published-dates.txt")
        pd = pd.filter(!$"value".contains("#"))
        val pdcleaned = pd.withColumn("nodeid", split(col("value"), "\t").getItem(0).cast("int"))
            .withColumn("pdate", split(col("value"), "\t").getItem(1))
            .drop("value")
        val pdcleaned2 = pdcleaned.withColumn("pyear", split(col("pdate"), "-").getItem(0).cast("int"))//.persist()
        pdcleaned2.createOrReplaceTempView("pdates")

        // var graph_diameter = spark.emptyDataFrame

        for( year <- 1992 to 1993)
        {
            println(s"********* Year : $year **************")

            // Single query and write per year
            val query = s"""
                WITH nodes AS (SELECT DISTINCT nodeid FROM (SELECT DISTINCT a AS nodeid FROM citations_all UNION SELECT DISTINCT b AS nodeid FROM citations_all)),
                -- WITH nodes AS (SELECT DISTINCT nodeid FROM pdates WHERE pyear <= $year),
                distComb AS (
                    SELECT 
                        n1.nodeid AS a
                        ,n2.nodeid AS b
                    FROM nodes n1
                    JOIN nodes n2 
                        ON n1.nodeid < n2.nodeid                
                ),
                citations AS (
                    SELECT
                        n.nodeid AS a
                        ,IF(n.nodeid=c.a, c.b, c.a) AS b
                    FROM nodes AS n
                    LEFT JOIN citations_all AS c
                        ON n.nodeid = c.a 
                            OR n.nodeid = c.b                
                ),
                all_links AS (
                    SELECT 
                        dc.a AS dca
                        ,dc.b AS dcb
                        ,c1.a AS c1a
                        ,c1.b AS c1b
                        ,c2.a AS c2a
                        ,c2.b AS c2b
                        ,c3.a AS c3a
                        ,c3.b AS c3b
                        ,c4.a AS c4a
                        ,c4.b AS c4b
                    FROM distComb AS dc
                    LEFT JOIN citations AS c1
                        ON dc.a = c1.a
                    LEFT JOIN citations AS c2
                        ON c1.b = c2.a
                    LEFT JOIN citations AS c3
                        ON c2.b = c3.a
                    LEFT JOIN citations AS c4
                        ON c3.b = c4.a 
                )
                SELECT 
                    '$year' AS year
                    ,SUM(g1) AS g1
                    ,SUM(g2) AS g2
                    ,SUM(g3) AS g3
                    ,SUM(g4) AS g4
                FROM (
                    SELECT
                        a
                        ,b
                        ,MAX(g1) AS g1
                        ,MAX(g2) AS g2
                        ,MAX(g3) AS g3
                        ,MAX(g4) AS g4
                    FROM (
                        SELECT 
                            dca AS a
                            ,dcb AS b
                            ,IF(c1b=dcb, 1, 0) AS g1
                            ,IF((c1b=dcb OR c2b=dcb), 1, 0) AS g2
                            ,IF((c1b=dcb OR c2b=dcb OR c3b=dcb), 1, 0) AS g3
                            ,IF((c1b=dcb OR c2b=dcb OR c3b=dcb OR c4b=dcb), 1, 0) AS g4
                        FROM all_links
                    )
                    GROUP BY 
                        a, b
                )            
            """
            val graph_diameter_py = spark.sql(query)
            // graph_diameter_py.show()
            val outputPath = s"hdfs:///pa1/graph_diameter_test_py_01/$year"
            // graph_diameter_py.coalesce(1).write.format("csv").save(outputPath)
            graph_diameter_py.write.format("csv").save(outputPath)

        } // for loop end

        // graph_diameter.show()
        // val outputPath = "hdfs:///pa1/graph_diameter_10"
        // graph_diameter.coalesce(1).write.format("csv").save(outputPath)
        // graph_diameter.write.format("csv").save(outputPath)

        spark.stop()
    }
}