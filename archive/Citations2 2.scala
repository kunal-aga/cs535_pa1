import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Citations2 {

    def main(args: Array[String]): Unit = {
 
        val spark = SparkSession.builder.appName("Citations2").getOrCreate()
        import spark.implicits._
        
        // Read Citations from HDFS
        var cit = spark.read.textFile("hdfs:///pa1/citations.txt")
        // var cit = spark.read.textFile("hdfs:///pa1/test_data.txt") // test data
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
        val pdcleaned2 = pdcleaned.withColumn("pyear", split(col("pdate"), "-").getItem(0).cast("int"))
            // .persist()
        pdcleaned2.createOrReplaceTempView("pdates")

        var graph_diameter = spark.emptyDataFrame

        for( year <- 1992 to 2002)
        {
            println(s"********* Year : $year **************")

            // Single query and write per year
            val query = s"""
                -- WITH nodes AS (SELECT DISTINCT nodeid FROM (SELECT DISTINCT a AS nodeid FROM citations_all UNION SELECT DISTINCT b AS nodeid FROM citations_all)),
                WITH nodes AS (SELECT DISTINCT nodeid FROM pdates WHERE pyear <= $year),
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
                    LEFT JOIN (
                        SELECT *
                        FROM citations_all
                        WHERE a IN (SELECT nodeid FROM nodes)
                    ) AS c
                        ON n.nodeid = c.a 
                            OR n.nodeid = c.b                
                ),
                gd1 AS (
                    SELECT 
                        dc.a AS a
                        ,dc.b AS b
                        ,c1.a AS c1a
                        ,c1.b AS c1b
                        ,IF(dc.b = c1.b, 1, 0) AS g1
                    FROM distComb AS dc
                    LEFT JOIN citations AS c1
                        ON dc.a = c1.a
                ),
                gd2 AS (
                    SELECT
                        gdf1.a AS a
                        ,gdf1.b AS b
                        ,gdf1.c1a AS c1a
                        ,gdf1.c1b AS c1b
                        ,c2.a AS c2a
                        ,c2.b AS c2b
                        ,IF(gdf1.b = c2.b, 1, 0) AS g2
                    FROM (SELECT *, MAX(g1) OVER(PARTITION BY a,b) AS g1p FROM gd1) AS gdf1
                    -- FROM (SELECT * FROM (SELECT *, MAX(g1) OVER(PARTITION BY a,b) AS g1p FROM gd1) WHERE g1p = 0) AS gdf1
                    LEFT JOIN citations AS c2
                        ON c1b = c2.a                    
                    WHERE g1p = 0
                ),
                gd3 AS (
                    SELECT
                        gdf2.a AS a
                        ,gdf2.b AS b
                        ,gdf2.c1a AS c1a
                        ,gdf2.c1b AS c1b
                        ,gdf2.c2a AS c2a
                        ,gdf2.c2b AS c2b
                        ,c3.a AS c3a
                        ,c3.b AS c3b
                        ,IF(gdf2.b = c3.b, 1, 0) AS g3
                    FROM (SELECT *, MAX(g2) OVER(PARTITION BY a,b) AS g2p FROM gd2) AS gdf2
                    -- FROM (SELECT * FROM (SELECT *, MAX(g2) OVER(PARTITION BY a,b) AS g2p FROM gd2) WHERE g2p = 0) AS gdf2
                    LEFT JOIN citations AS c3
                        ON c2b = c3.a                    
                    WHERE g2p = 0
                ), 
                gd4 AS (
                    SELECT
                        gdf3.a AS a
                        ,gdf3.b AS b
                        ,gdf3.c1a AS c1a
                        ,gdf3.c1b AS c1b
                        ,gdf3.c2a AS c2a
                        ,gdf3.c2b AS c2b
                        ,gdf3.c3a AS c3a
                        ,gdf3.c3b AS c3b
                        ,c4.a AS c4a
                        ,c4.b AS c4b
                        ,IF(gdf3.b = c4.b, 1, 0) AS g4
                    FROM (SELECT *, MAX(g3) OVER(PARTITION BY a,b) AS g3p FROM gd3) AS gdf3
                    -- FROM (SELECT * FROM (SELECT *, MAX(g3) OVER(PARTITION BY a,b) AS g3p FROM gd3) WHERE g3p = 0) AS gdf3
                    LEFT JOIN citations AS c4
                        ON c3b = c4.a                    
                    WHERE g3p = 0
                )
                SELECT
                    '$year' AS year
                    ,SUM(IF(density_level <= 1, density, 0)) AS g1
                    ,SUM(IF(density_level <= 2, density, 0)) AS g2
                    ,SUM(IF(density_level <= 3, density, 0)) AS g3
                    ,SUM(IF(density_level <= 4, density, 0)) AS g4
                FROM (
                    SELECT 1 AS density_level, COUNT(DISTINCT a, b) AS density
                    FROM gd1
                    WHERE g1 = 1
                    UNION
                    SELECT 2 AS density_level, COUNT(DISTINCT a, b) AS density
                    FROM gd2
                    WHERE g2 = 1
                    UNION
                    SELECT 3 AS density_level, COUNT(DISTINCT a, b) AS density
                    FROM gd3
                    WHERE g3 = 1
                    UNION
                    SELECT 4 AS density_level, COUNT(DISTINCT a, b) AS density
                    FROM gd4
                    WHERE g4 = 1
                )
            """
            val graph_diameter_py = spark.sql(query)
            // graph_diameter_py.show()

            val outputPathpy = s"hdfs:///pa1/graph_diameter_16_py/$year"
            graph_diameter_py.write.format("csv").save(outputPathpy)

            // if (year == 1992) {
            //     graph_diameter = graph_diameter_py
            // } else {
            //     graph_diameter = graph_diameter.union(graph_diameter_py)
            // }

        } // for loop end

        // graph_diameter.show()
        // val outputPath = "hdfs:///pa1/graph_diameter_15"
        // graph_diameter.coalesce(1).write.format("csv").save(outputPath)
        // graph_diameter.write.format("csv").save(outputPath)

        spark.stop()
    }
}