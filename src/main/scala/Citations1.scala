import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Citations1 {

    def main(args: Array[String]): Unit = {
 
        val spark = SparkSession.builder.appName("Citations1").getOrCreate()
        import spark.implicits._
        
        // Read Citations from HDFS
        var cit = spark.read.textFile("hdfs:///pa1/citations.txt")
        // filter comments
        // val countOg = cit.count()
        // println(s"Original lines count: $countOg")
        cit = cit.filter(!$"value".contains("#"))
        // val countFil = cit.count()
        // println(s"Filtered lines count: $countFil")
        // create columns
        val citcleaned = cit.withColumn("fromnode", split(col("value"), "\t").getItem(0).cast("int"))
            .withColumn("tonode", split(col("value"), "\t").getItem(1).cast("int"))
            .drop("value")
        citcleaned.show()
        citcleaned.printSchema()

        // Read published-dates from HDFS
        var pd = spark.read.textFile("hdfs:///pa1/published-dates.txt")
        pd = pd.filter(!$"value".contains("#"))
        // create columns
        val pdcleaned = pd.withColumn("nodeid", split(col("value"), "\t").getItem(0).cast("int"))
            .withColumn("pdate", split(col("value"), "\t").getItem(1))
            .drop("value")
        val pdcleaned2 = pdcleaned.withColumn("pyear", split(col("pdate"), "-").getItem(0).cast("int"))
        pdcleaned2.show()
        pdcleaned2.printSchema()

        // Crreate views
        citcleaned.createOrReplaceTempView("citations")
        pdcleaned2.createOrReplaceTempView("pdates")

        // n(e) and e(t) per year
        val query1 = """
            SELECT
                nodes.pyear
                ,n_nodes_py
                ,n_edges_py
            FROM (
                SELECT 
                    pyear
                    ,COUNT(nodeid) AS n_nodes_py
                FROM pdates AS pd
                GROUP BY pyear
            ) AS nodes
            LEFT JOIN (
                SELECT 
                    pyear
                    ,COUNT(fromnode) AS n_edges_py
                FROM(
                    SELECT *
                    FROM citations AS c
                    LEFT JOIN pdates AS pd
                        ON c.fromnode = pd.nodeid
                ) AS dfj
                GROUP BY pyear
            ) AS edges
                ON nodes.pyear = edges.pyear
            ORDER BY nodes.pyear
        """;
        val graph_density_1 = spark.sql(query1)
        graph_density_1.show()
        graph_density_1.createOrReplaceTempView("graph_density_1")
        
        // n(e) and e(t) per year rolling
        val query2 = """
            SELECT 
                gd1.pyear AS year
                ,SUM(gd2.n_nodes_py) AS n_nodes
                ,SUM(gd2.n_edges_py) AS n_edges
            FROM graph_density_1 AS gd1
            LEFT JOIN graph_density_1 AS gd2
                ON gd1.pyear >= gd2.pyear
            GROUP BY gd1.pyear
            ORDER BY gd1.pyear
        """;
        val graph_density_2 = spark.sql(query2)
        graph_density_2.show()        

        spark.stop()

    }
}