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
            .persist()
        citcleaned.createOrReplaceTempView("citations_all")

        // Read published-dates from HDFS
        var pd = spark.read.textFile("hdfs:///pa1/published-dates.txt")
        pd = pd.filter(!$"value".contains("#"))
        val pdcleaned = pd.withColumn("nodeid", split(col("value"), "\t").getItem(0).cast("int"))
            .withColumn("pdate", split(col("value"), "\t").getItem(1))
            .drop("value")
        val pdcleaned2 = pdcleaned.withColumn("pyear", split(col("pdate"), "-").getItem(0).cast("int")).persist()
        pdcleaned2.createOrReplaceTempView("pdates")

        // Seq (array) to save stats per year
        var resultData: Seq[Row] = Seq.empty[Row]

        for( year <- 1992 to 1992)
        {
            println(s"********* Year : $year **************")

            // Distinct nodes
            // test_data
            val nodes = spark.sql(s"SELECT DISTINCT nodeid FROM (SELECT DISTINCT a AS nodeid FROM citations_all UNION SELECT DISTINCT b AS nodeid FROM citations_all)")
            // val nodes = spark.sql(s"SELECT DISTINCT nodeid FROM pdates WHERE pyear <= $year")
            nodes.createOrReplaceTempView("nodes")

            // Distinct node combinations 
            val query = """
                SELECT 
                    n1.nodeid AS a
                    ,n2.nodeid AS b
                FROM nodes n1
                JOIN nodes n2 
                    ON n1.nodeid < n2.nodeid
            """;
            val distComb = spark.sql(query)//.persist()
            distComb.show()
            distComb.createOrReplaceTempView("distComb")

            // citations simplified and magnified
            val query2 = s"""
                SELECT
                    n.nodeid AS a
                    ,IF(n.nodeid=c.a, c.b, c.a) AS b
                FROM nodes AS n
                LEFT JOIN citations_all AS c
                    ON n.nodeid = c.a 
                        OR n.nodeid = c.b
            """;
            val cit_year = spark.sql(query2)//.persist()
            cit_year.show()
            cit_year.createOrReplaceTempView("citations")


            //Single query for g(1-4)
            val all_links_query = """
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
                    -- AND dc.b = c4.b
            """
            val all_links = spark.sql(all_links_query)
            all_links.show()
            val n_all_links = all_links.count().toInt
            println(s"Number of records in all_links: $n_all_links")
            val outputPath = "hdfs:///pa1/graph_diameter_io_01"
            all_links.coalesce(1).write.format("csv").save(outputPath)


            val singleQuery = """
                WITH all_links AS (
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
                        -- AND dc.b = c4.b
                )
                SELECT 
                    SUM(g1) AS g1
                    ,SUM(g2) AS g2
                    ,SUM(g2) AS g3
                    ,SUM(g2) AS g4
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
            val graph_diameter_py = spark.sql(singleQuery)
            graph_diameter_py.show()

            // // g(1)
            // val queryg1 = """
            //     SELECT dc.a, dc.b
            //     FROM distComb AS dc
            //     LEFT JOIN citations AS c
            //         ON dc.a = c.a AND dc.b = c.b
            //     WHERE c.a IS NOT NULL
            // """;
            // val g1 = spark.sql(queryg1).persist()
            // g1.createOrReplaceTempView("g1")
            // val n_g1 = g1.count().toInt
            // // println(s"Number of nodes in g(1) in $year year: $n_g1")

            // // g(2)
            // var remainingComb = spark.sql("""
            //         SELECT dc.a, dc.b
            //         FROM distComb AS dc
            //         LEFT JOIN g1
            //             ON dc.a = g1.a AND dc.b = g1.b
            //         WHERE g1.a IS NULL
            //     """).persist()
            // remainingComb.createOrReplaceTempView("remainingComb")
            // distComb.unpersist()
            // g1.unpersist()
            // val queryg2 = """
            //     SELECT DISTINCT rc.a, rc.b
            //     FROM remainingComb AS rc
            //     LEFT JOIN citations AS c1
            //         ON rc.a = c1.a
            //     LEFT JOIN citations AS c2
            //         ON c1.b = c2.a AND rc.b = c2.b
            //     WHERE c2.a IS NOT NULL
            // """;
            // val g2 = spark.sql(queryg2).persist()
            // g2.createOrReplaceTempView("g2")
            // val n_g2 = g2.count().toInt + n_g1
            // // println(s"Number of nodes in g(2) in $year year: $n_g2")

            // // g(3)
            // remainingComb = spark.sql("""
            //         SELECT dc.a, dc.b
            //         FROM remainingComb AS dc
            //         LEFT JOIN g2
            //             ON dc.a = g2.a AND dc.b = g2.b
            //         WHERE g2.a IS NULL
            //     """).persist()
            // remainingComb.createOrReplaceTempView("remainingComb")
            // g2.unpersist()
            // val queryg3 = """
            //     SELECT DISTINCT rc.a, rc.b
            //     FROM remainingComb AS rc
            //     LEFT JOIN citations AS c1
            //         ON rc.a = c1.a
            //     LEFT JOIN citations AS c2
            //         ON c1.b = c2.a
            //     LEFT JOIN citations AS c3
            //         ON c2.b = c3.a AND rc.b = c3.b
            //     WHERE c3.a IS NOT NULL             
            // """;
            // var g3 = spark.sql(queryg3).persist()
            // g3.createOrReplaceTempView("g3")
            // val n_g3 = g3.count().toInt + n_g2
            // // println(s"Number of nodes in g(3) in $year year: $n_g3")

            // // g(4)
            // remainingComb = spark.sql("""
            //         SELECT dc.a, dc.b
            //         FROM remainingComb AS dc
            //         LEFT JOIN g3
            //             ON dc.a = g3.a AND dc.b = g3.b
            //         WHERE g3.a IS NULL
            //     """).persist()
            // remainingComb.createOrReplaceTempView("remainingComb")
            // g3.unpersist()
            // val queryg4 = """
            //     SELECT COUNT(DISTINCT rc.a, rc.b)
            //     -- SELECT DISTINCT rc.a, rc.b
            //     FROM remainingComb AS rc
            //     LEFT JOIN citations AS c1
            //         ON rc.a = c1.a
            //     LEFT JOIN citations AS c2
            //         ON c1.b = c2.a
            //     LEFT JOIN citations AS c3
            //         ON c2.b = c3.a
            //     LEFT JOIN citations AS c4
            //         ON c3.b = c4.a AND rc.b = c4.b
            //     WHERE c4.a IS NOT NULL             
            // """;
            // val n_g4 = spark.sql(queryg4).first().getLong(0).toInt + n_g3
            // // val g4 = spark.sql(queryg4)
            // // val n_g4 = g4.count().toInt + n_g3
            // // g4.show()
            // // g4.createOrReplaceTempView("g4")
            // // val n_g4 = spark.sql("SELECT COUNT(a) FROM g4").first().getLong(0).toInt + n_g3
            // // println(s"Number of nodes in g(4) in $year year: $n_g4")

            // // Unpersist cached dataframes
            // remainingComb.unpersist()
            // cit_year.unpersist()

            // // Append stats to result seq
            // resultData = resultData :+ Row(year, n_g1, n_g2, n_g3, n_g4)

        } // for loop end

        // // create output DF and export to HDFS
        // val resultSchema = new StructType()
        //     .add("year",IntegerType)
        //     .add("G1",IntegerType)
        //     .add("G2",IntegerType)
        //     .add("G3",IntegerType)
        //     .add("G4",IntegerType)
        // val result = spark.createDataFrame(spark.sparkContext.parallelize(resultData), resultSchema)
        // result.printSchema()
        // result.show()
        // val outputPath = "hdfs:///pa1/graph_diameter_08"
        // result.coalesce(1).write.format("csv").save(outputPath)

        spark.stop()
    }
}