import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Citations2 {

    def main(args: Array[String]): Unit = {
 
        val spark = SparkSession.builder.appName("Citations2").getOrCreate()
        import spark.implicits._
        
        // Read Citations from HDFS
        var cit = spark.read.textFile("hdfs:///pa1/test_data.txt")
        cit = cit.filter(!$"value".contains("#"))
        // create columns
        val citcleaned = cit.withColumn("a", split(col("value"), "\t").getItem(0).cast("int"))
            .withColumn("b", split(col("value"), "\t").getItem(1).cast("int"))
            .drop("value")
        citcleaned.show()
        citcleaned.printSchema()
        citcleaned.createOrReplaceTempView("citations")

        // sample query for forming distinct combinations 
        val query = """
            WITH data AS (
                SELECT *
                FROM (VALUES (1), (2), (3), (4), (5)) AS t(col)
            )
            SELECT 
                d1.col AS a
                ,d2.col AS b
            FROM data d1
            JOIN data d2 
                ON d1.col < d2.col
        """;
        val distComb = spark.sql(query)
        distComb.show()
        distComb.createOrReplaceTempView("distComb")

        // g(1)
        val queryg1 = """
            SELECT dc.a, dc.b
            FROM distComb AS dc
            LEFT JOIN citations AS c
                ON (c.a = dc.a AND c.b = dc.b)
                    OR (c.a = dc.b AND c.b = dc.a)
            WHERE c.a IS NOT NULL
        """;
        val g1 = spark.sql(queryg1)
        g1.show()
        g1.createOrReplaceTempView("g1")
        val n_g1 = g1.count()
        println(s"Number of nodes in g(1): $n_g1")

        // g(2)
        val queryg2 = """
            WITH remainingComb AS (
                SELECT *
                FROM distComb AS dc
                LEFT JOIN g1
                    ON dc.a = g1.a AND dc.b = g1.b
                WHERE g1.a IS NULL
            )
            SELECT *
            FROM remainingComb
        """;
        val g2 = spark.sql(queryg2)
        g2.show()
        g2.createOrReplaceTempView("g2")

        spark.stop()
    }
}