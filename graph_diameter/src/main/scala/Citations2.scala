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
                SELECT dc.a, dc.b
                FROM distComb AS dc
                LEFT JOIN g1
                    ON dc.a = g1.a AND dc.b = g1.b
                WHERE g1.a IS NULL
            )
            SELECT DISTINCT rc.a, rc.b
            FROM remainingComb AS rc
            LEFT JOIN citations AS c1
                ON rc.a = c1.a OR rc.a = c1.b
            LEFT JOIN citations AS c2
                ON (((c1.a = c2.a OR c1.a = c2.b) AND (c1.a != rc.a))
                        OR ((c1.b = c2.a OR c1.b = c2.b) AND (c1.b != rc.a)))
                    AND (c2.a = rc.b OR c2.b = rc.b)
            WHERE c2.a IS NOT NULL
        """;
        val g2 = spark.sql(queryg2)
        g2.show()
        g2.createOrReplaceTempView("g2")
        val n_g2 = g2.count()
        println(s"Number of nodes in g(2): $n_g2")

        // g(3)
        val queryg3 = """
            WITH remainingComb AS (
                SELECT dc.a, dc.b
                FROM distComb AS dc
                LEFT JOIN g1
                    ON dc.a = g1.a AND dc.b = g1.b
                LEFT JOIN g2
                    ON dc.a = g2.a AND dc.b = g2.b
                WHERE g1.a IS NULL OR g2.a IS NULL
            )
            SELECT *
            FROM remainingComb AS rc
            
        """;
        val g3 = spark.sql(queryg3)
        g3.show()
        g3.createOrReplaceTempView("g3")
        val n_g3 = g3.count()
        println(s"Number of nodes in g(3): $n_g3")


        spark.stop()
    }
}