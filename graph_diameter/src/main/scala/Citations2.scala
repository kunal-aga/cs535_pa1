import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

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
                FROM (VALUES (1), (2), (3), (4), (5), (6)) AS t(col)
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
                ON (c2.a = rc.b OR c2.b = rc.b)
                    AND (c1.a = c2.a OR c1.a = c2.b OR c1.b = c2.a OR c1.b = c2.b)
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
                WHERE g1.a IS NULL AND g2.a IS NULL
            )
            SELECT DISTINCT rc.a, rc.b
            FROM remainingComb AS rc
            LEFT JOIN citations AS c1
                ON rc.a = c1.a OR rc.a = c1.b
            LEFT JOIN citations AS c2
                ON (
                    (c1.a != rc.a) 
                    AND ((c1.a = c2.a AND c1.b != c2.b) OR (c1.a = c2.b))
                ) OR (
                    (c1.b != rc.a) 
                    AND ((c1.b = c2.a) OR (c1.b = c2.b AND c1.a != c2.a))
                )
            LEFT JOIN citations AS c3
                ON (c3.a = rc.b OR c3.b = rc.b)
                    AND (c2.a = c3.a OR c2.a = c3.b OR c2.b = c3.a OR c2.b = c3.b)
            WHERE c3.a IS NOT NULL             
        """;
        val g3 = spark.sql(queryg3)
        g3.show()
        g3.createOrReplaceTempView("g3")
        val n_g3 = g3.count()
        println(s"Number of nodes in g(3): $n_g3")

        // g(4)
        val queryg4 = """
            WITH remainingComb AS (
                SELECT dc.a, dc.b
                FROM distComb AS dc
                LEFT JOIN g1
                    ON dc.a = g1.a AND dc.b = g1.b
                LEFT JOIN g2
                    ON dc.a = g2.a AND dc.b = g2.b
                LEFT JOIN g3
                    ON dc.a = g3.a AND dc.b = g3.b
                WHERE g1.a IS NULL AND g2.a IS NULL AND g3.a IS NULL
            )
            SELECT DISTINCT rc.a, rc.b
            FROM remainingComb AS rc
            LEFT JOIN citations AS c1
                ON rc.a = c1.a OR rc.a = c1.b
            LEFT JOIN citations AS c2
                ON (
                    (c1.a != rc.a) 
                    AND ((c1.a = c2.a AND c1.b != c2.b) OR (c1.a = c2.b))
                ) OR (
                    (c1.b != rc.a) 
                    AND ((c1.b = c2.a) OR (c1.b = c2.b AND c1.a != c2.a))
                )
            LEFT JOIN citations AS c3
                ON (
                    (c2.a != c1.a) 
                    AND ((c2.a = c3.a AND c2.b != c3.b) OR (c2.a = c3.b))
                ) OR (
                    (c2.b != c1.a) 
                    AND ((c2.b = c3.a) OR (c2.b = c3.b AND c2.a != c3.a))
                )
            LEFT JOIN citations AS c4
                ON (c4.a = rc.b OR c4.b = rc.b)
                    AND (c3.a = c4.a OR c3.a = c4.b OR c3.b = c4.a OR c3.b = c4.b)
            WHERE c4.a IS NOT NULL             
        """;
        val g4 = spark.sql(queryg4)
        g4.show()
        g4.createOrReplaceTempView("g4")
        val n_g4 = g4.count()
        println(s"Number of nodes in g(4): $n_g4")

        // create output
        val structureData = Seq(
            Row("test", g1, g2, g3, g4)
        )
        val structureSchema = new StructType()
            .add("year",StringType)
            .add("G1",IntegerType)
            .add("G2",IntegerType)
            .add("G3",IntegerType)
            .add("G4",IntegerType)

        val result = spark.createDataFrame(spark.sparkContext.parallelize(structureData), structureSchema)
        result.printSchema()
        result.show()

        spark.stop()
    }
}