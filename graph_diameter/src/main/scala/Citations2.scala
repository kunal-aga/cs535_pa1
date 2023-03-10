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
        // citcleaned.show()
        // citcleaned.printSchema()
        citcleaned.createOrReplaceTempView("citations_all")

        // Read published-dates from HDFS
        var pd = spark.read.textFile("hdfs:///pa1/published-dates.txt")
        pd = pd.filter(!$"value".contains("#"))
        val pdcleaned = pd.withColumn("nodeid", split(col("value"), "\t").getItem(0).cast("int"))
            .withColumn("pdate", split(col("value"), "\t").getItem(1))
            .drop("value")
        val pdcleaned2 = pdcleaned.withColumn("pyear", split(col("pdate"), "-").getItem(0).cast("int"))
            // .persist()
        // pdcleaned2.show()
        // pdcleaned2.printSchema()
        pdcleaned2.createOrReplaceTempView("pdates")

        // Seq (array) to save stats per year
        var resultData: Seq[Row] = Seq.empty[Row]

        for( year <- 1992 to 2002)
        {
            // println(s"********* Year : $year **************")

            // Year filtered raw data
            // nodes
            val nodes = spark.sql(s"""
                SELECT DISTINCT nodeid
                FROM pdates
                WHERE pyear <= $year
            """).persist()
            // val nodes = spark.sql("""SELECT DISTINCT nodeid FROM (SELECT DISTINCT a AS nodeid FROM citations_all UNION SELECT DISTINCT b AS nodeid FROM citations_all)""").persist()
            nodes.createOrReplaceTempView("nodes")
            // node combinations
            val distComb = spark.sql("""
                SELECT 
                    n1.nodeid AS a
                    ,n2.nodeid AS b
                FROM nodes n1
                JOIN nodes n2 
                    ON n1.nodeid < n2.nodeid
            """).persist()
            distComb.createOrReplaceTempView("distComb")
            // citations-simplified
            val cit_year = spark.sql("""
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
            """)
            // .repartition(1000)
            .persist()
            cit_year.createOrReplaceTempView("citations")   
            nodes.unpersist()        

            // g(1)
            val queryg1 = """
                SELECT dc.a, dc.b
                FROM distComb AS dc
                LEFT JOIN citations AS c
                    ON dc.a = c.a AND dc.b = c.b
                WHERE c.a IS NOT NULL
            """;
            val g1 = spark.sql(queryg1).persist()
            // g1.show()
            g1.createOrReplaceTempView("g1")
            val n_g1 = g1.count().toInt
            // val n_g1 = spark.sql("SELECT COUNT(a) FROM g1").first().getLong(0).toInt
            println(s"Number of nodes in g(1) in $year year: $n_g1")

            // g(2)
            var remainingComb = spark.sql("""
                    SELECT dc.a, dc.b
                    FROM distComb AS dc
                    LEFT JOIN g1
                        ON dc.a = g1.a AND dc.b = g1.b
                    WHERE g1.a IS NULL
                """)//.persist()
            remainingComb.createOrReplaceTempView("remainingComb")
            distComb.unpersist()
            g1.unpersist()
            val queryg2 = """
                SELECT DISTINCT rc.a, rc.b
                FROM remainingComb AS rc
                LEFT JOIN citations AS c1
                    ON rc.a = c1.a
                LEFT JOIN citations AS c2
                    ON c1.b = c2.a AND c2.b = rc.b
                WHERE c2.a IS NOT NULL
            """;
            var g2 = spark.sql(queryg2).persist()
            // g2.show()
            g2.createOrReplaceTempView("g2")
            val n_g2 = g2.count().toInt + n_g1
            println(s"Number of nodes in g(2) in $year year: $n_g2")

            // g(3)
            remainingComb = spark.sql("""
                    SELECT dc.a, dc.b
                    FROM remainingComb AS dc
                    LEFT JOIN g2
                        ON dc.a = g2.a AND dc.b = g2.b
                    WHERE g2.a IS NULL
                """)//.persist()
            remainingComb.createOrReplaceTempView("remainingComb")
            g2.unpersist()
            val queryg3 = """
                SELECT DISTINCT rc.a, rc.b
                FROM remainingComb AS rc
                LEFT JOIN citations AS c1
                    ON rc.a = c1.a
                LEFT JOIN citations AS c2
                    ON c1.b = c2.a
                LEFT JOIN citations AS c3
                    ON c2.b = c3.a AND c3.b = rc.b
                WHERE c3.a IS NOT NULL             
            """;
            var g3 = spark.sql(queryg3).persist()
            // g3.show()
            g3.createOrReplaceTempView("g3")
            val n_g3 = g3.count().toInt + n_g2
            println(s"Number of nodes in g(3) in $year year: $n_g3")

            // g(4)
            remainingComb = spark.sql("""
                    SELECT dc.a, dc.b
                    FROM remainingComb AS dc
                    LEFT JOIN g3
                        ON dc.a = g3.a AND dc.b = g3.b
                    WHERE g3.a IS NULL
                """)//.persist()
            remainingComb.createOrReplaceTempView("remainingComb")
            g3.unpersist()
            val queryg4 = """
                SELECT DISTINCT rc.a, rc.b
                FROM remainingComb AS rc
                LEFT JOIN citations AS c1
                    ON rc.a = c1.a
                LEFT JOIN citations AS c2
                    ON c1.b = c2.a
                LEFT JOIN citations AS c3
                    ON c2.b = c3.a
                LEFT JOIN citations AS c4
                    ON c3.b = c4.a AND c4.b = rc.b
                WHERE c4.a IS NOT NULL             
            """;
            val g4 = spark.sql(queryg4)
            // g4.show()
            // g4.createOrReplaceTempView("g4")
            val n_g4 = g4.count().toInt + n_g3
            println(s"Number of nodes in g(4) in $year year: $n_g4")

            // Unpersist cached dataframes
            cit_year.unpersist()

            // Append stats to result seq
            resultData = resultData :+ Row(year, n_g1, n_g2, n_g3, n_g4)

            // create output DF and export to HDFS
            val resultSchema = new StructType()
                .add("year",IntegerType)
                .add("G1",IntegerType)
                .add("G2",IntegerType)
                .add("G3",IntegerType)
                .add("G4",IntegerType)
            val result = spark.createDataFrame(spark.sparkContext.parallelize(resultData), resultSchema)
            result.printSchema()
            result.show()
            val outputPath = s"hdfs:///pa1/graph_diameter_18_py/$year"
            result.coalesce(1).write.format("csv").save(outputPath)

        } // for loop end

        // create output DF and export to HDFS
        val resultSchema = new StructType()
            .add("year",IntegerType)
            .add("G1",IntegerType)
            .add("G2",IntegerType)
            .add("G3",IntegerType)
            .add("G4",IntegerType)
        val result = spark.createDataFrame(spark.sparkContext.parallelize(resultData), resultSchema)
        result.printSchema()
        result.show()
        val outputPath = "hdfs:///pa1/graph_diameter_18"
        result.coalesce(1).write.format("csv").save(outputPath)

        spark.stop()
    }
}