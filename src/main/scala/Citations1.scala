import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Citations1 {

    def main(args: Array[String]): Unit = {
 
        val spark = SparkSession.builder.appName("Citations1").getOrCreate()
        import spark.implicits._
        
        // Read from HDFS
        var cit = spark.read.textFile("hdfs:///pa1/citations.txt")
        cit.printSchema()

        val countOg = cit.count()
        println(s"Original lines count: $countOg")

        cit = cit.filter(!$"value".contains("#"))
        val countFil = cit.count()
        println(s"Filtered lines count: $countFil")

        // val splitDf = udf(split)
        // cit = cit.withColumn("fromnode", splitDf(col("value"), "\t").getItem(0).cast("int"))
            // .withColumn("tonode", split(col("value"), "\t").getItem(1).cast("int"))

        cit.createOrReplaceTempView("citations")
        val sqlDF = spark.sql("SELECT * FROM citations LIMIT 10")
        sqlDF.show()


        cit.printSchema()
        
        spark.stop()

    }
}