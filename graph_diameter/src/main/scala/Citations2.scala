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

        // // Read published-dates from HDFS
        // var pd = spark.read.textFile("hdfs:///pa1/published-dates.txt")
        // pd = pd.filter(!$"value".contains("#"))
        // // create columns
        // val pdcleaned = pd.withColumn("nodeid", split(col("value"), "\t").getItem(0).cast("int"))
        //     .withColumn("pdate", split(col("value"), "\t").getItem(1))
        //     .drop("value")
        // val pdcleaned2 = pdcleaned.withColumn("pyear", split(col("pdate"), "-").getItem(0).cast("int"))
        // pdcleaned2.show()
        // pdcleaned2.printSchema()

        // Crreate views for Spark SQL
        // citcleaned.createOrReplaceTempView("citations")
        // pdcleaned2.createOrReplaceTempView("pdates")

        spark.stop()
    }
}