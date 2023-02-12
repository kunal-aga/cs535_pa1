import org.apache.spark.sql.SparkSession

object Citations1 {

    def main(args: Array[String]): Unit = {
 
        val spark = SparkSession.builder.appName("Citations1").getOrCreate()
        import spark.implicits._
        
        // Read from HDFS
        val cit = spark.read.textFile("hdfs:///pa1/citations.txt")
        cit.printSchema()

        val countOg = cit.count()
        println(s"Original lines count: $countOg")

        val cit = cit.filter(!$"value".contains("#"))
        val countFil = cit.count()
        println(s"Filtered lines count: $countFil")

        val cit = cit.withColumn("fromnode", split(col("value"), "\t").getItem(0).cast("int"))
            .withColumn("tonode", split(col("value"), "\t").getItem(1).cast("int"))

        cit.printSchema()
        
        spark.stop()

    }
}