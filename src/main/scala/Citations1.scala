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

        val filteredCit = cit.filter(!$"value".contains("#"))
        val countFil = filteredCit.count()
        println(s"Filtered lines count: $countFil")

        spark.stop()

    }
}