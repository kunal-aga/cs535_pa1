import org.apache.spark.sql.SparkSession

object Citations1 {

    def main(args: Array[String]): Unit = {
        
        // val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
        // val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
        // val logData = spark.read.textFile(logFile).cache()
        // val numAs = logData.filter(line => line.contains("a")).count()
        // val numBs = logData.filter(line => line.contains("b")).count()
        // println(s"Lines with a: $numAs, Lines with b: $numBs")
        // spark.stop()
 
        val spark = SparkSession.builder.appName("Citations1").getOrCreate()
        val cit = spark.read.textFile("hdfs:///pa1/citations.txt")

        val countOg = cit.count()
        println(s"Original lines count: $countOg")

        // val filteredCit = cit.filter(!"value".contains("#"))
        // val countFil = cit.count()
        // println(s"Filtered lines count: $countFil")

        cit.printSchema()

        // val countFil = cit.filter(line => line.contains("#")).count()

        spark.stop()

    }
}