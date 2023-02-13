import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Citations1 {

    def main(args: Array[String]): Unit = {
 
        val spark = SparkSession.builder.appName("Citations1").getOrCreate()
        import spark.implicits._
        
        // Read Citations from HDFS
        var cit = spark.read.textFile("hdfs:///pa1/citations.txt")
        cit.printSchema()
        // filter comments
        val countOg = cit.count()
        println(s"Original lines count: $countOg")
        cit = cit.filter(!$"value".contains("#"))
        val countFil = cit.count()
        println(s"Filtered lines count: $countFil")
        // create columns
        val citcleaned = cit.withColumn("fromnode", split(col("value"), "\t").getItem(0).cast("int"))
            .withColumn("tonode", split(col("value"), "\t").getItem(1).cast("int"))
            .drop("value")
        citcleaned.show()
        citcleaned.printSchema()

        // Read published-dates from HDFS
        var pd = spark.read.textFile("hdfs:///pa1/published-dates.txt")
        pd.printSchema()
        pd = pd.filter(!$"value".contains("#"))
        // create columns
        val pdcleaned = pd.withColumn("nodeid", split(col("value"), "\t").getItem(0).cast("int"))
            .withColumn("pdate", split(col("value"), "\t").getItem(1))
            .drop("value")
        val pdcleaned2 = pdcleaned.withColumn("pyear", split(col("pdate"), "-").getItem(0).cast("int"))
        pdcleaned2.show()
        pdcleaned2.printSchema()



        // cit.createOrReplaceTempView("citations")
        // val query1 = """
        //     SELECT *, SPLIT(value, '\t')[0] as fromnode, SPLIT(value, '\t')[1] as tonode 
        //     FROM citations 
        //     LIMIT 10
        // """;
        // cit = spark.sql(query1)
        // val citcleaned = spark.sql("SELECT *, SPLIT(value, '\t')[0] as fromnode, SPLIT(value, '\t')[1] as tonode FROM citations LIMIT 10")




        spark.stop()

    }
}