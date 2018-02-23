
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import java.util.Calendar

object big-data-test {  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
        
    println(Calendar.getInstance.getTime)
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // The next three operations are vitually identical to those carried out
    // in the spark-pythion version of this code
    
    // get the initial data set
    val df = spark.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "true").option("delimiter", "|").load("file:///d:/tmp/iholding/issueholding.txt")
    
    // Add a new column that's a copy of the second field
    val newdf = df.withColumn("period", df("_c1"))
    
    // write the data out to files
    newdf.write.partitionBy("period").format("com.databricks.spark.csv").option("header", "false").option("delimiter", "|").mode("overwrite").save("d://tmp/myfiles")

    println(Calendar.getInstance.getTime)
    
    spark.stop()
  }
}
