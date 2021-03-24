import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession


object TestDataset extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Dataset")

//  val lines = sc.textFile("C:/Users/DELL/Desktop/Dataset/Test-1.csv")
//  lines.collect.foreach(println)

  val spark = SparkSession
    .builder
    .master("Local[*]")
    .appName("Test")
    .getOrCreate()

//
//  val lines_df = spark.createDataFrame(lines).toDF()
//  Lines_df.show()

  val lines_df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:/Users/DELL/Desktop/Dataset/Test-1.csv")

  lines_df.show(4)


  //Reading the length of dataset
  val TestData = spark.read
    .option("header", "false")
    .option("inferSchema", "true")
    .csv("C:/Users/DELL/Desktop/Dataset/Test-1.csv")

  val lengthCheck = TestData.count()
  println("Length of the TestData set is : " + lengthCheck)


}
