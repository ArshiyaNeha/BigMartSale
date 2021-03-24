import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object TrainDataset extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Dataset")

  // Creating Spark session.Spark session is a unified entry point of a spark application.
  val spark = SparkSession
    .builder
    .master("Local[*]")
    .appName("Test")
    .getOrCreate()

  import spark.implicits._

  val lines = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:/Users/DELL/Desktop/Dataset/Train-1.csv")

  lines.show(2)

  println("First one")
/*
  1.Which products are being sold most.
  So, that seller will concentrate more on improving the quality of products.
 */
  lines.groupBy("Item_Type").count().orderBy($"count".desc).show(8)

  println("second one")
  /*
  2.Which products are being sold less.
  So, they will be able give discounts on products to improve their business
   */
  lines.groupBy("Item_Type").count().orderBy($"count".asc).show(5)

  println("Fourth one")
 /*
  4.	Which age group of customers interested in which product.
  Based on this analysis, sellers will advertise products to that age group using channels like social media.
 */
  val interval = 30
  val new1 = lines.withColumn("age group", $"age" - ($"age" % interval))
    .withColumn("age group", concat($"age group", lit(" - "), $"age group" + interval))
    //.groupBy($"age group")
    //.agg(collect_list($"Item_Type").as("Item_Type"))
    //.show(false)
  val new2 = new1.groupBy("age group","Item_Type").count().orderBy($"age group".asc,$"count".desc)
  val windowp = Window.partitionBy("age group").orderBy($"age group".asc,$"count".desc)
  val result = new2.withColumn("index",row_number().over(windowp)).show()

  println("Fifth one")
  //5.Total earnings of each outlet type (ex: supermarket, grocery store).
  lines.groupBy("Outlet_Type").agg(round(sum("Item_Outlet_Sales"),2)).show(4)

  println("sixth one")
  //6.Which gender is interested in which product.
  val lines1 = lines.groupBy("Gender","Item_Type").count().orderBy($"count".desc)
  val toptwo = Window.partitionBy("Gender").orderBy($"Gender".asc)
  val aggdf = lines1.withColumn("row",row_number.over(toptwo))
  aggdf.show()
  //aggdf.filter("gender == 'M'").show(false)
  aggdf.filter("row == '1'").show(false)
  aggdf.filter("row == '2'").show(false)

  //Reading the length of dataset
  val TrainData = spark.read
    .option("header", "false")
    .option("inferSchema", "true")
    .csv("C:/Users/DELL/Desktop/Dataset/Train-1.csv")

  val lengthCheck = TrainData.count()
  println("Length of the TrainData set is :" + lengthCheck)

  //counting the number of males and females in dataset.
  lines.groupBy("Gender").count().show()

}
