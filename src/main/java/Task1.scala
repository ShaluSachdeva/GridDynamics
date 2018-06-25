import java.sql.Date

import org.apache.log4j.{Level,Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, DataFrame, SQLContext, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType,StructField,LongType,IntegerType,DateType}



object Task1 extends App{
   Logger.getLogger("org").setLevel(Level.ERROR)
   

  val session = SparkSession.builder.appName("one").master("local")getOrCreate()
  
  
  
  val sqlContext = session.sqlContext

  
  val schema =
       StructType(
         StructField("user_id", LongType, false) ::
        StructField("date", DateType, false) :: 
        StructField("score", IntegerType, false)::Nil)
  
  
 val inputDf =  session.sparkContext.textFile("data/UserScore.txt").
  map { x => x.split(",")}.map { p => Row(p(0).toLong,Date.valueOf(p(1)),p(2).trim.toInt )}
  
  var df = sqlContext.createDataFrame(inputDf, schema)
  
  def markMostRecent(dataframe: DataFrame): DataFrame ={
    var inputData: DataFrame = dataframe
 
	  val status = udf { (stts: String) => "most_recent" }
	 

			  var updateddf = inputData.groupBy("user_id").agg("date" -> "max").
			  withColumn("status",status(inputData("user_id"))).
			  toDF("user_id","date","status")
			  // updateddf.show


			  session.sparkContext.broadcast(updateddf)  


			  inputData = inputData.join(updateddf, Seq("user_id","date"), "left_outer")
			  updateddf = inputData.filter(inputData("status")<=>"most_recent").
			  groupBy("user_id","date","status").
			  max("score").
			  toDF("user_id","date","status","score")

			  var markedDf = inputData.drop("status").
			  join(updateddf, Seq("user_id","date","score"), "left_outer").na.fill("")

        markedDf  
  }                     

var markedDf = markMostRecent(df)
markedDf.show
 session.stop
  
}