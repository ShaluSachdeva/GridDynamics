import org.apache.spark.sql.types.{LongType,IntegerType,DateType}
import org.apache.spark.sql.types.{StructType,StructField}
import org.apache.log4j.{Level,Logger}
import java.sql.Date
import org.apache.spark.sql.{Row,DataFrame,SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Task2 extends App{
  
    Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession.builder.appName("one").master("local")getOrCreate()  
  val sqlContext = session.sqlContext  
  val schema =
       StructType(
         StructField("user_id", LongType, false) ::
         StructField("date", DateType, false) :: 
         StructField("score", IntegerType, false)::Nil)
        
  
  
  val left =  session.sparkContext.textFile("data/input1.txt").
                                    map { x => x.split(",")}.
                                    map { p => Row(p(0).toLong,Date.valueOf(p(1)),p(2).trim.toInt )}
  
  val right =  session.sparkContext.textFile("data/input2.txt").
                                    map { x => x.split(",")}.
                                    map { p => Row(p(0).toLong,Date.valueOf(p(1)),p(2).trim.toInt )}
  
  var dataset1   = sqlContext.createDataFrame(left, schema)
  var dataset2  = sqlContext.createDataFrame(right, schema)
 
  def findDelta(dataset1: DataFrame, dataset2: DataFrame, primaryKey: Seq[String]): DataFrame ={
     
  var leftDf: DataFrame = dataset1
  var rightDf: DataFrame = dataset2
     
  //Deleted and Updated
  val deleted   =  udf { (stts: String) => "Deleted" }
  var deletedDf =  leftDf.except(rightDf)
      deletedDf =  deletedDf.withColumn("delta", deleted(deletedDf("user_id")))
      session.sparkContext.broadcast(deletedDf)  
  //insert
  var insertededDf = rightDf.join(leftDf,primaryKey.take(2),"left_anti")
 
  val insert = udf { (stts: String) => "Inserted" }
  var insertededDf1 =  insertededDf.withColumn("delta", insert(insertededDf("user_id")))
      session.sparkContext.broadcast(insertededDf1)  
  //update
 var updatedDf =  rightDf.join(leftDf,primaryKey,"left_anti")
 val update = udf { (stts: String) => "Updated" }
     session.sparkContext.broadcast(updatedDf)  
     updatedDf = updatedDf.except(insertededDf).withColumn("delta",update(updatedDf("user_id")))
 
     insertededDf1.union(updatedDf).union(deletedDf)
 
  } 
   var deltaDf = findDelta(dataset1,dataset2,schema.fieldNames.toSeq)
  deltaDf.show
  //rightDf.intersect(leftDf).show
  session.stop
  
}