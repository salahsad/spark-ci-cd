package sparkproject.functions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.logging.log4j.LogManager

import org.apache.spark.sql.functions._



trait specefic {
  private val logger = LogManager.getLogger(getClass.getName)
  def extractfromurl(df:DataFrame,columncreated1:String,columnurl:String):DataFrame={
    val df1 = df.withColumn(columncreated1,split(col(columnurl),"/"))
    val x: Array[String] => String = (a: Array[String]) => if (a!= null ) a.last else null
    //creation d'une edf pour extraire le dernier fichier
    val getLastelementsUDF = udf(x)
    val dfusedon = df1.withColumn(columnurl.slice(0,7),getLastelementsUDF(col(columncreated1))).drop(col(columncreated1)
    ).drop(col(columnurl))
    dfusedon
  }

  def minFileSize(dataFrame: DataFrame,column:String = "FileSize") = {
    val newdf = dataFrame.filter(col(column)>15000)
    newdf
  }

  def searchbyKeyword(dataFrame: DataFrame,keyword :String,sparkSession: SparkSession) = {
    val df = dataFrame.filter(col("AlternateText").contains(keyword) || col("BaseMemeName").contains(keyword))
    df match {
      case a:DataFrame if a.count()==0 =>
        logger.warn("le dataframe est vide rien ne sera affiché")
        sparkSession.emptyDataFrame
      case df:DataFrame if df.count()>0 => df
    }}

    def groupedby(dataFrame: DataFrame) = {
      val df = dataFrame.groupBy(col("BaseMemeName")).count()
      df
    }

    def fusion(dataFrame: DataFrame) = {
      val df1 = dataFrame.select(col("MemeID"),col("BaseMemeName"),col("FileSize")).filter(col("FileSize")>15000)
        .select(col("MemeID"),col("BaseMemeName"))
      val df1updated = df1.withColumn("BaseMemeNameCapitalistic",upper(col("BaseMemeName"))).drop("BaseMemeName")
      val fusion  = dataFrame.join(df1updated,"MemeID","left_outer")
      fusion.withColumn("BaseMemeNameCapitalistic",coalesce(col("BaseMemeNameCapitalistic"),lit("valeur non trouvé")))
    }
  }



