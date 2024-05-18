package sparkproject.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import sparkproject.base.schema
import sparkproject.base.sparksession

trait classic extends sparksession {
  private val logger = LogManager.getLogger(getClass.getName)
  def readDF(FileName: String ="az"): DataFrame = {
    try {
      val df = spark.read.option("Delimiter", ",").option("Header", "True").option("inferSchmea", "True")
        .csv(FileName)
      logger.info("La lecture du fichier CSV s'est déroulée avec succès.")
      df
    } catch {
      case e: Throwable =>
        logger.error(s"Une erreur est survenue lors de la lecture du fichier CSV : ${e.getMessage}")
        spark.emptyDataFrame
    }
    }


  def converttorightschema(df:DataFrame,fileName:String):DataFrame = {
    val schema = classOf[schema].getDeclaredFields.map(x=>x.getName).toList
    val originalschema = readDF(fileName).columns.map(x=>x.replaceAll("\\s","")).toList
    val dftransformed = df.toDF(originalschema:_*)
    val schematoselect = schema.intersect(originalschema)
    val dfRenamed = dftransformed.selectExpr(schematoselect:_*)
    dfRenamed
  }

  def writetoparquet(df:DataFrame,output:String,date:String) = {
     df.write.partitionBy(date).option("compression","snappy")
       .mode("overwrite").parquet(output)
  }


}
