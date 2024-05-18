package sparkproject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import sparkproject.functions.classic
import sparkproject.functions.specefic
import com.typesafe.config.{Config, ConfigFactory}
import sparkproject.base.sparksession
import java.io.File


object demo extends App with classic with specefic with sparksession {
  private val logger = LogManager.getLogger(getClass.getName)
  //val date  = args(0)
  //val config :Config = ConfigFactory.parseFile(new File(args(1)))
  try {
    logger.info("lancement de l'application Spark")
    //val configurations = config.getConfig("conf")
    val spark = initSpark("SparkProject", "local[*]")
    val lu = readDF("memegenerator.csv")
    val luRenamed = converttorightschema(lu,"memegenerator.csv")
    val luRenamedExtractionUrl = extractfromurl(luRenamed,"_tmp","ArchivedUrl")
    val luRenamedExtractionMeme = extractfromurl(luRenamedExtractionUrl,"_tmp","MemePageUrl")
    val finaldf = luRenamedExtractionMeme.withColumn("partitionDate", lit("2023-12"))

    writetoparquet(finaldf,"memegenerator.parquet","partitionDate")
    val userInput = scala.io.StdIn.readLine()

  } catch {
    case e: Exception => logger.error(s"echec du lancement${e.getMessage}")
    }

  def initSpark(appName: String, master: String) = {
    SparkSession.builder().appName(appName).master(master).getOrCreate()
  }



  }
