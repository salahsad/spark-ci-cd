package sparkproject.base

import org.apache.spark.sql.SparkSession
import sparkproject.demo


trait sparksession {

  implicit val spark = demo.initSpark("SparkProject","local[*]")
}
