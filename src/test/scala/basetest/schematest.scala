package basetest
import sparkproject.base.schema
import org.apache.spark.sql.SparkSession
import sparkproject.base.sparksession
import sparkproject.functions.classic
import sparkproject.functions.specefic
import org.scalatest.matchers.should.Matchers


import org.scalatest.flatspec.AnyFlatSpec

object utili{
val a : Seq[schema] = Seq(schema(
  Some("MemeID"),
  Some("ArchivedUrl"),
  Some("BaseMemeName"),
  Some("MemePageUrl"),
  Some("MD5Hash")
))}

class schematest extends AnyFlatSpec with sparksession with classic with Matchers {
  "dataframe " should "read the right schema" in {
    //given
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(utili.a).toDF()
    //when
    val listeschema = rdd.schema.toList.map(x=>x.name)
    val schema = classOf[schema].getDeclaredFields.map(x=>x.getName).toList
    //then
    listeschema should be (schema)
      }

  "dataframe extraction" should "extract the right file name " in {
    //given
    import spark.implicits._
    val trait1 = new specefic {}
    val liste = List("https://www.tutorialspoint.com/scala/scala_options.htm").toDF("colonne1")
    //when
    val df = trait1.extractfromurl(liste,"col1","colonne1")
    //then
    val listea = df.select("colonne").map(x=>x.getString(0)).collect().toList
    listea(0) should be("scala_options.htm")
  }




}
