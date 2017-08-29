package spark.Transformation

/**
  * Created by junlang on 7/20/17.
  */

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class ArrayToString extends Transformer {
  override val uid: String = Identifiable.randomUID("arrayToString")
  final val inputCol= new Param[String](this, "inputCol", "The input column")
  //  final val outputCol = new Param[String](this, "outputCol", "The output column")
  val values = new Param[Array[String]](this, "values", "distinct values")

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    //    val outputColName = $(outputCol)
    val i = $(inputCol)
    val outCol: StructField = inputType match {
      case ArrayType(StringType, true) =>

        val json = """{"origin": """" + i + """"}"""
        StructField(i + "-str", StringType, true, Metadata.fromJson(json))
      case _ => throw new IllegalArgumentException(s"Data type $inputType is not supported.")
    }

    val commons = schema.fieldNames.contains(i + "-str")
    if (commons) {
      throw new IllegalArgumentException(s"Output columns $i-str already exists.")
    }
    StructType(schema.fields :+ outCol)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val schema = dataset.schema
    val inputType = schema($(inputCol)).dataType
    val mapTerms = udf { (arr: scala.collection.mutable.WrappedArray[String]) => {
      if (arr.length < 1) {
        "others"
      }
      else {
        val term = arr(0)
        if ($(values).contains(term)) {
          term
        }
        else {
          "others"
        }
      }

    }}
    val df = dataset.withColumn($(inputCol) + "-str", mapTerms(col($(inputCol))))
    df
  }

  def setInputCol(value: String): this.type = set(inputCol, value)
  def getInputCol: String = $(inputCol)

  def setValues(value: Array[String]): this.type = set(values, value)
  def getOutputCol: String = $(inputCol) + "-str"

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)


}
