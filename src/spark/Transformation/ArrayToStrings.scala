package spark.Transformation

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
  * Created by junlang on 7/11/17.
  */
class ArrayToStrings extends Transformer {
  override val uid: String = Identifiable.randomUID("arrayToStrings")
  final val inputCol= new Param[String](this, "inputCol", "The input column")
//  final val outputCol = new Param[String](this, "outputCol", "The output column")
  val values = new Param[Array[String]](this, "values", "distinct values")

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
//    val outputColName = $(outputCol)

    val outCols: Array[StructField] = inputType match {
      case ArrayType(StringType, true) =>
        $(values) map (term => {
          val i = $(inputCol)
          val json = """{"origin": """" + i + """"}"""
          StructField(i + "-" + term, BooleanType, true, Metadata.fromJson(json))
        }
        )
      case _ => throw new IllegalArgumentException(s"Data type $inputType is not supported.")
    }

    val commons = schema.fieldNames.intersect($(values))
    if (commons.length > 0) {
      throw new IllegalArgumentException(s"Output columns $commons already exists.")
    }
    StructType(schema.fields ++ outCols)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val schema = dataset.schema
    val inputType = schema($(inputCol)).dataType
    val df = mapTerms($(values), dataset.toDF())
    df
  }

  def setInputCol(value: String): this.type = set(inputCol, value)
  def getInputCol: String = $(inputCol)

  def setValues(value: Array[String]): this.type = set(values, value)
  def getOutputCols: Array[String] = $(values) map (x => $(inputCol) + "-" + x)

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  def mapTerms(arr: Array[String], dataset: DataFrame): DataFrame = {
    if (arr.length == 0) dataset
    else {
      val term = arr.head
      val addNewCol = udf {(terms: scala.collection.mutable.WrappedArray[String]) => {
        if (terms.contains(term)) true else false
      }}
      val df = dataset.withColumn($(inputCol) + "-" + term, addNewCol(col($(inputCol))).cast(BooleanType))
      mapTerms(arr.drop(1), df)
    }

  }



}
