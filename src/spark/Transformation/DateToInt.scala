/**
  * Created by junlang on 7/7/17.
  */

package spark.Transformation

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}


class DateToInt extends Transformer {
  override val uid: String = Identifiable.randomUID("dateStringifier")
  final val inputCol= new Param[String](this, "inputCol", "The input column")
  final val outputCol = new Param[String](this, "outputCol", "The output column")
//  final val format = new Param[String](this, "format", "The date format")


//  def setFormat(value: String): this.type = set(format, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val schema = dataset.schema
    val inputType = schema($(inputCol)).dataType
    dataset.withColumn($(outputCol), second(col($(inputCol))).as($(outputCol), outputSchema($(outputCol)).metadata))
  }



  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    val outputColName = $(outputCol)

    val outCol: StructField = inputType match {
      case TimestampType =>
        StructField(outputColName, StringType)
      case _ => throw new IllegalArgumentException(s"Data type $inputType is not supported.")
    }

    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    StructType(schema.fields :+ outCol)
  }

  def setInputCol(value: String): this.type = set(inputCol, value)
  def getInputCol: String = $(inputCol)

  def setOutputCol(value: String): this.type = set(outputCol, value)
  def getOutputCol: String = $(outputCol)

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
