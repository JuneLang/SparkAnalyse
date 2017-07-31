/**
  * Created by junlang on 5/4/17.
  */

package spark.Transformation

import com.github.nscala_time.time.Imports._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import spark.DataLoadFunctions._

class TypeTransformer extends Transformer{
  override val uid: String = Identifiable.randomUID("typeTransformer")
  final val inputCol = new Param[String](this, "inputCol", "The input column")
  final val outputCol = new Param[String](this, "outputCol", "The output column")
  final val colInformation = new Param[DataFrame](this, "colInformation", "The column information")

  override def transformSchema(schema: StructType): StructType = {
    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
//    if (field.dataType != StringType) {
//      throw new Exception(s"Input type ${field.dataType} did not match input type StringType")
//    }
    // Add the return field
    val outputColName = $(outputCol)
    require(schema.fields.forall(_.name != outputColName), s"Output column $outputColName already exists.")
    val colType = getColType($(colInformation), $(inputCol))
//    colType match {
//      case "string" => schema
//      case _ => {
//        if (field.metadata == Metadata.empty)
//          schema.add(StructField($(outputCol),
//            CatalystSqlParser.parseDataType(colType),
//            true,
//            Metadata.fromJson($(colInformation).select($(inputCol)).toJSON.head())))
//        else
//          schema.add(StructField($(outputCol),CatalystSqlParser.parseDataType(colType),true))
//      }
//    }
    if (field.metadata == Metadata.empty)
      schema.add(StructField($(outputCol),
        CatalystSqlParser.parseDataType(colType.orNull),
        true,
        Metadata.fromJson($(colInformation).select($(inputCol)).toJSON.head())))

    else
      schema.add(StructField($(outputCol),CatalystSqlParser.parseDataType(colType.orNull),true))
  }

  override def transform(dataset: Dataset[_]): Dataset[Row] = {
    val schema = transformSchema(dataset.schema, logging = true)
    val colType = getColType($(colInformation), $(inputCol))
    colType match {
      case Some("string") =>
        val arr = getColValues($(colInformation), $(inputCol))
        arr match {
          case Some(values) =>
            val valueFilter = udf{(s: String) => {
              // get the most frequent elements
              if (values.contains(s)) s else "null"
            }}
            dataset.withColumn($(outputCol) + "quali",
              valueFilter(col($(inputCol))).as($(outputCol), schema($(outputCol)).metadata)).drop($(inputCol))
          case None => dataset.withColumn($(outputCol) + "quali",
            dataset($(inputCol)).as($(outputCol), schema($(outputCol)).metadata)).drop($(inputCol))
        }

      case Some("date") => {
        val dateFormat = getColInfo[String]($(colInformation))($(inputCol))("format")
        dateFormat match {
          case Some(format) => dataset.withColumn($(outputCol) + "date", to_date(from_unixtime(
            unix_timestamp(col($(inputCol)), format), "yyyy-MM-dd"))
            .as($(outputCol), schema($(outputCol)).metadata)).drop($(inputCol))
          case None => throw new Exception("Date format not found.")
        }
      }
      case Some(i) => dataset.withColumn($(outputCol) + "quanti",
        dataset($(inputCol)).cast(i).as($(outputCol), schema($(outputCol)).metadata)).drop($(inputCol))
      case None => throw new Exception(s"Information of ${$(inputCol)} not found")
    }
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  //convert a string to date then get its days
  private def getDays(str: String, format: String): Long = {
    val dateTime = DateTime.parse(str, DateTimeFormat.forPattern(format))
    dateTime.getMillis / (1000 * 60 * 60 * 24)
  }

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setColInformation(infos: DataFrame): this.type = set(colInformation, infos)

  def setOutputCol(value: String): this.type = set(outputCol, value)
}
