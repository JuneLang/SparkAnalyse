package spark.Transformation


import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.annotation.tailrec

/**
  * Created by junlang on 5/18/17.
  */
class NullValuesHandler extends Transformer{
  override val uid: String = Identifiable.randomUID("nullValuesHandler")
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    // no check here
    schema
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
//    dataset.columns.map(col => (col, count(dataset(col).isNotNull). / count(dataset(col)))
    transformSchema(dataset.schema)
    //get counts of each column
    val notNullCounts = dataset.columns.map(col =>
      dataset.select(count(dataset(col))).head().getLong(0))
//    val max = notNullCounts.max
    val c = dataset.count()
    val notNullPercentage = notNullCounts.map(v => 1.0 * v / c)
    val selectedColumns = dataset.columns.zip(notNullPercentage)
    recursiveHandler(selectedColumns, dataset.toDF())
  }

  @tailrec
  final def recursiveHandler(cols: Array[(String, Double)], dataset: DataFrame): DataFrame = {
    if(cols.length > 0) {
      val col = cols.head
      if (col._2 < 0.5) {
        val df = dataset.drop(col._1)
        recursiveHandler(cols.drop(1), df)
      }
      else if (col._2 >= 0.5 && col._2 < 0.8) {
        dataset.schema(col._1).dataType match {
          case StringType =>
            val df = dataset.na.fill("null", Array(col._1))
            recursiveHandler(cols.drop(1), df)
          case i : NumericType =>
            val df = dataset.na.fill(2809, Array(col._1))
            recursiveHandler(cols.drop(1), df)
//          case ArrayType(StringType, true) =>
//            val df = dataset.na.fill(Array.empty[String], Array(col._1))
        }
      }
      else {
        val df = dataset.na.drop(Array(col._1))
        recursiveHandler(cols.drop(1), df)
      }
    }
    else dataset
  }
}
