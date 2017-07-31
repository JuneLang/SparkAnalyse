package spark

// library
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{ArrayType, NumericType, StringType, TimestampType}
import spark.Transformation.{ArrayToString, ArrayToStrings, DateToInt, NullValuesHandler}
import DataLoadFunctions._
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.feature.{Bucketizer, MinMaxScaler, StringIndexer, VectorAssembler}

import scala.collection.mutable

// modules
import DataLoadFunctions._
/**
  * Created by junlang on 7/7/17.
  */

object SimpleApp{
  def main(args: Array[String]): Unit = {
    // meta data from JS telling information about columns
    val json = "data/columnInformations.json"
    val db_id = "dc66431c-3a11-40fc-bf68-57dc19baf30c"
    // create sparkSession, setting ES config
    val sparkSession = SparkSession.builder.master("local[2]").appName("Simple Application").
      config("es.nodes", "http://localhost").
      config("es.port", "9200").getOrCreate()

    // read from ES, specify which column/field are arrays
    val colInfo = sparkSession.read.json(json)
    val qualiCols = getQualiCols(colInfo)
    val rawDf = sparkSession.read.format("org.elasticsearch.spark.sql")
      .option("es.read.field.as.array.include", qualiCols mkString ",").load(db_id)

    // clean cols
    val columns = cleanCols(colInfo)
    val df = rawDf.select(columns.head, columns.tail: _*).select("data-achat", "data-likefacebook", "data-cartefidelite", "data-age")

    val colsWithTypes = df.columns.map(col => (col, df.schema(col).dataType))

    // clean null values
    val nullValuesHandler = new NullValuesHandler()

    // StringArray => each term becomes a column, and the value is boolean type
    //    val multiQualiCols = colsWithTypes.filter(col => getIsMulti(colInfo, col._1).getOrElse(false))
    val arrayQualiCols = colsWithTypes.map(col => (col._1, col._2, getIsMulti(colInfo, col._1))).filter(_._3.isDefined)
    val arraytoStringss = arrayQualiCols.filter(_._3.get) map (col => {
      val values = getColValues(colInfo, col._1) match {
        case Some(arr) => arr.toArray
        case None => throw new Exception("Can't find any value in this column.")
      }
      new ArrayToStrings().setInputCol(col._1).setValues(values)
    })
    // StringArray => Strings in a column
    val arraytoStrings = arrayQualiCols.filter(!_._3.get) map (col => {
      val values = getColValues(colInfo, col._1) match {
        case Some(arr) => arr.toArray
        case None => throw new Exception("Can't find any value in this column.")
      }
      new ArrayToString().setInputCol(col._1).setValues(values)
    })

    // Timestamp => seconds in Int
    val dateCols = colsWithTypes.filter(_._2 == TimestampType)
    val dateToIntTransformers: Array[DateToInt] = dateCols map (col =>
      new DateToInt().setInputCol(col._1).setOutputCol(s"${col._1}_int_index"))

    // StringIndexer: String to index
    val stringCols = colsWithTypes.filter(_._2 == StringType) ++ arraytoStrings.map(x => (x.getOutputCol, StringType))
    val stringIndexers: Array[StringIndexer] = stringCols map (col =>
      new StringIndexer().setInputCol(col._1).setOutputCol(s"${col._1}-index").setHandleInvalid("skip"))

    // bucketize numerics to compute the significance
    val quantiCols = colsWithTypes.filter(x => x._2 match {
      case i: NumericType => true
      case _ => false
    }).map(_._1) ++ dateToIntTransformers.map(_.getOutputCol)
    val bucketizers = quantiCols map (col => {
      val max = getMax(colInfo, col)
      val min = getMin(colInfo, col)
      if (!(max.isDefined && min.isDefined)) throw new Exception("max or min value is not defined")
      val minv: Double = min.get
      val maxv: Double = max.get
      val splits = minv to maxv by (maxv - minv) / 10

      new Bucketizer().setInputCol(col).setOutputCol(col + "-bucketized").setSplits(splits.toArray)
    })


    // assembler the columns in vector

    // multi-valued features vector
    val multiQualiFeatures = new VectorAssembler().setInputCols(arraytoStringss.flatMap(_.getOutputCols))
      .setOutputCol("multiQualiFeatures")

    // numericFeatures vector to be MaxMinScaler
    val numericFeatures = new VectorAssembler().setInputCols(quantiCols).setOutputCol("numericFeatures")
    val scaler = new MinMaxScaler().setInputCol(numericFeatures.getOutputCol).setOutputCol("scaledNumericFeatures")

//    val singleFeatures = new VectorAssembler().setInputCols(quantiCols ++ dateToIntTransformers.map(_.getOutputCol) ++ bucketizers.map(_.getOutputCol) ++
//      stringIndexers.map(_.getOutputCol)).setOutputCol("singleFeatures")
//    val features = new VectorAssembler().setInputCols(Array(multiQualiFeatures.getOutputCol, singleFeaures.getOutputCol))
//      .setOutputCol("features")
    val features = new VectorAssembler().setInputCols(stringIndexers.map(_.getOutputCol) :+ scaler.getOutputCol)

    val pipeline = new Pipeline().setStages(Array(nullValuesHandler) ++
      arraytoStringss ++
      arraytoStrings ++
      dateToIntTransformers ++
      stringIndexers ++
      bucketizers :+
      multiQualiFeatures :+
      numericFeatures :+
      scaler :+ features)

    val initialDataFrame = pipeline.fit(df).transform(df)
    val inputDF = initialDataFrame.select(features.getOutputCol, stringIndexers.map(_.getOutputCol) ++ bucketizers.map(_.getOutputCol):_*)

  }


}
