package spark

// library
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{ArrayType, NumericType, StringType, TimestampType}
import spark.Transformation.{ArrayToString, ArrayToStrings, DateToLong, NullValuesHandler}
import DataLoadFunctions._
import elastic4s.Aggregation
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions._

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

    //
    val aggregations = new Aggregation("localhost", 9200)
    // map(qualiCol -> (term -> count))
    val termsAggs = aggregations.getTermsAggregation(db_id, qualiCols)

    // clean cols
    val columns = cleanCols(colInfo)
    val df = rawDf.select(columns.head, columns.tail: _*).select("data-achat", "data-likefacebook", "data-cartefidelite", "data-sexe", "data-pointdevente","data-dejaachete", "data-age", "data-enfants")

    val colsWithTypes = df.columns.map(col => (col, df.schema(col).dataType))

    // clean null values
    val nullValuesHandler = new NullValuesHandler()

    // StringArray => each term becomes a column, and the value is boolean type
    //    val multiQualiCols = colsWithTypes.filter(col => getIsMulti(colInfo, col._1).getOrElse(false))
    val arrayQualiCols = colsWithTypes.map(col => {
      (col._1, getIsMulti(colInfo, col._1))
    })
    val arraytoStringss = arrayQualiCols.filter(x => x._2.isDefined && x._2.get) map (col => {
      val values = termsAggs(col._1).map(_._1)
      new ArrayToStrings().setInputCol(col._1).setValues(values.toArray)
    })
    // StringArray => Strings in a column
    val arraytoStrings = arrayQualiCols.filter(!_._2.get) map (col => {
      val values = termsAggs(col._1).map(_._1)
      new ArrayToString().setInputCol(col._1).setValues(values.toArray)
    })

    // Timestamp => seconds in Int
    val dateCols = colsWithTypes.filter(_._2 == TimestampType)
    val dateToIntTransformers: Array[DateToLong] = dateCols map (col =>
      new DateToLong().setInputCol(col._1).setOutputCol(s"${col._1}_int_index"))

    // StringIndexer: String to index
    val stringCols = colsWithTypes.filter(_._2 == StringType) ++ arraytoStrings.map(x => (x.getOutputCol, StringType))
    val stringIndexers: Array[StringIndexer] = stringCols map (col =>
      new StringIndexer().setInputCol(col._1).setOutputCol(s"${col._1}-index").setHandleInvalid("skip"))

    // one hot encoder
    val oneHotEncoders:Array[OneHotEncoder] = stringIndexers map (indexers =>
      new OneHotEncoder().setInputCol(indexers.getOutputCol).setDropLast(false))

    // bucketize numerics to compute the significance
    val quantiCols = colsWithTypes.filter(x => x._2 match {
      case i: NumericType => true
      case _ => false
    }).map(_._1)
    val bucketizers1 = quantiCols map (col => {
      val max = getMax(colInfo, col)
      val min = getMin(colInfo, col)
      if (!(max.isDefined && min.isDefined)) throw new Exception(s"${col}:max or min value is not defined")
      val minv: Double = min.get
      val maxv: Double = max.get
      val splits = minv to maxv by (maxv - minv) / 10
      val intervals = splits.updated(0, Double.NegativeInfinity).updated(splits.length - 1, Double.PositiveInfinity)
      new Bucketizer().setInputCol(col).setOutputCol(col + "-bucketized").setSplits(intervals.toArray)
    })
    val bucketizers2 = dateToIntTransformers map (t => {
      val col = t.getInputCol
      val max = getMax(colInfo, col)
      val min = getMin(colInfo, col)
      if (!(max.isDefined && min.isDefined)) throw new Exception(s"${col}:max or min value is not defined")
      val minv: Double = min.get
      val maxv: Double = max.get
      val splits = minv to maxv by (maxv - minv) / 10
      val intervals = splits.updated(0, Double.NegativeInfinity).updated(splits.length - 1, Double.PositiveInfinity)
      val out = t.getOutputCol
      new Bucketizer().setInputCol(out).setOutputCol(out + "-bucketized").setSplits(intervals.toArray)
    })
    val bucketizers = bucketizers1 ++ bucketizers2

    // assembler the columns in vector

    // multi-valued features vector

    val multiQualiFeatures = if (!arraytoStringss.isEmpty){
      Some(new VectorAssembler().setInputCols(arraytoStringss.flatMap(_.getOutputCols)).setOutputCol("multiQualiFeatures"))
    } else {
      None
    }
    // numericFeatures vector to be MaxMinScaler
    val numericCols = quantiCols ++ dateToIntTransformers.map(_.getOutputCol)
    val numericFeatures = if (numericCols.nonEmpty) {
      Some(new VectorAssembler().setInputCols(numericCols).setOutputCol("numericFeatures"))
    } else {
      None
    }
    val scaler = if (numericFeatures.nonEmpty) {
      Some(new MinMaxScaler().setInputCol(numericFeatures.get.getOutputCol).setOutputCol("scaledNumericFeatures"))
    } else {
      None
    }

//    val singleFeatures = new VectorAssembler().setInputCols(quantiCols ++ dateToIntTransformers.map(_.getOutputCol) ++ bucketizers.map(_.getOutputCol) ++
//      stringIndexers.map(_.getOutputCol)).setOutputCol("singleFeatures")
//    val features = new VectorAssembler().setInputCols(Array(multiQualiFeatures.getOutputCol, singleFeaures.getOutputCol))
//      .setOutputCol("features")
    val features = scaler match {
  case Some(i) => new VectorAssembler().setInputCols(oneHotEncoders.map(_.getOutputCol) :+ i.getOutputCol)
  case None => new VectorAssembler().setInputCols(oneHotEncoders.map(_.getOutputCol))
    }

    val stages = Array(nullValuesHandler) ++
      arraytoStringss ++
      arraytoStrings ++
      dateToIntTransformers ++
      stringIndexers ++
      oneHotEncoders ++
      bucketizers ++
      Array(multiQualiFeatures,
      numericFeatures,
      scaler).filter(_.nonEmpty).map(_.get) :+ features
    val pipeline = new Pipeline().setStages(stages)

    val initialDataFrame = pipeline.fit(df).transform(df)
    val inputDF = initialDataFrame.select(features.getOutputCol, stringIndexers.map(_.getOutputCol) ++ bucketizers.map(_.getOutputCol):_*)

    // need to switch to map(qualiCol -> (term_index -> count))
    // first, get stringIndexers' metadata
    val idexMetadata = stringIndexers.map(ele => {
      val output = ele.getOutputCol
      (output, initialDataFrame.schema(output).metadata.getMetadata("ml_attr").
               getStringArray("vals").zipWithIndex.toMap)
    }).toMap
//    val co_occurences = termsAggs.map {
//      case (k, v) =>
//        val idexes = idexMetadata(k + "-str-index")
//
//        val values = idexes.map{
//          case (term, index) => {
//            val res = v.find(ele => ele._1 == term)
//            if (res.nonEmpty) {
//              (index.toDouble, res.get._2)
//            }
//            else {
//              val count = initialDataFrame.filter(col(k + "-str") === term).count()
//              (index.toDouble, count)
//            }
//        }}
//        k + "-str-index" -> values.toArray
//    }
    // or
    val occurrences = getTermsAggregations(initialDataFrame, stringIndexers.map(_.getOutputCol))

    val kmfdm = new KMeansForMixedData().setFeaturesCol(features.getOutputCol).
      setInputQualitativeCols(stringIndexers.map(_.getOutputCol)).
      setInputQuantitativeCols(bucketizers.map(_.getOutputCol)).setOccurrences(occurrences)

    val (coo, sig) = kmfdm.coOccurrencesAndSignificances(inputDF)

  }

  def getTermsAggregations(df: Dataset[_], qualiCols: Array[String]) = {
    qualiCols.map(col => {
      val values = df.select(col).rdd.map(row =>
        (row.getDouble(0), 1)).
      reduceByKey(_ + _).
      collect().map(value => (value._1, value._2.toLong))
      col -> values
    }).toMap
  }

}
