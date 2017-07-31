/**
  * Created by junlang on 4/25/17.
  */
import java.sql.Date

import org.apache.avro.generic.GenericData.StringType
import org.elasticsearch.spark._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import spark.DateOperations
import spark.Estimation._
import spark.Transformation._

object SimpleApp_ancient {
  def main(args: Array[String]) {
    val csv = "data/TechCrunchcontinentalUSA.csv" // Should be some file on your system
    val json = "data/tech.json"
    val sparkSession = SparkSession.builder.master("local[2]").appName("Simple Application").
      config("es.nodes", "http://localhost").
      config("es.port", "9200").getOrCreate()

    // elastic search
    val es =sparkSession.sparkContext.esRDD("d3f06eb3-794a-47e7-a8fb-fc440976f2c1")
    // elastic search
//    val ds = sparkSession.read.format("org.elasticsearch.spark.sql").option("es.read.field.as.array.include","data\\:achat").load("d3f06eb3-794a-47e7-a8fb-fc440976f2c1")


    val colInfo = sparkSession.read.json(json)
    val df = sparkSession.read.options(Map(
      "header" -> "true"
    )).csv(csv)
    val columns = df.columns
    // Pipeline for transforming types
    val typeTransformers: Array[PipelineStage] = columns.map(col => new TypeTransformer()
      .setInputCol(col).setOutputCol(s"${col}_").setColInformation(colInfo.select(col)))
    val typeTransformPipeline = new Pipeline().setStages(typeTransformers)
    val model = typeTransformPipeline.fit(df).transform(df)

    // bucketize dates into proper buckets
    val dfDate = dateBucketize(model.columns.filter(_ matches ".*_date$"), model)

    // Pipeline for StringIndexer
    // stringIndex
    val qualitativesToIndex = model.columns.filter(x => x matches ".*_quali$")
    val quantitativesToIndex = model.columns.filter(x => x matches ".*_quanti$")
    val dfToIndex= dfDate.na.fill("null", qualitativesToIndex).na.fill(Double.NaN, quantitativesToIndex)
    val stringIndexers: Array[PipelineStage] = qualitativesToIndex.map(col =>
        new StringIndexer().setInputCol(col).setOutputCol(s"${col}_index").setHandleInvalid("skip"))
    val stringIndexPipeline = new Pipeline().setStages(stringIndexers)
    val model2 = stringIndexPipeline.fit(dfToIndex).transform(dfToIndex)

    // loop over the columns...
    // all columns gonna use
    val columnsIndexed = model2.columns.filter(_ matches ".*((_quali_index$)|(_quanti$))")
    // ArrayBuffer to store the results
    val resBuf = scala.collection.mutable.Map[String, Array[(String, Double)]]()
    for (col <- columnsIndexed) {
      val modelAss = assembleFeatures(model2, columnsIndexed, col)
      val rf = if (col matches ".*quali_index") FeaturesRelationsRFClassifier() else FeaturesRelationsRFRegressor()
      val featureImportance = rf.featureRelations(modelAss, col)
      resBuf += (col -> columnsIndexed.filter(_ != col).zip(featureImportance).sortBy(t => t._2))
    }
    resBuf.foreach(t => {
      print(t._1 + ", ")
      t._2.reverse.array foreach print
      println()
    })

    // assemble the features into a vector
//    val dfIndexed = model2.select(columnsIndexed.head, columnsIndexed.tail: _*)
//    val assembler = new VectorAssembler().setInputCols(columnsIndexed.filter(_ != "achat_quali_index")).setOutputCol("features")
//    val modelAss = assembler.transform(dfIndexed).select("features", "achat_quali_index")
//
//    val ff = FeaturesRelationsRFClassifier()
//    val rfmodel = ff.featureRelations(modelAss, "achat_quali_index")


//    val frc = new RandomForestClassifier() with FeatureImportancesClassification
//    val res = frc.calculateFeatureRelations(df)
    sparkSession.stop()
  }

  def dateBucketize(arr: Array[String], df: DataFrame): DataFrame = {
    import spark.DateOperations._
    if (!arr.isEmpty) {
      val c = arr.head
      val maxDate = new DateTime(df.select(max(c)).head.getDate(0))
      val minDate = new DateTime(df.select(min(c)).head.getDate(0))
      val years = getYears(minDate, maxDate)
      if (years > spark.Estimation.RandomForestParams.maxBins) {
        val interval = Math.ceil(years * 1.0 / spark.Estimation.RandomForestParams.maxBins)
        def newCol = udf{(d: Date) => {
          val dt = new DateTime(d)
          Math.floor((dt.getYear - minDate.getYear) / interval)
        }}
        val newdf = df.withColumn(c + "_year_quali", newCol(col(c)))
        dateBucketize(arr.drop(1), newdf)
      }
      else if (years > 2) {
        // use year
        def toYear: Date => Int = new DateTime(_).getYear
        val newCol = udf(toYear)
        val newdf = df.withColumn(c + "_year_quanti", newCol(col(c)))
        dateBucketize(arr.drop(1), newdf)
      }
      else if (getMonths(minDate, maxDate) > 2) {
        val newCol = udf {(d: Date) => {
          val dt = new DateTime(d)
          dt.getYear + "-" + dt.getMonthOfYear
        }}
        val newdf = df.withColumn(c + "_month_quali", newCol(col(c)))
        dateBucketize(arr.drop(1), newdf)
      }
      else if (getWeeks(minDate, maxDate) >= 4) {
        val newCol = udf {(d: Date) => {
          val dt = new DateTime(d)
          dt.getWeekOfWeekyear
        }}
        val newdf = df.withColumn(c + "_week_quanti", newCol(col(c)))
        dateBucketize(arr.drop(1), newdf)
      }
//      (getDays(minDate, maxDate) >= 6)
      else {
        val newCol = udf {(d: Date) => {
          val dt = new DateTime(d)
          dt.getDayOfYear
        }}
        val newdf = df.withColumn(c + "_day_quanti", newCol(col(c)))
        dateBucketize(arr.drop(1), newdf)
      }
    }
    else df
  }

  def assembleFeatures(df: DataFrame, cols: Array[String], label: String): DataFrame = {
    val dfIndexed = df.select(cols.head, cols.tail: _*)
    new VectorAssembler().setInputCols(cols.filter(_ != label)).setOutputCol("features")
      .transform(dfIndexed).select("features", label)
  }


}
