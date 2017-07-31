package spark.Estimation
import org.apache.spark.ml.{Estimator, Pipeline}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.clustering.{KMeansModel, KMeans => NewKmeans}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.util.random.XORShiftRandom

import scala.collection.mutable
/**
  * Created by junlang on 7/18/17.
  */
class KMeansForMixedData extends java.io.Serializable { //extends Estimator[KMeansModel]
//  override val uid: String = Identifiable.randomUID("KMeansForMixedData")

//  val multiFeaturesCol: Param[String] = new Param[String](this, "multiFeaturesCol", "the vector of the multi-valued" +
//    " features")
//  val singleFeaturesCOl: Param[String] = new Param[String](this, "singleFeaturesCol", "the vector of the single-valued"+
//    " features")
//  val indices: Param[String] = new Param[String](this, "indices", "indices of each feature in multiFeatureCol")
//  val initialMode: Param[String] = new Param[String](this, "initialMode", "Initial randomly or use K-means||")

//  def run(data:DataFrame): KMeansModel = {
//
//    null
//  }
  val K = 3
  val seed = 2
  val INTERVAL = 10
  def fit(dataset: Dataset[_]): Unit = {
    // Step1: for each qualitative feature and discretized quantitative feautre, calculate co-occurrence of each value-pair.
    // TODO: For now, not considering multi-valued features
//    val data = dataset.select($(singleFeaturesCOl))
    val qualiColumns = dataset.columns.filter(_ matches ".*_index$")
    val qualiColLen = qualiColumns.length
    val bucketizedCols = dataset.columns.filter(_ matches ".*-bucketized$")
    val features = dataset.columns.diff(qualiColumns ++ bucketizedCols)

    val columns = qualiColumns ++ bucketizedCols
    val m = columns.length
    val coOccurrence = Array.fill(columns.length)(mutable.ListMap.empty[(Double, Double), Double])

    for (i <- columns.indices) {
      val coli = columns(i)
      val values = if (qualiColumns.length > i) {
        occurrences(qualiColumns(i))
      } else {
        //df.select(coli).collect().groupBy(row => row.getString(0)).mapValues(_.length).toArray
        dataset.select(coli).groupBy(coli).count().collect().map(row => row.getDouble(0) -> row.getLong(1))
      }

      val coo_i = coOccurrence(i) // mutable.ListMap.empty[(String, String), Double]
      var conditionalProbas = mutable.ListMap.empty[Double, Array[(Double, Double)]] //Array.empty[collection.Map[String, Array[(String, Double)]]]
      // a map likes (vi -> (vj, p_ij))
      values.foreach(v => conditionalProbas += (v._1 -> Array.empty[(Double, Double)]))

      val rest = if (i == 0) {
        columns.drop(i)
      } else {
        columns.take(i) ++ columns.drop(i + 1)
      }
      for(j <- rest.indices) {
        val colj = rest(j)
        val temp = dataset.groupBy(coli, colj).count().rdd
          .map(row => {
            val vi = row.getDouble(0)
            val vj = row.getDouble(1)
            val count = row.getLong(2)
            val p_ij = count.toDouble / values.find(_._1 == vi).get._2.toDouble
            vi -> (vj, p_ij)
          })
        temp.collect().foreach(row => conditionalProbas += (row._1 -> (conditionalProbas(row._1) :+ row._2)))

        // now, we cal calculate the d_ij(x,y) for every x and y in column i
        for (x <- 0 to values.length - 2) {
          val vx = values(x)
          for (y <- x+1 until values.length) {
            val vy = values(y)

            // d_j(x, y)
            val d_jx = conditionalProbas(vx._1)
            d_jx foreach print
            println()
            val d_jy = conditionalProbas(vy._1)
            d_jy foreach print
            println()

            if (d_jx.length != d_jy.length)
              throw new Exception("The length of array `conditionalProbas` should be same")
            var d_jxy = 0.0
            for (k <- d_jx.indices) {
              d_jxy += Math.max(d_jx(k)._2, d_jy(k)._2)
            }
            // add d_j(x,y) to the array of d(x,y), and directly divided by m-1 to avoid another loop.
            coo_i += ((vx._1, vy._1) -> d_jxy / (m - 1))
//            coo_i((vx._1, vy._1)) += d_jxy / (m - 1)
          }
        }
      }
      // now we have all the co-occurrence, last step, sum all the d_j(x,y) and divided by m-1
//      coOccurrence(i) = mutable.ListMap(coo_i.mapValues(d_xy => d_xy / (columns.length - 1)).toArray:_*)
    }

    val ss = dataset.sparkSession
    // the co-occurrence of numeric features is not needed
    val coOccurrenceMatrix = ss.sparkContext.broadcast(coOccurrence.dropRight(bucketizedCols.length))
    println("start:co-occurrence")
    coOccurrenceMatrix.value foreach print
    println("end:co-occurrence")
    // Step 2: for each numeric column, compute the significance using the co-occurrence of numeric features.
    val sigs = coOccurrence.drop(qualiColumns.length).map(colListMap => {
      colListMap.values.sum / (INTERVAL * (INTERVAL - 1) / 2)
    })
    val significances = ss.sparkContext.broadcast(sigs)
    println("start:significance")
    significances.value foreach print
    // TODO Begin clustering...
    // step 3: random initialisation
//    dataset.map

  }

//  override def copy(extra: ParamMap): Estimator[KMeansModel] = ???
//
//  override def transformSchema(schema: StructType): StructType = ???

  /**
    * TODO: return the count of the value
    * @param colName: dd
    * @return
    */
  def occurrences(colName: String): Array[(Double, Long)] = {
    colName match {
      case "data-achat-str_index" => Array(0.0 -> 1362, 1.0 -> 5439, 2.0 -> 8506)
      case "data-likefacebook-str_index" => Array(0.0 -> 13456, 1.0 -> 1842)
      case "data-cartefidelite-str_index" => Array(0.0 -> 12938, 1.0 -> 2369)
    }
  }

//  private def initRandom(dataset: Dataset[_]): Array[Vector] = {
//    dataset.rdd.takeSample(false, K, new XORShiftRandom(this.seed).nextInt())
//  }
}

