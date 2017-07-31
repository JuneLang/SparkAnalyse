package spark.Estimation

import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.Dataset

/**
  * Created by junlang on 6/15/17.
  */

trait Strategy {
//  type FeatureRelations[T] = (Dataset[_], Estimator[T]) => Array[Double]
  type T <: this.type
  def calculateFeatureRelations(ds: Dataset[_])(implicit strategy: ((Dataset[_], T) => Array[Double])): Array[Double]
}

trait FeatureImportancesStrategy extends Strategy {

}

object FeatureImportancesStrategy {
  implicit def featureImportances(ds: Dataset[_]): Array[Double] = {
    Array.emptyDoubleArray
  }
}

trait FeatureImportancesClassification extends FeatureImportancesStrategy {
  self: RandomForestClassifier =>
  override type T = this.type
  override def calculateFeatureRelations(ds: Dataset[_])(implicit strategy: ((Dataset[_], T) => Array[Double])): Array[Double] = {

    Array.emptyDoubleArray
  }
//  val fi = calculateFeatureRelations(_)
}

//object FeatureImportancesClassification {
//
//  implicit val featureImportance:((Dataset[_], T) => Array[Double])) = featureImportances
//  def featureImportances(ds: Dataset[_]): Array[Double] = {
//    println("fiiiiiiii")
//    Array.emptyDoubleArray
//  }
//}

trait Algorithm
trait RFClassifier extends Algorithm {

}