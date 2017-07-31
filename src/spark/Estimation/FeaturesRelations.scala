package spark.Estimation

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.{Model, _}
import org.apache.spark.sql.Dataset


trait FeaturesRelationsCalculator[T <: Model[T]] {

  def setDefaultParams(esti: Estimator[T]): Estimator[T]
  def featureRelations(ds: Dataset[_], label: String): Array[Double]
}

case class FeaturesRelationsRFClassifier(rfc: RandomForestClassifier = new RandomForestClassifier())
  extends FeaturesRelationsCalculator[RandomForestClassificationModel] {
//  val cc = setDefaultParams(rfc)

  def getModel(ds: Dataset[_], label: String): RandomForestClassificationModel = {
    val classifier = setDefaultParams(rfc)
    classifier.set(classifier.getParam("labelCol"), label).asInstanceOf[RandomForestClassifier].fit(ds)
  }

  override def featureRelations(ds: Dataset[_], label: String): Array[Double] = {
    val model = getModel(ds, label)
    model.featureImportances.toArray
  }

  override def setDefaultParams(esti: Estimator[RandomForestClassificationModel]):
  Estimator[RandomForestClassificationModel] =
    RandomForestParams setDefaultParams esti.asInstanceOf[RandomForestClassifier]
}


case class FeaturesRelationsRFRegressor(rfr: RandomForestRegressor = new RandomForestRegressor())
  extends FeaturesRelationsCalculator[RandomForestRegressionModel] {
//  override val estimator: Estimator[RandomForestRegressionModel] = rfr

  def getModel(ds: Dataset[_], label: String): RandomForestRegressionModel = {
    val regressor = setDefaultParams(rfr)

    regressor.set(regressor.getParam("labelCol"), label).asInstanceOf[RandomForestRegressor].fit(ds)
  }

  override def featureRelations(ds: Dataset[_], label: String): Array[Double] = {
    val model = getModel(ds, label)
    model.featureImportances.toArray
  }

  override def setDefaultParams(esti: Estimator[RandomForestRegressionModel]):
  Estimator[RandomForestRegressionModel] = {
    RandomForestParams.setDefaultParams(esti.asInstanceOf[RandomForestRegressor])
  }
}


//trait FeatureRelations {
//  self: FeaturesRelationsCalculator =>
//  // necessary functions
//
//  def featureImportances(): Array[Double]
//}

//class FeatureRelationsUsingRFClassifier(ds: Dataset[_]) extends FeaturesRelationsRFClassifier with FeatureRelations {
//
//  def featureImportances(): Array[Double] = {
//    // TODO: set params
//
//    val model = estimator.fit(ds)
//
//    model.featureImportances.toArray
//  }
//
////  override val classifier: RandomForestClassifier = _
////  override val featureRelations: Array[Double] = _
////
////  override def setLabelCol(s: String): Unit = ???
////
////  override def setFeaturesCol(s: String): Unit = ???
//  override val estimator: RandomForestClassifier = new RandomForestClassifier()
//}
