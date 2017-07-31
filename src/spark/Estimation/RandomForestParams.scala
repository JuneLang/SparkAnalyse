package spark.Estimation

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.regression.RandomForestRegressor

/**
  * Created by junlang on 6/22/17.
  */
object RandomForestParams {
  val maxBins = 30
  val subsamplingRate = 0.3
  val featuresCol = "features"
  val impurityClassifier = "gini"
  val impurityRegressor = "variance"
  val maxDepth = 5
  val minInfoGain = 0
  val featureSubsetStrategy = "sqrt"
  val minInstancesPernode = 10
  val numTrees = 50


  def setDefaultParams(c: RandomForestClassifier): RandomForestClassifier = {
    c.setMaxBins(maxBins).setSubsamplingRate(subsamplingRate)
      .setImpurity(impurityClassifier).setMaxDepth(maxDepth).setMinInfoGain(minInfoGain)
      .setFeatureSubsetStrategy(featureSubsetStrategy).
      setMinInstancesPerNode(minInstancesPernode).setNumTrees(numTrees).setSeed(27)
  }
  def setDefaultParams(c: RandomForestRegressor): RandomForestRegressor = {
    c.setMaxBins(maxBins).setSubsamplingRate(subsamplingRate)
      .setImpurity(impurityRegressor).setMaxDepth(maxDepth).setMinInfoGain(minInfoGain)
      .setFeatureSubsetStrategy(featureSubsetStrategy).setMinInstancesPerNode(minInstancesPernode).setNumTrees(numTrees)
  }
}
