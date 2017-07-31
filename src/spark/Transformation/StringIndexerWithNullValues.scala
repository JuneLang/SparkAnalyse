package spark.Transformation

import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._

/**
  * Created by junlang on 6/15/17.
  */
class StringIndexerWithNullValues extends StringIndexer{

  override def fit(dataset: Dataset[_]): StringIndexerModel = {
    val dsWithoutNull = dataset.na.drop(Array($(inputCol)))
    super.fit(dsWithoutNull)
  }

}
