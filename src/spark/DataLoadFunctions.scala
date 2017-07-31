package spark
import org.apache.spark.sql.Dataset

/**
  * Created by junlang on 5/23/17.
  */
object DataLoadFunctions {
  /**
    * get a value according to its path in json
    * @param col  column name
    * @param info  tha path associate to the col
    * @return  the value
    */
  def getColInfo[T](ds: Dataset[_])(col: String)(info: String): Option[T] = {
    val path = col + "." + info

    try {
      val res = ds.select(path).head().getAs[T](0)
      Some(res)
    }
    catch {
      case _ => None
    }
  }

  /**
    *
    * @param col  get the data type of this column
    * @return The type
    */
  def getColType(ds: Dataset[_], col: String) = getColInfo[String](ds)(col)("type")

  def getIsMulti(ds: Dataset[_], col: String) = getColInfo[Boolean](ds)(col)("multi")

  def getColValues(ds: Dataset[_], col: String) = getColInfo[Seq[String]](ds)(col)("array")

  def getMax(ds: Dataset[_], col:String) = getColInfo[Double](ds)(col)("max")
  def getMin(ds: Dataset[_], col:String) = getColInfo[Double](ds)(col)("min")

  /**
    *  cleaning columns, just need `data-xxx`. Then, drop the columns wont be used, like ids, texts, etc...
    * @param ds
    * @return
    */
  def cleanCols(ds: Dataset[_]): Array[String] = {
    // data columns
    val dataCols = ds.columns.filter(col => {
      if (col matches "^data-.*") {
        val t = getColType(ds, col)
        t match {
          case Some("other") =>
            false
          case _ =>
            true
        }
      }
      else false
    })
    dataCols
  }

  def getQualiCols(ds: Dataset[_]): Array[String] = {
    ds.columns.filter(col => {
      val t = getColType(ds, col)
      t match {
        case Some("string") => true
        case _ => false
      }
    })
  }
}
