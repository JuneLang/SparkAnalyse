package spark
import org.apache.spark.sql.Dataset
import spark.DataLoadFunctions.getColInfo

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

  def getIsMulti(ds: Dataset[_], col: String) = {
    getColType(ds, col) match {
      case Some("terms") =>
        getColInfo[Boolean](ds)(col)("typeSettings.separator") match {
          case Some(i) => Some(true)
          case None => Some(false)
        }
      case _ => None
    }

  }

  def getColValues(ds: Dataset[_], col: String) = getColInfo[Seq[String]](ds)(col)("values")

  def getMax(ds: Dataset[_], col:String) = {
    import DataLoadFunctions.longToDouble
    getColInfo[Double](ds)(col)("metrics.maxValue")// match {
//      case Some(i) =>
//        if (i.isInstanceOf[Long]) {
//          Some(Long.long2double(i))
//        }
//        else {
//          Some(i)
//        }
//      case _ => None
//    }
  }
  def getMin(ds: Dataset[_], col:String) = {
    import DataLoadFunctions.longToDouble
    getColInfo[Double](ds)(col)("metrics.minValue")
  }

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
        case Some("terms") => true
        case _ => false
      }
    })
  }

  implicit def longToDouble(l: Long):Double = Long.long2double(l)
}
