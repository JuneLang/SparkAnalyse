package spark

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.sql.functions._
import java.{util => jutil}
import org.apache.spark.SparkException
import org.apache.spark.mllib.tree.impurity.Entropy.log2
import org.joda.time.{DateTime, Duration}
import DateOperations._

/**
  * Created by junlang on 5/17/17.
  */

object CalculateEntropy {
  val NUMBER_INTERVAL = 20
  def apply(dataset: DataFrame, colInfo: DataFrame): Array[Double] = {
    // get the values of each column
    val columnValues = dataset.columns.map(col => dataset.select(col).na.drop().collect())
    dataset.dtypes.zip(columnValues).map(col => {
      if(col._1._2 == "StringType") {
        val totalCount = col._2.length
        val colCounts = col._2.groupBy(row => row).mapValues(_.length) // MapReduce
        calculate(colCounts.map(l => l._2.toDouble).toArray, totalCount)
      }
      else if (col._1._2 == "DateType") {
        val dateArr =col._2 map (row => new DateTime(row.getDate(0)))
        val len = dateArr.length.toDouble
        val maxDate = dateArr.max
        val minDate = dateArr.min
        val entropyBuf = scala.collection.mutable.ArrayBuffer.empty[(String, Double)]
        val minYear = minDate.getYear
        if (getYears(minDate, maxDate) >= 1) { // set 1 year is an interval
          val yearArr = dateArr.map(_.getYear.toDouble)
          entropyBuf += getDateEntropy("year", yearArr, len)
        }

        if (getMonths(minDate, maxDate) >= 1) { // set 1 month as an interval
          val minMonth = minDate.getMonthOfYear
          val monthArr = dateArr map(d => (d.getYear - minYear) * 12 + d.getMonthOfYear - minMonth)
          entropyBuf += getDateEntropy("month", monthArr, len)
        }

        if (getWeeks(minDate, maxDate) >= 1 && minDate.getDayOfWeek != 1) { // set 1 week as an interval
          val weekArray = dateArr map(d => d.getWeekyear + "-" + d.getWeekOfWeekyear)
          entropyBuf += getDateEntropy("week", weekArray, len)
        }

        if (getDays(minDate, maxDate) >= 1) { // set 1 day as an interval
          val dayArr = dateArr map(d => d.getYear + "-" + d.getDayOfYear)
          entropyBuf += getDateEntropy("day", dayArr, len)
        }

        if (entropyBuf.length <= 1) { // calculate for hh/mm/ss only when the duration is not too long
          val sec = getSeconds(minDate, maxDate)
          val min = Math.ceil(sec / 60.0)
          val hour = Math.ceil(sec / 3600.0)
          if (hour > 1) {
            val hourArr = dateArr map(d => Math.floor(d.getMillis / 3600000.0))
            entropyBuf += getDateEntropy("hour", hourArr, len)
          }
          if (min > 1) {
            val minArr = dateArr map(d => Math.floor(d.getMillis / 60000.0))
            entropyBuf += getDateEntropy("min", minArr, len)
          }
          if (sec > 1) {
            val secArr = dateArr map(d => Math.floor(d.getMillis / 1000.0))
            entropyBuf += getDateEntropy("second", secArr, len)
          }
        }
//        entropyBuf foreach print _
        val minEntropy = if (entropyBuf.nonEmpty) entropyBuf.minBy(t => t._2)._2 else 1.0
        minEntropy
      }
      else { // numeric types
        val numList = col._2.map(r => r.getAs(0).asInstanceOf[Number].doubleValue())
        val dist = numList.max - numList.min

        dist > NUMBER_INTERVAL match {
          case true => {
            val interval = dist / NUMBER_INTERVAL
            val range = (numList.min to numList.max by interval).toArray
            range(range.length - 1) = Double.PositiveInfinity
            val buckets = bucketize(numList, range) // set nbr of buckets as the constant
//            val totalCount = buckets.foldLeft(0)((sum, v) => sum + v)
            calculate(buckets.map(v => v.toDouble), numList.length)
          }
          case false => {
            val range = (numList.min to numList.max by 1.0).toList ::: Double.PositiveInfinity :: Nil
            val buckets = bucketize(numList, range.toArray)
//            val totalCount = buckets.foldLeft(0)((sum, v) => sum + v)
            calculate(buckets.map(v => v.toDouble), numList.length)
          }
        }
      }
    })
//    val counts = columnValues.map(col => col.length)
//    val colsCounts = columnValues.map(col => col.groupBy(row => row).mapValues(_.size))
//    colsCounts.zip(counts).map(col => Entropy.calculate(col._1.map(r => r._2.toDouble).toArray, col._2))
  }

  def bucketize(input: Array[Double], range: Array[Double]): Array[Int] = {
    val buckets = Array.ofDim[Int](range.length - 1)
    input.foreach(row => {
      val idx = jutil.Arrays.binarySearch(range, row)
      if (idx >= 0) {
        buckets(idx) += 1
      } else {
        val insertPos = -idx - 1
        if (insertPos == 0 || insertPos == input.length) {
          throw new SparkException(s"Feature value $row out of Bucketizer bounds" +
            s" [${input.min}, ${input.max}].  Check your features, or loosen " +
            s"the lower/upper bound constraints.")
        } else {
          buckets(insertPos - 1) += 1
        }
      }
    })
    buckets
  }

  private def getDateEntropy[T](s: String, input: Array[T], length: Double):(String, Double) = {
    val buckets = input.groupBy(e => e).mapValues(_.length)
    (s, calculate(buckets.map(y => y._2.toDouble).toArray, length))
  }

  private def calculate(counts: Array[Double], totalCount: Double): Double = {
    if (totalCount == 0) {
      return 0
    }
    val numClasses = counts.length
    var impurity = 0.0
    var classIndex = 0
    while (classIndex < numClasses) {
      val classCount = counts(classIndex)
      if (classCount != 0) {
        val freq = classCount / totalCount
        impurity -= freq * log2(freq)
      }
      classIndex += 1
    }
    impurity / log2(numClasses)
  }

  private def log2(x: Double) = scala.math.log(x) / scala.math.log(2)

  implicit def cmp: Ordering[DateTime] = new Ordering[DateTime] {
    override def compare(a: DateTime, b: DateTime): Int = {
      a.compareTo(b)
    }
  }


}

