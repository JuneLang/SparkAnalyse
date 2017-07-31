package spark

import org.joda.time.{DateTime, Duration, DurationFieldType}
/**
  * Created by junlang on 6/26/17.
  * @usecase Works like @decorator in Python.
  */
object DateOperations {
  def baseGet[T](t1: DateTime, t2: DateTime)(f: ((DateTime, DateTime) => T)): T = {
    if(t1.compareTo(t2) > 0) throw new Exception("The first date should not be greater than the second one.")
    else f(t1, t2)
  }

  def baseGetYears(t1: DateTime, t2: DateTime): Int = t2.getYear - t1.getYear
  def getYears(t1: DateTime, t2: DateTime) = baseGet(t1, t2)(baseGetYears)

  def baseGetMonths(t1: DateTime, t2: DateTime): Int = getYears(t1, t2) * 12 + t2.getMonthOfYear - t1.getMonthOfYear
  def getMonths(t1: DateTime, t2: DateTime) = baseGet(t1, t2)(baseGetMonths)

  def baseGetWeeks(t1: DateTime, t2: DateTime): Int = Math.ceil(getDays(t1, t2) / 7.0).toInt
  def getWeeks(t1: DateTime, t2: DateTime) = baseGet(t1, t2)(baseGetWeeks)

  def baseGetDays(t1: DateTime, t2: DateTime): Int = {
    val days = new Duration(t1, t2).getStandardDays.toInt
    val t3 = t1.withFieldAdded(DurationFieldType.days(), days)
    if (t3.compareTo(t2) < 0) days + 1
    else days
  }
  def getDays(t1: DateTime, t2: DateTime) = baseGet(t1, t2)(baseGetDays)

  def baseGetSeconds(t1: DateTime, t2: DateTime): Long = new Duration(t1, t2).getStandardSeconds
  def getSeconds(t1: DateTime, t2: DateTime) = baseGet(t1, t2)(baseGetSeconds)

}

