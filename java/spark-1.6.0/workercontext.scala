package sparklyr

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConversions._

class WorkerContext(
  iterator: Iterator[Row],
  lock: AnyRef,
  closure: Array[Byte],
  columns: Array[String],
  groupBy: Array[String],
  closureRLang: Array[Byte],
  bundlePath: String,
  context: Array[Byte],
  timeZoneId: String,
  schema: org.apache.spark.sql.types.StructType,
  options: Map[String, String],
  barrier: Map[String, Any],
  partitionIndex: Int) {

  private var result: Array[Row] = Array[Row]()
  private var sourceArray: Option[Array[Row]] = None

  def getClosure(): Array[Byte] = {
    closure
  }

  def getClosureRLang(): Array[Byte] = {
    closureRLang
  }

  def getColumns(): Array[String] = {
    columns
  }

  def getGroupBy(): Array[String] = {
    groupBy
  }

  def getIterator(): Iterator[Row] = {
    iterator
  }

  def getSourceArray(): Array[Row] = {
    if (sourceArray.isEmpty) {
      sourceArray = Option(iterator.toArray)
    }

    sourceArray.get
  }

  def getSourceArrayLength(): Int = {
    getSourceArray.length
  }

  def getSourceArraySeq(): Array[Seq[Any]] = {
    getSourceArray.map(x => x.toSeq)
  }

  def getSourceArrayGroupedSeq(): Array[Array[Array[Any]]] = {
    getSourceArray.map(x => x.toSeq.map(g => g.asInstanceOf[Seq[Any]].toArray).toArray)
  }

  def setResultArraySeq(resultParam: Array[Any]) = {
    result = resultParam.map(x => Row.fromSeq(x.asInstanceOf[Array[_]].toSeq))
  }

  def setResultIter(resultParam: Iterator[Row]) = {
    result = resultParam.toArray
  }

  def getResultArray(): Array[Row] = {
    result
  }

  def finish(): Unit = {
    lock.synchronized {
      lock.notify
    }
  }

  def getBundlePath(): String = {
    bundlePath
  }

  def getContext(): Array[Byte] = {
    context
  }

  def getTimeZoneId(): String = {
    timeZoneId
  }

  def getSchema() : StructType = {
    schema
  }

  def getArrowConvertersImpl() : ArrowConvertersImpl = {
    new ArrowConvertersImpl()
  }

  def getArrowConverters() : Any = {
    ArrowConverters
  }

  def getSqlUtils() : Any = {
    SQLUtils
  }

  def getOptions() : Map[String, String] = {
    options
  }

  def getBarrier() : Map[String, Any] = {
    barrier
  }

  def getPartitionIndex() : Int = {
    partitionIndex
  }
}
