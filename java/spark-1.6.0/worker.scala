package sparklyr

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

class WorkerContext(
  sourceArray: Array[Row],
  lock: AnyRef,
  closure: Array[Byte],
  columns: Array[String],
  groupBy: Array[String],
  closureRLang: Array[Byte],
  bundlePath: String) {

  private var result: Array[Row] = Array[Row]()

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

  def getSourceArray(): Array[Row] = {
    sourceArray
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
}

object WorkerHelper {
  def computeRdd(
    rdd: RDD[Row],
    closure: Array[Byte],
    config: String,
    port: Int,
    columns: Array[String],
    groupBy: Array[String],
    closureRLang: Array[Byte],
    bundlePath: String): RDD[Row] = {

    val computed: RDD[Row] = new WorkerRDD(
      rdd,
      closure,
      columns,
      config,
      port,
      groupBy,
      closureRLang,
      bundlePath)

    computed
  }
}
