package sparklyr

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.ClassTag

class WorkerContext[T: ClassTag](
  rdd: RDD[T],
  split: Partition,
  task: TaskContext,
  lock: AnyRef,
  closure: Array[Byte],
  columns: Array[String]) {

  private var result: Array[T] = Array[T]()

  def getClosure(): Array[Byte] = {
    closure
  }

  def getColumns(): Array[String] = {
    columns
  }

  def getSourceIterator(): Iterator[T] = {
    rdd.iterator(split, task)
  }

  def getSourceArray(): Array[T] = {
    getSourceIterator.toArray
  }

  def getSourceArrayLength(): Int = {
    getSourceIterator.toArray.length
  }

  def getSourceArraySeq(): Array[Seq[Any]] = {
    getSourceArray.map(x => x.asInstanceOf[Row].toSeq)
  }

  def setResultArray(resultParam: Array[T]) = {
    result = resultParam
  }

  def setResultArraySeq(resultParam: Array[Any]) = {
    result = resultParam.map(x => Row.fromSeq(x.asInstanceOf[Array[_]].toSeq).asInstanceOf[T])
  }

  def getResultArray(): Array[T] = {
    result
  }

  def finish(): Unit = {
    lock.synchronized {
      lock.notify
    }
  }
}

object WorkerHelper {
  def computeRdd(df: DataFrame, closure: Array[Byte], config: String): RDD[Row] = {

    val parent: RDD[Row] = df.rdd
    val computed: RDD[Row] = new WorkerRDD[Row](
      parent,
      closure,
      df.columns,
      config)

    computed
  }
}
