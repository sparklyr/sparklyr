package sparklyr

import org.apache.spark.sql.DataFrame
import org.apache.spark.TaskContext

object PartitionUtils {
  def computePartitionSizes(df: DataFrame): Array[Array[Int]] = {
    df.mapPartitions(
      iter => Array(Array(TaskContext.getPartitionId, iter.length)).iterator
    ).
    collect
  }
}
