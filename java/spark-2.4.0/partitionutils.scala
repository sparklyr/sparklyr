package sparklyr

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.TaskContext

object PartitionUtils {
  def computePartitionSizes(df: DataFrame): Array[Array[Int]] = {
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    df.mapPartitions(
      iter => Array(Array(TaskContext.getPartitionId, iter.length)).iterator
    ).
    collect
  }
}
