//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

import org.apache.spark.sql._

object Repartition {

  def repartition(df: DataFrame, numPartitions: Int, partitionCols: String*): DataFrame = {
    val partitionExprs = partitionCols.map(df.col(_))

    (numPartitions, partitionExprs.length) match {
      case (0, l) if l > 0 => df.repartition(partitionExprs: _*)
      case (n, 0) if n > 0 => df.repartition(numPartitions)
      case (n, l) if n > 0 && l > 0 => df.repartition(numPartitions, partitionExprs: _*)
      case _ => throw new IllegalArgumentException
    }
  }
}
