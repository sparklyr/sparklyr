package sparklyr

object ArrowUtils {
  import org.apache.spark.SparkContext

  def rddFromBinaryBatches(sc: SparkContext, batches: Seq[Array[Byte]], parallelism: Int):
      org.apache.spark.rdd.RDD[Array[Byte]] = {
    sc.parallelize(batches, parallelism)
  }
}
