package sparklyr

object ArrowHelper {
  import org.apache.spark.SparkContext

  def rddFromBinaryBatches(sc: SparkContext, batches: Seq[Array[Byte]], parallelism: Int):
      org.apache.spark.rdd.RDD[Array[Byte]] = {
    sc.parallelize(batches, parallelism)
  }

  def javaRddFromBinaryBatches(sc: SparkContext, batches: Seq[Array[Byte]], parallelism: Int):
    org.apache.spark.api.java.JavaRDD[Array[Byte]] = {
    val rdd = rddFromBinaryBatches(sc, batches, parallelism)
    org.apache.spark.api.java.JavaRDD.fromRDD(rdd)
  }
}
