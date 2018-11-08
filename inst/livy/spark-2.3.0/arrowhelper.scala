//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

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

  def javaRddUnion(context: org.apache.spark.SparkContext, rdds: Seq[org.apache.spark.api.java.JavaRDD[Array[Byte]]]):
    org.apache.spark.rdd.RDD[Array[Byte]] = {
    context.union(rdds.map(x => x.rdd))
  }
}
