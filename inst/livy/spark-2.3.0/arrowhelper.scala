//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

object ArrowHelper {
  import org.apache.spark.SparkContext
  import java.nio.file.{Files, Paths}

  def rddFromBinaryBatches(sc: SparkContext, batches: Seq[Array[Byte]], parallelism: Int):
      org.apache.spark.rdd.RDD[Array[Byte]] = {
    sc.parallelize(batches, parallelism)
  }

  def javaRddFromBinaryBatches(sc: SparkContext, batches: Seq[Array[Byte]], parallelism: Int):
    org.apache.spark.api.java.JavaRDD[Array[Byte]] = {
    val rdd = rddFromBinaryBatches(sc, batches, parallelism)
    org.apache.spark.api.java.JavaRDD.fromRDD(rdd)
  }

  def javaRddFromBinaryBatchFile(sc: SparkContext, path: String, parallelism: Int):
    org.apache.spark.api.java.JavaRDD[Array[Byte]] = {

    val batch = Files.readAllBytes(Paths.get(path))
    val batches: Seq[Array[Byte]] = Seq(batch)

    val rdd = rddFromBinaryBatches(sc, batches, parallelism)
    org.apache.spark.api.java.JavaRDD.fromRDD(rdd)
  }
}
