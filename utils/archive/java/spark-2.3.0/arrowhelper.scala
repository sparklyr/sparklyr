package sparklyr

import java.nio.file.{Files, Paths}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkContext

object ArrowHelper {

  def rddFromBinaryBatches(sc: SparkContext, batches: Seq[Array[Byte]], parallelism: Int):
      org.apache.spark.rdd.RDD[Array[Byte]] = {
    sc.parallelize(batches, parallelism)
  }

  def javaRddFromBinaryBatches(sc: SparkContext, batches: Seq[Array[Byte]], parallelism: Int):
    JavaRDD[Array[Byte]] = {
    val rdd = rddFromBinaryBatches(sc, batches, parallelism)
    JavaRDD.fromRDD(rdd)
  }

  def javaRddFromBinaryBatchFile(sc: SparkContext, path: String, parallelism: Int):
    JavaRDD[Array[Byte]] = {

    val batch = Files.readAllBytes(Paths.get(path))
    val batches: Seq[Array[Byte]] = Seq(batch)

    val rdd = rddFromBinaryBatches(sc, batches, parallelism)
    JavaRDD.fromRDD(rdd)
  }
}
