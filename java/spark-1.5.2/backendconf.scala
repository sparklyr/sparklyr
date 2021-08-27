package sparklyr

import org.apache.spark.SparkConf

object BackendConf {
  def getNumThreads(): Int = {
    val conf = new SparkConf()

    conf.getInt(
      BackendConfConstants.kNumThreads,
      BackendConfConstants.kNumThreadsDefaultValue
    )
  }
}
