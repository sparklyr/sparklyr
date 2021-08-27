package sparklyr

import org.apache.spark.SparkConf

object BackendConf {
  def getNumThreads(): Int = {
    // NOTE: this version will not work for Databricks Connect use cases
    val conf = new SparkConf()

    conf.getInt(
      BackendConfConstants.kNumThreads,
      BackendConfConstants.kNumThreadsDefaultValue
    )
  }
}
