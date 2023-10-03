package sparklyr

import org.apache.spark.{SparkConf, SparkEnv}

object BackendConf {
  def getNumThreads(): Int = {
    val sparkEnv = SparkEnv.get
    if (sparkEnv != null && sparkEnv.conf != null) {
      sparkEnv.conf.getInt(
        BackendConfConstants.kNumThreads,
        BackendConfConstants.kNumThreadsDefaultValue
      )
    } else {
      val conf = new SparkConf()

      conf.getInt(
        BackendConfConstants.kNumThreads,
        BackendConfConstants.kNumThreadsDefaultValue
      )
    }
  }
}
