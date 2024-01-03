package sparklyr

import org.apache.spark.ml.feature.Bucketizer

object BucketizerUtils {
 def setSplitsArrayParam(bucketizer: Bucketizer,
                     splitsArray: Array[Double]*): Bucketizer = {
   bucketizer.setSplitsArray(splitsArray.toArray)
 }
}
