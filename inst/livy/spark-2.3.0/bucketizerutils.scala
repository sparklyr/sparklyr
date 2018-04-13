//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

import org.apache.spark.ml.feature.Bucketizer

object BucketizerUtils {
 def setSplitsArrayParam(bucketizer: Bucketizer,
                     splitsArray: Array[Double]*): Bucketizer = {
   bucketizer.setSplitsArray(splitsArray.toArray)
 }
}
