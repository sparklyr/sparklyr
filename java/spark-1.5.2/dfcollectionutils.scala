package sparklyr

import org.apache.spark.sql._

// this class encapsulates any logic to process a Spark dataframe before its
// data is collected and sent back to R
object DFCollectionUtils {
  // default: do nothing
  def prepareDataFrameForCollection(df: DataFrame): (DataFrame, Array[(String, String)]) = {
    (df, df.dtypes)
  }
}
