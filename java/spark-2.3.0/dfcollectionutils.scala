package sparklyr

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// this class encapsulates any logic to process a Spark dataframe before its
// data is collected and sent back to R
object DFCollectionUtils {
  def prepareDataFrameForCollection(df: DataFrame): (DataFrame, Array[(String, String)]) = {
    val dtypes = df.dtypes
    val columns = df.columns
    val struct_type_matcher = StructTypeAsJSON.ReStructType.pattern.matcher(_)
    // transform all StructType elements into JSON
    val transformed_df = df.select(Array.range(0, dtypes.length).map(i => {
      val is_struct_col: Boolean = struct_type_matcher(dtypes(i)._2).matches
      val col_name_quoted = "`" + columns(i) + "`";
      if (is_struct_col) {
        dtypes(i) = (dtypes(i)._1, StructTypeAsJSON.DType)
        to_json(col(col_name_quoted)).as(columns(i))
      } else {
        col(col_name_quoted)
      }
    }) :_*)
    (transformed_df, dtypes)
  }
}
