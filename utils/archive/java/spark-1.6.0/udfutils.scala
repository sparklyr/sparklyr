package sparklyr

import org.apache.spark.sql.UDFRegistration

object UdfUtils {
  def registerSparklyrUDFs(udf: UDFRegistration): Unit = {
    udf.register("sparklyr_str_split", StrSplit.impl)
    udf.register("sparklyr_cumprod", new CumProd)
  }
}
