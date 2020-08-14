package sparklyr

import org.apache.spark.sql.UDFRegistration

object UdfUtils {
  def registerSparklyrUDFs(udf: UDFRegistration): Unit = {
    val strSplit = (str: String, regex: String, limit: Int) => {
      if (null == str || null == regex)
        null
      else
        str.split(regex, limit)
    }

    udf.register("sparklyr_str_split", strSplit)
  }
}
