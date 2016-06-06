as_spark_rdd <- function(jobj) {
  scon <- spark_scon(jobj)
  jobj <- as_spark_dataframe(jobj)
  rdd <- spark_invoke(jobj, "rdd")
  spark_invoke_static(scon, "utils", "schemaRddToVectorRdd", rdd)
}
