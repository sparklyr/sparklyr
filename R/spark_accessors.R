# Access common 'static' objects / instances associated
# with a Spark connection.
spark_get_sql_context <- function(sc) {
  sconInst <- spark_connection_get_inst(sc)
  if (!identical(sconInst$hive, NULL))
    sconInst$hive
  else
    sconInst$sql
}

spark_get_spark_context <- function(sc) {
  sconInst <- spark_connection_get_inst(sc)
  sconInst$sc
}
