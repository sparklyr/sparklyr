# Access common 'static' objects / instances associated
# with a Spark connection.
spark_get_sql_context <- function(sc) {
  if (!is.null(sc$hive_context))
    sc$hive_context
  else
    sc$sql_context
}
