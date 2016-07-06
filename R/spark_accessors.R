# Access common 'static' objects / instances associated
# with a Spark connection.
spark_get_sql_context <- function(sc) {
  hive_context(sc)
}
