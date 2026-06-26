# Java numeric bounds used by the spark_write_rds() round-trip tests. Wrapped in
# a helper so that no Spark calls run at file-load time (i.e. outside test_that).
java_numeric_bounds <- function(sc) {
  list(
    double_min = invoke_static(sc, "java.lang.Double", "MIN_VALUE"),
    double_max = invoke_static(sc, "java.lang.Double", "MAX_VALUE"),
    float_min = invoke_static(sc, "java.lang.Float", "MIN_VALUE"),
    float_max = invoke_static(sc, "java.lang.Float", "MAX_VALUE")
  )
}
