spark_data_build_types <- function(sc, columns) {
  names <- names(columns)
  fields <- lapply(names, function(name) {
    invoke_static(sc, "sparklyr.SQLUtils", "createStructField", name, columns[[name]][[1]], TRUE)
  })

  invoke_static(sc, "sparklyr.SQLUtils", "createStructType", fields)
}
