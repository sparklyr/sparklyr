spark_data_build_types <- function(sc, columns) {
  names <- names(columns)
  fields <- lapply(
    names,
    function(name) {
      if (is.list(columns[[name]])) {
        struct <- spark_data_build_types(sc, columns[[name]])
        invoke_static(
          sc,
          "sparklyr.SQLUtils",
          "createStructField",
          name,
          struct
        )
      } else {
        invoke_static(
          sc,
          "sparklyr.SQLUtils",
          "createStructField",
          name,
          columns[[name]][[1]],
          TRUE
        )
      }
    }
  )

  invoke_static(sc, "sparklyr.SQLUtils", "createStructType", fields)
}
