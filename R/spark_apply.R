spark_schema_from_rdd <- function(rdd, column_names) {
  firstRow <- rdd %>% invoke("first") %>% invoke("toSeq")
  fields <- lapply(seq_along(firstRow), function(idx) {
    name <- if (is.null(column_names)) as.character(idx) else column_names[[idx]]

    invoke_static(
      sc,
      "sparklyr.SQLUtils",
      "createStructField",
      name,
      typeof(firstRow[[idx]]),
      TRUE
    )
  })

  invoke_static(
    sc,
    "sparklyr.SQLUtils",
    "createStructType",
    fields
  )
}

#' @export
spark_apply <- function(x, f, names = colnames(x)) {
  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)

  closure <- serialize(f, NULL)

  rdd <- invoke_static(sc, "sparklyr.WorkerHelper", "computeRdd", sdf, closure)
  schema <- spark_schema_from_rdd(rdd, names)

  transformed <- invoke(hive_context(sc), "createDataFrame", rdd, schema)

  sdf_register(transformed)
}
