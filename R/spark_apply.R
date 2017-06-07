spark_schema_from_rdd <- function(sc, rdd, column_names) {
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

#' Apply a Function in Spark
#'
#' Applies a function to a Spark object (typically, a Spark DataFrame).
#'
#' @param x An object (usually a \code{spark_tbl}) coercable to a Spark DataFrame.
#' @param f A function that transforms a data frame partition into a data frame.
#' @param names The column names for the transformed object, defaults to the
#'   names from the original object.
#'
#' @export
spark_apply <- function(x, f, names = colnames(x)) {
  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)

  closure <- serialize(f, NULL)

  rdd <- invoke_static(sc, "sparklyr.WorkerHelper", "computeRdd", sdf, closure)
  schema <- spark_schema_from_rdd(sc, rdd, names)

  transformed <- invoke(hive_context(sc), "createDataFrame", rdd, schema)

  sdf_register(transformed)
}
