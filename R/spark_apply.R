spark_schema_from_rdd <- function(sc, rdd, column_names) {
  sampleRows <- rdd %>% invoke(
    "take",
    sparklyr::ensure_scalar_integer(
      spark_config_value(sc$config, "sparklyr.apply.schema.infer", 10)
    )
  )

  colTypes <- NULL
  lapply(sampleRows, function(r) {
    row <- r %>% invoke("toSeq")

    if (is.null(colTypes))
      colTypes <<- replicate(length(row), "character")

    lapply(seq_along(row), function(colIdx) {
      colVal <- row[[colIdx]]
      if (!is.na(colVal) && !is.null(colVal)) {
        colTypes[[colIdx]] <<- typeof(colVal)
      }
    })
  })

  if (any(sapply(colTypes, is.null)))
    stop("Failed to infer column types, please use explicit types.")

  fields <- lapply(seq_along(colTypes), function(idx) {
    name <- if (is.null(column_names)) as.character(idx) else column_names[[idx]]

    invoke_static(
      sc,
      "sparklyr.SQLUtils",
      "createStructField",
      name,
      colTypes[[idx]],
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
#' @param memory Boolean; should the table be cached into memory?
#' @param ... Optional arguments; currently unused.
#'
#' @export
spark_apply <- function(x, f, names = colnames(x), memory = TRUE, ...) {
  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)
  args <- list(...)

  # create closure for the given function
  closure <- serialize(f, NULL)

  # build the configuration definetion for each worker role

  # create a configuration string to initialize each worker
  worker_config <- worker_config_serialize(
    c(
      list(
        debug = isTRUE(args$debug)
      ),
      sc$config
    )
  )

  worker_port <- spark_config_value(sc$config, "sparklyr.gateway.port", "8880")

  rdd <- invoke_static(
    sc,
    "sparklyr.WorkerHelper",
    "computeRdd",
    sdf,
    closure,
    worker_config,
    as.integer(worker_port))

  # while workers need to relaunch sparklyr backends, cache by default
  if (memory) rdd <- invoke(rdd, "cache")

  schema <- spark_schema_from_rdd(sc, rdd, names)

  transformed <- invoke(hive_context(sc), "createDataFrame", rdd, schema)

  sdf_register(transformed)
}
