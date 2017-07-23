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
#' @param group_by Column name used to group by data frame partitions.
#' @param rlang Boolean; should the closure be serialize using the rlang package?
#' @param ... Optional arguments; currently unused.
#'
#' @export
spark_apply <- function(x,
                        f,
                        names = colnames(x),
                        memory = TRUE,
                        group_by = NULL,
                        rlang = TRUE,
                        ...) {
  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)
  sdf_columns <- colnames(x)
  rdd_base <- invoke(sdf, "rdd")
  grouped <- !is.null(group_by)
  args <- list(...)

  # create closure for the given function
  closure <- serialize(f, NULL)

  # create rlang closure
  rlang_serialize <- spark_apply_rlang_serialize()
  closure_rlang <- if (rlang && !is.null(rlang_serialize)) rlang_serialize(f) else raw()

  # create a configuration string to initialize each worker
  worker_config <- worker_config_serialize(
    c(
      list(
        debug = isTRUE(args$debug)
      ),
      sc$config
    )
  )

  if (grouped) {
    colpos <- which(colnames(x) == group_by)
    if (length(colpos) == 0) stop("Column '", group_by, "' not found.")

    grouped_schema <- invoke_static(sc, "sparklyr.ApplyUtils", "groupBySchema", sdf)
    grouped_rdd <- invoke_static(sc, "sparklyr.ApplyUtils", "groupBy", rdd_base, as.integer(colpos - 1))
    grouped_df <- invoke(hive_context(sc), "createDataFrame", grouped_rdd, grouped_schema)

    storage_level <- invoke_static(
      sc,
      "org.apache.spark.storage.StorageLevel",
      ifelse(memory, "MEMORY_AND_DISK", "DISK_ONLY")
    )

    invoke(grouped_df, "persist", storage_level)
    invoke(grouped_df, "count")

    rdd_base <- grouped_df %>% invoke("rdd")
  }

  worker_port <- spark_config_value(sc$config, "sparklyr.gateway.port", "8880")

  rdd <- invoke_static(
    sc,
    "sparklyr.WorkerHelper",
    "computeRdd",
    rdd_base,
    closure,
    worker_config,
    as.integer(worker_port),
    as.list(sdf_columns),
    group_by,
    closure_rlang
    )

  # while workers need to relaunch sparklyr backends, cache by default
  if (memory) rdd <- invoke(rdd, "cache")

  schema <- spark_schema_from_rdd(sc, rdd, names)

  transformed <- invoke(hive_context(sc), "createDataFrame", rdd, schema)

  sdf_register(transformed)
}

spark_apply_rlang_serialize <- function() {
  rlang_serialize <- core_get_package_function("rlang", "serialise_bytes")
  if (is.null(rlang_serialize))
    core_get_package_function("rlanglabs", "serialise_bytes")
  else
    rlang_serialize
}
