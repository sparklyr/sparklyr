spark_schema_from_rdd <- function(sc, rdd, column_names) {
  columns_typed <- length(names(column_names)) > 0

  if (columns_typed) {
    schema <- spark_data_build_types(sc, column_names)
    return(schema)
  }

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

#' Apply an R Function in Spark
#'
#' Applies an R function to a Spark object (typically, a Spark DataFrame).
#'
#' @param x An object (usually a \code{spark_tbl}) coercable to a Spark DataFrame.
#' @param f A function that transforms a data frame partition into a data frame.
#'   The function \code{f} has signature \code{f(df, group1, group2, ...)} where
#'   \code{df} is a data frame with the data to be processed and \code{group1} to
#'   \code{groupN} contain the values of the \code{group_by} values. When
#'   \code{group_by} is not specified, \code{f} takes only one argument.
#' @param columns A vector of column names or a named vector of column types for
#'   the transformed object. Defaults to the names from the original object.
#' @param memory Boolean; should the table be cached into memory?
#' @param group_by Column name used to group by data frame partitions.
#' @param packages Boolean; distribute \code{.libPaths()} packages to nodes?
#' @param ... Optional arguments; currently unused.
#'
#' @export
spark_apply <- function(x,
                        f,
                        columns = colnames(x),
                        memory = TRUE,
                        group_by = NULL,
                        packages = TRUE,
                        ...) {
  args <- list(...)
  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)
  sdf_columns <- colnames(x)
  rdd_base <- invoke(sdf, "rdd")
  grouped <- !is.null(group_by)
  args <- list(...)
  rlang <- spark_config_value(x$sc, "sparklyr.closures.rlang", FALSE)

  # backward compatible support for names argument from 0.6
  if (!is.null(args$names)) {
    columns <- args$names
  }

  columns_typed <- length(names(columns)) > 0

  if (rlang) warning("The `rlang` parameter is under active development.")

  # disable package distribution for local connections
  if (spark_master_is_local(sc$master)) packages = FALSE

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
    colpos <- which(colnames(x) %in% group_by)
    if (length(colpos) != length(group_by)) stop("Not all group_by columns found.")

    group_by_list <- as.list(as.integer(colpos - 1))

    grouped_rdd <- invoke_static(sc, "sparklyr.ApplyUtils", "groupBy", rdd_base, group_by_list)

    rdd_base <- grouped_rdd

    if (!columns_typed) {
      columns <- c(group_by, columns)
    }
  }

  worker_port <- spark_config_value(sc$config, "sparklyr.gateway.port", "8880")

  bundle_path <- ""
  if (packages) {
    bundle_path <- core_spark_apply_bundle_path()
    if (!file.exists(bundle_path)) {
      bundle_path <- core_spark_apply_bundle()
    }

    if (!is.null(bundle_path)) {
      bundle_was_added <- file.exists(
        invoke_static(
          sc,
          "org.apache.spark.SparkFiles",
          "get",
          basename(bundle_path))
      )

      if (!bundle_was_added) {
        spark_context(sc) %>% invoke("addFile", bundle_path)
      }
    }
  }

  rdd <- invoke_static(
    sc,
    "sparklyr.WorkerHelper",
    "computeRdd",
    rdd_base,
    closure,
    worker_config,
    as.integer(worker_port),
    as.list(sdf_columns),
    as.list(group_by),
    closure_rlang,
    bundle_path
  )

  # while workers need to relaunch sparklyr backends, cache by default
  if (memory) rdd <- invoke(rdd, "cache")

  schema <- spark_schema_from_rdd(sc, rdd, columns)

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
