#' @include spark_apply_bundle.R
#' @include spark_schema_from_rdd.R
#' @include utils.R
NULL

spark_apply_worker_config <- function(sc,
                                      debug,
                                      profile,
                                      schema = FALSE,
                                      arrow = FALSE,
                                      fetch_result_as_sdf = TRUE,
                                      single_binary_column = FALSE,
                                      spark_read = FALSE) {
  worker_config_serialize(
    c(
      list(
        debug = isTRUE(debug),
        profile = isTRUE(profile),
        schema = isTRUE(schema),
        arrow = isTRUE(arrow),
        fetch_result_as_sdf = isTRUE(fetch_result_as_sdf),
        spark_version = spark_version(sc),
        single_binary_column = single_binary_column,
        spark_read = spark_read
      ),
      sc$config
    )
  )
}

#' Apply an R Function in Spark
#'
#' Applies an R function to a Spark object (typically, a Spark DataFrame).
#'
#' @param x An object (usually a \code{spark_tbl}) coercable to a Spark DataFrame.
#' @param f A function that transforms a data frame partition into a data frame.
#'   The function \code{f} has signature \code{f(df, context, group1, group2, ...)} where
#'   \code{df} is a data frame with the data to be processed, \code{context}
#'   is an optional object passed as the \code{context} parameter and \code{group1} to
#'   \code{groupN} contain the values of the \code{group_by} values. When
#'   \code{group_by} is not specified, \code{f} takes only one argument.
#'
#'   Can also be an \code{rlang} anonymous function. For example, as \code{~ .x + 1}
#'   to define an expression that adds one to the given \code{.x} data frame.
#' @param columns A vector of column names or a named vector of column types for
#'   the transformed object. When not specified, a sample of 10 rows is taken to
#'   infer out the output columns automatically, to avoid this performance penalty,
#'   specify the column types. The sample size is configurable using the
#'   \code{sparklyr.apply.schema.infer} configuration option.
#' @param memory Boolean; should the table be cached into memory?
#' @param group_by Column name used to group by data frame partitions.
#' @param packages Boolean to distribute \code{.libPaths()} packages to each node,
#'   a list of packages to distribute, or a package bundle created with
#'   \code{spark_apply_bundle()}.
#'
#'   Defaults to \code{TRUE} or the \code{sparklyr.apply.packages} value set in
#'   \code{spark_config()}.
#'
#'   For clusters using Yarn cluster mode, \code{packages} can point to a package
#'   bundle created using \code{spark_apply_bundle()} and made available as a Spark
#'   file using \code{config$sparklyr.shell.files}. For clusters using Livy, packages
#'   can be manually installed on the driver node.
#'
#'   For offline clusters where \code{available.packages()} is not available,
#'   manually download the packages database from
#'  https://cran.r-project.org/web/packages/packages.rds and set
#'   \code{Sys.setenv(sparklyr.apply.packagesdb = "<pathl-to-rds>")}. Otherwise,
#'   all packages will be used by default.
#'
#'   For clusters where R packages already installed in every worker node,
#'   the \code{spark.r.libpaths} config entry can be set in \code{spark_config()}
#'   to the local packages library. To specify multiple paths collapse them
#'   (without spaces) with a comma delimiter (e.g., \code{"/lib/path/one,/lib/path/two"}).
#' @param context Optional object to be serialized and passed back to \code{f()}.
#' @param name Optional table name while registering the resulting data frame.
#' @param barrier Optional to support Barrier Execution Mode in the scheduler.
#' @param fetch_result_as_sdf Whether to return the transformed results in a Spark
#'   Dataframe (defaults to \code{TRUE}). When set to \code{FALSE}, results will be
#'   returned as a list of R objects instead.
#'
#'   NOTE: \code{fetch_result_as_sdf} must be set to \code{FALSE} when the transformation
#'   function being applied is returning R objects that cannot be stored in a Spark
#'   Dataframe (e.g., complex numbers or any other R data type that does not have an
#'   equivalent representation among Spark SQL data types).
#' @param partition_index_param Optional if non-empty, then \code{f} also receives
#'   the index of the partition being processed as a named argument with this name, in
#'   addition to all positional argument(s) it will receive
#'
#'   NOTE: when \code{fetch_result_as_sdf} is set to \code{FALSE}, object returned from the
#'   transformation function also must be serializable by the \code{base::serialize}
#'   function in R.
#' @param arrow_max_records_per_batch Maximum size of each Arrow record batch,
#'   ignored if Arrow serialization is not enabled.
#' @param auto_deps [Experimental] Whether to infer all required R packages by
#'   examining the closure \code{f()} and only distribute required R and their
#'   transitive dependencies to Spark worker nodes (default: FALSE).
#'   NOTE: this option will only take effect if \code{packages} is set to
#'   \code{TRUE} or is a character vector of R package names. If \code{packages}
#'   is a character vector of R package names, then both the set of packages
#'   specified by \code{packages} and the set of inferred packages will be
#'   distributed to Spark workers.
#' @param ... Optional arguments; currently unused.
#'
#' @section Configuration:
#'
#' \code{spark_config()} settings can be specified to change the workers
#' environment.
#'
#' For instance, to set additional environment variables to each
#' worker node use the \code{sparklyr.apply.env.*} config, to launch workers
#' without \code{--vanilla} use \code{sparklyr.apply.options.vanilla} set to
#' \code{FALSE}, to run a custom script before launching Rscript use
#' \code{sparklyr.apply.options.rscript.before}.
#'
#' @examples
#' \dontrun{
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "local[3]")
#'
#' # creates an Spark data frame with 10 elements then multiply times 10 in R
#' sdf_len(sc, 10) %>% spark_apply(function(df) df * 10)
#'
#' # using barrier mode
#' sdf_len(sc, 3, repartition = 3) %>%
#'   spark_apply(nrow, barrier = TRUE, columns = c(id = "integer")) %>%
#'   collect()
#' }
#'
#' @export
spark_apply <- function(
    x,
    f,
    columns = NULL,
    memory = TRUE,
    group_by = NULL,
    packages = NULL,
    context = NULL,
    name = NULL,
    barrier = NULL,
    fetch_result_as_sdf = TRUE,
    partition_index_param = "",
    arrow_max_records_per_batch = NULL,
    auto_deps = FALSE,
    ...) {
  UseMethod("spark_apply")
}

#' @export
spark_apply.default <- function(
    x,
    f,
    columns = NULL,
    memory = TRUE,
    group_by = NULL,
    packages = NULL,
    context = NULL,
    name = NULL,
    barrier = NULL,
    fetch_result_as_sdf = TRUE,
    partition_index_param = "",
    arrow_max_records_per_batch = NULL,
    auto_deps = FALSE,
    ...) {
  if (!is.character(partition_index_param)) {
    stop("Expected 'partition_index_param' to be a string.")
  }

  memory <- force(memory)
  args <- list(...)
  if (identical(fetch_result_as_sdf, FALSE)) {
    # If we are fetching R objects returned from the
    # transformation function in their serialized form,
    # then the result will contain a single binary column
    columns <- list(spark_apply_binary_result = "spark_apply_binary_result")
  } else {
    # If columns is of the form c("col_name1", "col_name2", ...)
    # then leave it as-is
    # Otherwise if it is of the form c(col_name1 = "col_type1", ...)
    # or list(col_name1 = "col_type1", ...), etc, then make sure it gets coerced
    # into a list instead of a character vector with names
    if (!identical(names(columns), NULL)) {
      columns <- as.list(columns)
    }
  }
  assert_that(is.function(f) || is.raw(f) || is.language(f))
  if (is.language(f)) f <- rlang::as_closure(f)

  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)
  sdf_columns <- colnames(x)

  if (identical(barrier, TRUE)) {
    # barrier works in rdd
    args$rdd <- TRUE
    if (is.null(columns)) {
      stop("Barrier execution requires explicit columns names.")
    }
  }

  if (identical(args$rdd, TRUE)) {
    rdd_base <- invoke(sdf, "rdd")
    if (identical(columns, NULL)) {
      columns <- colnames(x)
    }
  }

  # backward compatible support for names argument from 0.6
  if (!is.null(args$names)) {
    columns <- args$names
  }

  grouped <- !is.null(group_by)

  rlang <- spark_config_value(sc$config, "sparklyr.apply.rlang", FALSE)
  packages_config <- spark_config_value(sc$config, "sparklyr.apply.packages", NULL)
  proc_env <- c(connection_config(sc, "sparklyr.apply.env."), args$env)
  serialize_version <- spark_config_value(sc$config, "sparklyr.apply.serializer", 2)

  time_zone <- ""
  records_per_batch <- NULL
  arrow <- if (!is.null(args$arrow)) args$arrow else arrow_enabled(sc, sdf)
  if (identical(fetch_result_as_sdf, FALSE) &&
    identical(arrow, TRUE)) {
    warning(
      "Disabling arrow due to its potential",
      " incompatibility with fetch_result_as_sdf = FALSE"
    )
    arrow <- FALSE
  }
  if (arrow) {
    time_zone <- spark_session(sc) %>%
      invoke("sessionState") %>%
      invoke("conf") %>%
      invoke("sessionLocalTimeZone")
    records_per_batch <- as.integer(
      arrow_max_records_per_batch %||%
        spark_session_config(sc)[["spark.sql.execution.arrow.maxRecordsPerBatch"]] %||%
        10000
    )
  }

  # build reduced size query plan in case schema needs to be inferred
  if (sdf_is_streaming(sdf)) {
    sdf_limit <- sdf
  } else {
    sdf_limit <- invoke(
      sdf,
      "limit",
      cast_scalar_integer(
        spark_config_value(sc$config, "sparklyr.apply.schema.infer", 10)
      )
    )
  }

  if (!is.null(group_by) && sdf_is_streaming(sdf)) {
    stop("'group_by' is unsupported with streams.")
  }

  # set default value for packages based on config
  if (identical(packages, NULL)) {
    if (identical(packages_config, NULL)) {
      packages <- TRUE
    } else {
      packages <- packages_config
    }
  }

  columns_typed <- length(names(columns)) > 0

  if (rlang) warning("The `rlang` parameter is under active development.")

  # disable package distribution for local connections
  if (spark_master_is_local(sc$master)) {
    packages <- FALSE
  }

  # disable package distribution for livy connections and no package spec
  if (identical(tolower(sc$method), "livy") && identical(packages, TRUE)) {
    packages <- FALSE
  }

  # inject column types and partition_index_param to context
  context <- list(
    column_types = translate_spark_column_types(x),
    partition_index_param = partition_index_param,
    user_context = context
  )

  rlang_serialize <- spark_apply_rlang_serialize()
  create_rlang_closure <- (rlang && !is.null(rlang_serialize))

  # create closure for the given function
  serializer <- spark_apply_serializer()
  serialize_impl <- (
    if (is.list(serializer)) {
      function(x, ...) serializer$serializer(x)
    } else {
      serializer
    })
  deserializer <- spark_apply_deserializer()
  closure <- (
    if (create_rlang_closure) {
      serialize_impl(NULL, version = serialize_version)
    } else if (is.function(f)) {
      suppressWarnings(serialize_impl(f, version = serialize_version))
    } else {
      f
    })
  context_serialize <- serialize_impl(context, version = serialize_version)

  # create rlang closure
  closure_rlang <- if (create_rlang_closure) rlang_serialize(f) else raw()

  # add debug connection message
  if (isTRUE(args$debug)) {
    message("Debugging spark_apply(), connect to worker debugging session as follows:")
    message("  1. Find the workers <sessionid> and <port> in the worker logs, from RStudio click")
    message("     'Log' under the connection, look for the last entry with contents:")
    message("     'Session (<sessionid>) is waiting for sparklyr client to connect to port <port>'")
    message("  2. From a new R session run:")
    message("     debugonce(sparklyr:::spark_worker_main)")
    message("     sparklyr:::spark_worker_main(<sessionid>, <port>)")
  }

  if (grouped) {
    colpos <- which(colnames(x) %in% group_by)
    if (length(colpos) != length(group_by)) stop("Not all group_by columns found.")

    group_by_list <- as.list(as.integer(colpos - 1))

    if (!columns_typed) {
      columns <- c(group_by, columns)
    }

    if (identical(args$rdd, TRUE)) {
      rdd_base <- invoke_static(
        sc,
        "sparklyr.ApplyUtils",
        "groupBy",
        rdd_base,
        group_by_list
      )
    } else if (arrow) {
      sdf <- invoke_static(
        sc,
        "sparklyr.ApplyUtils",
        "groupByArrow",
        sdf,
        group_by_list,
        time_zone,
        records_per_batch
      )
      sdf_limit <- invoke_static(
        sc,
        "sparklyr.ApplyUtils",
        "groupByArrow",
        sdf_limit,
        group_by_list,
        time_zone,
        records_per_batch
      )
    } else {
      sdf <- invoke_static(
        sc,
        "sparklyr.ApplyUtils",
        "groupBy",
        sdf,
        group_by_list
      )
      sdf_limit <- invoke_static(
        sc,
        "sparklyr.ApplyUtils",
        "groupBy",
        sdf_limit,
        group_by_list
      )
    }
  }

  worker_port <- spark_config_value(sc$config, "sparklyr.gateway.port", "8880")

  # packages should be either a boolean or a character vector
  packages <- unlist(packages)
  if (auto_deps && !spark_apply_packages_is_bundle(packages)) {
    required_pkgs <- infer_required_r_packages(f)
    if (is.character(packages)) {
      packages <- union(packages, required_pkgs)
    } else {
      packages <- required_pkgs
    }
  }
  bundle_path <- get_spark_apply_bundle_path(sc, packages)

  spark_apply_options <- lapply(
    connection_config(sc, "sparklyr.apply.options."),
    as.character
  )
  if (!is.null(records_per_batch)) {
    spark_apply_options[["maxRecordsPerBatch"]] <- as.character(records_per_batch)
  }

  if (identical(args$rdd, TRUE)) {
    if (identical(barrier, TRUE)) {
      rdd <- invoke_static(
        sc,
        "sparklyr.RDDBarrier",
        "transformBarrier",
        rdd_base,
        closure,
        as.list(sdf_columns),
        spark_apply_worker_config(
          sc,
          args$debug,
          args$profile,
          arrow = arrow,
          fetch_result_as_sdf = fetch_result_as_sdf,
          single_binary_column = args$single_binary_column
        ),
        as.integer(worker_port),
        as.list(group_by),
        closure_rlang,
        bundle_path,
        as.integer(60),
        as.environment(proc_env),
        context_serialize,
        as.environment(spark_apply_options),
        serialize(serializer, NULL, version = serialize_version),
        serialize(deserializer, NULL, version = serialize_version)
      )
    } else {
      rdd <- invoke_static(
        sc,
        "sparklyr.WorkerHelper",
        "computeRdd",
        rdd_base,
        closure,
        spark_apply_worker_config(
          sc,
          args$debug,
          args$profile,
          arrow = arrow,
          fetch_result_as_sdf = fetch_result_as_sdf,
          single_binary_column = args$single_binary_column
        ),
        as.integer(worker_port),
        as.list(sdf_columns),
        as.list(group_by),
        closure_rlang,
        bundle_path,
        as.environment(proc_env),
        as.integer(60),
        context_serialize,
        as.environment(spark_apply_options),
        serialize(serializer, NULL, version = serialize_version),
        serialize(deserializer, NULL, version = serialize_version)
      )
    }

    # cache by default
    if (memory) rdd <- invoke(rdd, "cache")

    schema <- spark_schema_from_rdd(sc, rdd, columns)

    transformed <- invoke(hive_context(sc), "createDataFrame", rdd, schema)
  } else {
# ----------------------- Post Spark 2.0 a.k.a non-RDD -------------------------

    if(spark_version(sc) >= "4" && !grouped && !arrow) {
      sdf <- invoke_static(sc, "sparklyr.LatestUtils", "convertToArray", sdf)
      sdf_limit <- invoke_static(sc, "sparklyr.LatestUtils", "convertToArray", sdf_limit)
    }

    json_cols <- c()
    if (identical(columns, NULL) || is.character(columns)) {
      columns_schema <- spark_data_build_types(
        sc,
        list(
          names = "character",
          types = "character",
          json_cols = "character"
        )
      )

      columns_op <- invoke_static(
        sc,
        "sparklyr.WorkerHelper",
        "computeSdf",
        sdf_limit,
        columns_schema,
        closure,
        spark_apply_worker_config(
          sc,
          args$debug,
          args$profile,
          schema = TRUE,
          arrow = arrow,
          fetch_result_as_sdf = fetch_result_as_sdf,
          single_binary_column = args$single_binary_column
        ),
        as.integer(worker_port),
        as.list(sdf_columns),
        as.list(group_by),
        closure_rlang,
        bundle_path,
        as.environment(proc_env),
        as.integer(60),
        context_serialize,
        as.environment(spark_apply_options),
        spark_session(sc),
        time_zone,
        serialize(serializer, NULL, version = serialize_version),
        serialize(deserializer, NULL, version = serialize_version)
      )

      columns_query <- columns_op %>% sdf_collect()

      columns_infer <- strsplit(columns_query[1, ]$types, split = "\\|")[[1]]
      names(columns_infer) <- strsplit(columns_query[1, ]$names, split = "\\|")[[1]]
      json_cols <- array(strsplit(columns_query[1, ]$json_cols, split = "\\|")[[1]])

      if (is.character(columns)) {
        names(columns_infer)[seq_along(columns)] <- columns
      }

      columns <- columns_infer

      if (identical(args$schema, TRUE)) {
        return(columns)
      }
    }

    schema <- spark_data_build_types(sc, columns)

    transformed <- invoke_static(
      sc,
      "sparklyr.WorkerHelper",
      "computeSdf",
      sdf,
      schema,
      closure,
      spark_apply_worker_config(
        sc,
        args$debug,
        args$profile,
        arrow = arrow,
        fetch_result_as_sdf = fetch_result_as_sdf,
        single_binary_column = args$single_binary_column
      ),
      as.integer(worker_port),
      as.list(sdf_columns),
      as.list(group_by),
      closure_rlang,
      bundle_path,
      as.environment(proc_env),
      as.integer(60),
      context_serialize,
      as.environment(spark_apply_options),
      spark_session(sc),
      time_zone,
      serialize(serializer, NULL, version = serialize_version),
      serialize(deserializer, NULL, version = serialize_version)
    )

    if (spark_version(sc) >= "2.4.0" && !is.na(json_cols) && length(json_cols) > 0) {
      transformed <- invoke_static(
        sc,
        "sparklyr.StructColumnUtils",
        "parseJsonColumns",
        transformed,
        json_cols
      )
    }
  }

  if (identical(barrier, TRUE)) {
    registered <- transformed
  } else {
    name <- name %||% random_string("sparklyr_tmp_")
    registered <- sdf_register(transformed, name = name)
    if (memory && !identical(args$rdd, TRUE) && !sdf_is_streaming(sdf)) {
      tbl_cache(sc, name, force = FALSE)
    }
  }

  if (identical(fetch_result_as_sdf, FALSE)) {
    registered %>%
      sdf_collect(arrow = arrow) %>%
      (
        function(x) {
          lapply(x$spark_apply_binary_result, function(res) deserializer(res[[1]]))
        })
  } else {
    registered
  }
}

spark_apply_rlang_serialize <- function() {
  rlang_serialize <- core_get_package_function("rlang", "serialise_bytes")
  if (is.null(rlang_serialize)) {
    core_get_package_function("rlanglabs", "serialise_bytes")
  } else {
    rlang_serialize
  }
}

spark_apply_serializer <- function() {
  serializer <- getOption("sparklyr.spark_apply.serializer")
  impl <- (
    if (identical(serializer, "qs")) {
      qserialize <- core_get_package_function("qs", "qserialize")
      if (is.null(qserialize)) {
        stop(
          "Unable to locate qs::qserialize(). Please ensure 'qs' is installed."
        )
      }
      function(x, ...) qserialize(x)
    } else if (is.null(serializer)) {
      function(x, version = NULL) serialize(x, NULL, version = version)
    } else {
      list(serializer = serializer)
    })

  impl
}

spark_apply_deserializer <- function() {
  if (identical(getOption("sparklyr.spark_apply.serializer"), "qs")) {
    impl <- core_get_package_function("qs", "qdeserialize")
    if (is.null(impl)) {
      stop(
        "Unable to locate qs::qdeserialize(). Please ensure 'qs' is installed."
      )
    }

    impl
  } else {
    getOption("sparklyr.spark_apply.deserializer") %||% unserialize
  }
}

#' Log Writer for Spark Apply
#'
#' Writes data to log under \code{spark_apply()}.
#'
#' @param ... Arguments to write to log.
#' @param level Severity level for this entry; recommended values: \code{INFO},
#'   \code{ERROR} or \code{WARN}.
#'
#' @export
spark_apply_log <- function(..., level = "INFO") {
  worker_log_level(..., level = level, component = "RClosure")
}
