spark_apply_packages <- function(packages) {
  db <- Sys.getenv("sparklyr.apply.packagesdb")
  if (nchar(db) == 0) {
    if (!exists("availablePackagesChache", envir = .globals)) {
      db <- tryCatch({
        available.packages()
      }, error = function(e) {
        warning(
          "Failed to run 'available.packages()', using offline connection? ",
          "See '?spark_apply' for details."
        )
        NULL
      })

      assign("availablePackagesChache", db, envir = .globals)
    }
    else {
      db <- get("availablePackagesChache", envir = .globals)
    }
  }

  if (is.null(db)) {
    TRUE
  } else {
    deps <- tools::package_dependencies(packages, db = db, recursive = TRUE)
    names(deps) <- NULL
    unique(c(unlist(deps), packages))
  }
}

spark_apply_packages_is_bundle <- function(packages) {
  is.character(packages) && length(packages) == 1 && grepl("\\.tar$", packages)
}

spark_apply_worker_config <- function(sc, debug, profile, schema = FALSE, arrow = FALSE) {
  worker_config_serialize(
    c(
      list(
        debug = isTRUE(debug),
        profile = isTRUE(profile),
        schema = isTRUE(schema),
        arrow =isTRUE(arrow)
      ),
      sc$config
    )
  )
}

spark_apply_colum_types <- function(sdf) {
  type_map <- list(
    IntegerType = "integer",
    FloatType = "numeric",
    DoubleType = "numeric",
    LongType = "numeric",
    StringType = "character",
    BinaryType = "raw",
    BooleanType = "logical",
    TimestampType = "POSIXct",
    DateType = "Date",
    DateType = "date"
  )

  lapply(sdf_schema(sdf), function(e) type_map[[e$type]])
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
spark_apply <- function(x,
                        f,
                        columns = NULL,
                        memory = !is.null(name),
                        group_by = NULL,
                        packages = NULL,
                        context = NULL,
                        name = NULL,
                        barrier = NULL,
                        ...) {
  memory <- force(memory)
  args <- list(...)
  assert_that(is.function(f) || is.raw(f) || is.language(f))
  if (is.language(f)) f <- rlang::as_closure(f)

  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)
  sdf_columns <- colnames(x)

  if (identical(barrier, TRUE)){
    # barrier works in rdd
    args$rdd <- TRUE

    if (spark_version(sc) < "2.4.0"){
      stop("Barrier execution is only available for spark 2.4.0 or greater.")
    }

    if (is.null(columns)) {
      stop("Barrier execution requires explicit columns names.")
    }
  }

  if (spark_version(sc) < "2.0.0") args$rdd <- TRUE
  if (identical(args$rdd, TRUE)) {
    rdd_base <- invoke(sdf, "rdd")
    if (identical(columns, NULL)) columns <- colnames(x)
  }

  grouped <- !is.null(group_by)

  rlang <- spark_config_value(sc$config, "sparklyr.apply.rlang", FALSE)
  packages_config <- spark_config_value(sc$config, "sparklyr.apply.packages", NULL)
  proc_env <- c(connection_config(sc, "sparklyr.apply.env."), args$env)
  serialize_version <- spark_config_value(sc$config, "sparklyr.apply.serializer", 2)

  time_zone <- ""
  records_per_batch <- NULL
  arrow <- if (!is.null(args$arrow)) args$arrow else arrow_enabled(sc, sdf)
  if (arrow) {
    time_zone <- spark_session(sc) %>% invoke("sessionState") %>% invoke("conf") %>% invoke("sessionLocalTimeZone")
    records_per_batch <- as.integer(spark_session_config(sc)[["spark.sql.execution.arrow.maxRecordsPerBatch"]] %||% 10000)
  }

  # build reduced size query plan in case schema needs to be inferred
  if (sdf_is_streaming(sdf)) {
    sdf_limit <- sdf
  }
  else {
    sdf_limit <- invoke(
      sdf,
      "limit",
      cast_scalar_integer(
        spark_config_value(sc$config, "sparklyr.apply.schema.infer", 10)
      )
    )
  }

  # backward compatible support for names argument from 0.6
  if (!is.null(args$names)) {
    columns <- args$names
  }

  if (!is.null(group_by) && sdf_is_streaming(sdf)) {
    stop("'group_by' is unsupported with streams.")
  }

  # set default value for packages based on config
  if (identical(packages, NULL)) {
    if (identical(packages_config, NULL)) {
      packages <- TRUE
    }
    else {
      packages <- packages_config
    }
  }

  columns_typed <- length(names(columns)) > 0

  if (rlang) warning("The `rlang` parameter is under active development.")

  # disable package distribution for local connections
  if (spark_master_is_local(sc$master)) packages <- FALSE

  # disable package distribution for livy connections and no package spec
  if (identical(tolower(sc$method), "livy") && identical(packages, TRUE)) packages <- FALSE

  # inject column types to context
  context <- list(
    column_types = spark_apply_colum_types(x),
    user_context = context
  )

  # create closure for the given function
  closure <- if (is.function(f)) suppressWarnings(serialize(f, NULL, version = serialize_version)) else f
  context_serialize <- serialize(context, NULL)

  # create rlang closure
  rlang_serialize <- spark_apply_rlang_serialize()
  closure_rlang <- if (rlang && !is.null(rlang_serialize)) rlang_serialize(f) else raw()

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
      rdd_base <- invoke_static(sc, "sparklyr.ApplyUtils", "groupBy", rdd_base, group_by_list)
    }
    else if (arrow) {
      sdf <- invoke_static(sc, "sparklyr.ApplyUtils", "groupByArrow", sdf, group_by_list, time_zone, records_per_batch)
      sdf_limit <- invoke_static(sc, "sparklyr.ApplyUtils", "groupByArrow", sdf_limit, group_by_list, time_zone, records_per_batch)
    }
    else {
      sdf <- invoke_static(sc, "sparklyr.ApplyUtils", "groupBy", sdf, group_by_list)
      sdf_limit <- invoke_static(sc, "sparklyr.ApplyUtils", "groupBy", sdf_limit, group_by_list)
    }
  }

  worker_port <- spark_config_value(sc$config, "sparklyr.gateway.port", "8880")

  bundle_path <- ""
  if (spark_apply_packages_is_bundle(packages)) {
    bundle_path <- packages
  }
  else if (isTRUE(packages) || is.character(packages)) {
    bundle_base <- spark_apply_bundle_path()
    bundle_path <- spark_apply_bundle_file(packages, bundle_base)
    if (!file.exists(bundle_path)) {
      bundle_path <- spark_apply_bundle(packages, bundle_base)
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

  spark_apply_options <- lapply(
    connection_config(sc, "sparklyr.apply.options."),
    as.character
  )
  if (!is.null(records_per_batch)) spark_apply_options[["maxRecordsPerBatch"]] <- as.character(records_per_batch)

  if (identical(args$rdd, TRUE)) {
    if (identical(barrier, TRUE)){
      rdd <- invoke_static(
        sc,
        "sparklyr.RDDBarrier",
        "transformBarrier",
        rdd_base,
        closure,
        as.list(sdf_columns),
        spark_apply_worker_config(sc, args$debug, args$profile),
        as.integer(worker_port),
        as.list(group_by),
        closure_rlang,
        bundle_path,
        as.integer(60),
        as.environment(proc_env),
        context_serialize,
        as.environment(spark_apply_options)
      )
    } else {
      rdd <- invoke_static(
        sc,
        "sparklyr.WorkerHelper",
        "computeRdd",
        rdd_base,
        closure,
        spark_apply_worker_config(sc, args$debug, args$profile),
        as.integer(worker_port),
        as.list(sdf_columns),
        as.list(group_by),
        closure_rlang,
        bundle_path,
        as.environment(proc_env),
        as.integer(60),
        context_serialize,
        as.environment(spark_apply_options)
      )
    }

    # cache by default
    if (memory) rdd <- invoke(rdd, "cache")

    schema <- spark_schema_from_rdd(sc, rdd, columns)

    transformed <- invoke(hive_context(sc), "createDataFrame", rdd, schema)
  }
  else {
    if (identical(columns, NULL) || is.character(columns)) {
      columns_schema <- spark_data_build_types(
        sc,
        list(
          names = "character",
          types = "character"
        )
      )

      columns_op <- invoke_static(
        sc,
        "sparklyr.WorkerHelper",
        "computeSdf",
        sdf_limit,
        columns_schema,
        closure,
        spark_apply_worker_config(sc, args$debug, args$profile, schema = TRUE, arrow = arrow),
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
        time_zone
      )

      columns_query <- columns_op %>% sdf_collect()
      if (arrow && !arrow_enabled_dataframe_schema(columns_query$types)) {
        arrow <- FALSE
      }

      columns_infer <- strsplit(columns_query[1, ]$types, split = "\\|")[[1]]
      names(columns_infer) <- strsplit(columns_query[1, ]$names, split = "\\|")[[1]]

      if (is.character(columns)) {
        names(columns_infer)[seq_along(columns)] <- columns
      }

      columns <- columns_infer

      if (identical(args$schema, TRUE)) return(columns)
    }

    schema <- spark_data_build_types(sc, columns)

    transformed <- invoke_static(
      sc,
      "sparklyr.WorkerHelper",
      "computeSdf",
      sdf,
      schema,
      closure,
      spark_apply_worker_config(sc, args$debug, args$profile, arrow = arrow),
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
      time_zone
    )
  }

  if (identical(barrier, TRUE)){
    registered <- transformed
  } else {
    name <- name %||% random_string("sparklyr_tmp_")
    registered <- sdf_register(transformed, name = name)

    if (memory && !identical(args$rdd, TRUE) && !sdf_is_streaming(sdf)) tbl_cache(sc, name, force = FALSE)

  }
  registered
}

spark_apply_rlang_serialize <- function() {
  rlang_serialize <- core_get_package_function("rlang", "serialise_bytes")
  if (is.null(rlang_serialize))
    core_get_package_function("rlanglabs", "serialise_bytes")
  else
    rlang_serialize
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
