#' @include dplyr_hof.R
#' @include spark_sql.R
#' @include utils.R
NULL

#' @export
#' @importFrom dplyr transmute
#' @importFrom dplyr all_of
transmute.tbl_spark <- function(.data, ...) {
  dots_org <- rlang::enquos(..., .named = TRUE)
  dots <- fix_na_real_values(dots_org)
  if (identical(dots, dots_org)) {
    NextMethod()
  } else {
    transmute(.data, !!!dots)
  }
}

#' @export
#' @importFrom dplyr mutate
mutate.tbl_spark <- function(.data, ...) {
  dots_org <- rlang::enquos(...)
  dots <- fix_na_real_values(dots_org)
  if (identical(dots, dots_org)) {
    NextMethod()
  } else {
    mutate(.data, !!!dots)
  }
}

#' @export
#' @importFrom dplyr filter
filter.tbl_spark <- function(.data, ..., .preserve = FALSE) {
  if (!identical(.preserve, FALSE)) {
    stop("`.preserve` is not supported on database backends", call. = FALSE)
  }
  NextMethod()
}

#' @export
#' @importFrom dplyr select
select.tbl_spark <- function(.data, ...) {
  NextMethod()
}

#' @export
#' @importFrom dplyr summarise
#' @importFrom dbplyr op_vars
summarise.tbl_spark <- function(.data, ..., .groups = NULL) {
  NextMethod()
}

fix_na_real_values <- function(dots) {
  for (i in seq_along(dots)) {
    if (identical(rlang::quo_get_expr(dots[[i]]), rlang::expr(NA_real_))) {
      dots[[i]] <- rlang::quo(dbplyr::sql("CAST(NULL AS DOUBLE)"))
    }
  }

  dots
}

#' Join Spark tbls.
#'
#' These functions are wrappers around their `dplyr` equivalents that set
#' Spark SQL-compliant values for the `suffix` argument by replacing dots (`.`)
#' with underscores (`_`). See [join] for a description of the general purpose
#' of the functions.
#'
#' @inheritParams dbplyr::join.tbl_sql
#'
#' @name join.tbl_spark
NULL

join_tbl_spark_impl <- function(
  x,
  y,
  by = NULL,
  copy = FALSE,
  suffix = c("_x", "_y"),
  auto_index = FALSE,
  ...,
  sql_on = NULL
) {
  if (any(grepl("\\.", suffix))) {
    suffix <- gsub("\\.", "_", suffix)
    message(
      "Replacing '.' with '_' in suffixes. New suffixes: ",
      paste(suffix, collapse = ", ")
    )
  }

  NextMethod(suffix = suffix)
}

#' @rdname join.tbl_spark
#' @export
#' @importFrom dplyr inner_join
inner_join.tbl_spark <- join_tbl_spark_impl

#' @rdname join.tbl_spark
#' @export
#' @importFrom dplyr left_join
left_join.tbl_spark <- join_tbl_spark_impl

#' @rdname join.tbl_spark
#' @export
#' @importFrom dplyr right_join
right_join.tbl_spark <- join_tbl_spark_impl

#' @rdname join.tbl_spark
#' @export
#' @importFrom dplyr full_join
full_join.tbl_spark <- join_tbl_spark_impl

#' @export
#' @importFrom dplyr do
#' @importFrom dplyr groups
#' @importFrom dplyr select
#' @importFrom dplyr count
do.tbl_spark <- function(.data, ...) {
  sdf <- spark_dataframe(.data)
  quosures <- rlang::quos(...)

  # get column references to grouped variables
  groups <- as.character(as.list(groups(.data)))
  columns <- lapply(groups, function(group) {
    invoke(sdf, "col", group)
  })

  # compute unique combinations of values
  combos <- sdf %>%
    invoke("select", columns) %>%
    invoke("groupBy", columns) %>%
    invoke("count") %>%
    sdf_collect() %>%
    select(-count)

  # apply function on subsets of data
  outputs <- vector("list", nrow(combos))
  nm <- names(combos)

  lapply(seq_len(nrow(combos)), function(i) {
    # generate filters for each combination
    filters <- lapply(seq_along(nm), function(j) {
      sdf %>%
        invoke("col", nm[[j]]) %>%
        invoke("equalTo", combos[[j]][[i]])
    })

    # apply filters
    filtered <- sdf
    for (filter in filters) {
      filtered <- spark_dataframe(filtered) %>%
        invoke("filter", filter) %>%
        sdf_register()
    }

    # apply functions with this data
    fits <- enumerate(quosures, function(name, quosure) {
      # override '.' in envir
      assign(".", filtered, envir = environment(quosure))

      # evaluate in environment
      tryCatch(
        rlang::eval_tidy(quosure),
        error = identity
      )
    })

    # store
    outputs[[i]] <<- fits
  })

  # produce 'result' dataset by adding outputs to 'combos'
  result <- combos
  columns <- lapply(names(quosures), function(name) {
    lapply(outputs, `[[`, name)
  })

  for (i in seq_along(quosures)) {
    key <- names(quosures)[[i]]
    val <- lapply(outputs, `[[`, key)
    result[[key]] <- val
  }

  result
}

#' Register a Parallel Backend
#'
#' Registers a parallel backend using the \code{foreach} package.
#'
#' @param spark_conn Spark connection to use
#' @param parallelism Level of parallelism to use for task execution
#'   (if unspecified, then it will take the value of
#'    `SparkContext.defaultParallelism()` which by default is the number
#'    of cores available to the `sparklyr` application)
#' @param ... additional options for sparklyr parallel backend
#'   (currently only the only valid option is `nocompile`)
#'
#' @return None
#'
#' @examples
#' \dontrun{
#'
#' sc <- spark_connect(master = "local")
#' registerDoSpark(sc, nocompile = FALSE)
#' }
#'
#' @export
registerDoSpark <- function(spark_conn, parallelism = NULL, ...) {
  do_spark_parallelism <- as.integer(
    parallelism %||%
      tryCatch(
        spark_context(spark_conn) %>% invoke("defaultParallelism"),
        # return 0 as number of workers if there is an exception
        error = function(e) {
          0L
        }
      )
  )

  # internal function to process registerDoSpark options
  .processOpts <- function(...) {
    opts <- list(...)
    optnames <- names(opts)
    if (is.null(optnames)) {
      optnames <- rep("", length(opts))
    }

    # filter out unnamed arguments with a warning
    unnamed <- !nzchar(optnames)
    if (any(unnamed)) {
      warning(
        "ignoring doSpark package option(s) specified with unnamed argument"
      )
      opts <- opts[!unnamed]
      optnames <- optnames[!unnamed]
    }

    # filter out unrecognized options with a warning
    recog <- optnames %in% genv_get_do_spark("valid_options")
    if (any(!recog)) {
      warning(
        sprintf(
          "ignoring unrecognized doSpark package option(s): %s",
          paste(optnames[!recog], collapse = ", ")
        ),
        call. = FALSE
      )
      opts <- opts[recog]
      optnames <- optnames[recog]
    }

    # clear .options from any previous call to registerDoSpark
    genv_clear_do_spark_options()

    # set new options
    genv_set_do_spark_options(optnames, opts)
  }

  serializer <- spark_apply_serializer()
  if (is.list(serializer)) {
    serializer <- serializer$serializer
  }
  deserializer <- spark_apply_deserializer()
  # internal function called by foreach
  .doSpark <- function(obj, expr, envir, data) {
    # internal function to compile an expression if possible
    .compile <- function(expr, ...) {
      if (
        getRversion() < "2.13.0" ||
          isTRUE(genv_get_do_spark("options")$nocompile)
      ) {
        expr
      } else {
        compiler::compile(expr, ...)
      }
    }

    expr_globals <- globals::globalsOf(
      expr,
      envir = envir,
      recursive = TRUE,
      mustExist = FALSE
    )

    # internal function to process spark data frames
    .process_spark_items <- function(spark_items, ...) {
      pkgs_to_attach <- (.packages())
      worker_fn <- function(items) {
        for (pkg in pkgs_to_attach) {
          library(pkg, character.only = TRUE)
        }
        enclos <- envir
        expr_globals <- as.list(expr_globals)
        for (k in names(expr_globals)) {
          v <- expr_globals[[k]]
          if (identical(typeof(v), "closure")) {
            # to make `enclos` truly self-contained, each of its element that is
            # a closure will need to have its enclosing environment replaced with
            # `enclos` itself
            environment(v) <- enclos
          }
          if (!exists(k, where = enclos)) {
            assign(k, v, pos = enclos)
          }
        }
        # `enclos` now contains a snapshot of all external variables and functions
        # needed for evaluating `expr`
        tryCatch(
          {
            lapply(
              items$encoded,
              function(item) {
                eval(expr, envir = as.list(deserializer(item)), enclos = enclos)
              }
            )
          },
          error = function(ex) {
            list(ex)
          }
        )
      }

      spark_apply(spark_items, worker_fn, ..., memory = TRUE)
    }

    if (!inherits(obj, "foreach")) {
      stop("obj must be a foreach object")
    }

    spark_conn <- data$spark_conn
    spark_apply_args <- data$spark_apply_args
    spark_apply_args$fetch_result_as_sdf <- FALSE
    spark_apply_args$single_binary_column <- TRUE
    spark_apply_args$packages <- obj$packages

    it <- iterators::iter(obj)
    accumulator <- foreach::makeAccum(it)
    items <- dplyr::tibble(
      encoded = it %>% as.list() %>% lapply(serializer)
    )
    spark_items <- sdf_copy_to(
      spark_conn,
      items,
      name = random_string("sparklyr_tmp_"),
      repartition = do_spark_parallelism
    )
    expr <- .compile(expr)
    res <- do.call(
      .process_spark_items,
      append(list(spark_items), as.list(spark_apply_args))
    )
    tryCatch(
      accumulator(res, seq(along = res)),
      error = function(e) {
        cat("error calling combine function:\n", file = stderr())
        print(e)
      }
    )
    err_val <- foreach::getErrorValue(it)
    err_idx <- foreach::getErrorIndex(it)
    if (identical(obj$errorHandling, "stop") && !is.null(err_val)) {
      msg <- sprintf(
        "task %d failed - \"%s\"",
        err_idx,
        conditionMessage(err_val)
      )
      stop(simpleError(msg, call = expr))
    } else {
      foreach::getResult(it)
    }
  }

  # internal function reporting info about the sparklyr parallel backend
  .info <- function(data, item) {
    pkgName <- "doSpark"
    switch(
      item,
      name = pkgName,
      version = packageDescription(pkgName, fields = "Version"),
      workers = do_spark_parallelism,
      NULL
    )
  }

  genv_set_do_spark(
    list(
      # list of valid options for doSpark
      valid_options = c("nocompile"),
      # options from the last successful call to registerDoSpark
      options = new.env(parent = emptyenv())
    )
  )

  .processOpts(...)
  foreach::setDoPar(
    fun = .doSpark,
    data = list(spark_conn = spark_conn, spark_apply_args = list(...)),
    info = .info
  )
}

#' Bind multiple Spark DataFrames by row and column
#'
#' \code{sdf_bind_rows()} and \code{sdf_bind_cols()} are implementation of the common pattern of
#' \code{do.call(rbind, sdfs)} or \code{do.call(cbind, sdfs)} for binding many
#' Spark DataFrames into one.
#'
#' The output of \code{sdf_bind_rows()} will contain a column if that column
#' appears in any of the inputs.
#'
#' @param ... Spark tbls to combine.
#'
#'   Each argument can either be a Spark DataFrame or a list of
#'   Spark DataFrames
#'
#'   When row-binding, columns are matched by name, and any missing
#'   columns with be filled with NA.
#'
#'   When column-binding, rows are matched by position, so all data
#'   frames must have the same number of rows.
#' @param id Data frame identifier.
#'
#'   When \code{id} is supplied, a new column of identifiers is
#'   created to link each row to its original Spark DataFrame. The labels
#'   are taken from the named arguments to \code{sdf_bind_rows()}. When a
#'   list of Spark DataFrames is supplied, the labels are taken from the
#'   names of the list. If no names are found a numeric sequence is
#'   used instead.
#' @return \code{sdf_bind_rows()} and \code{sdf_bind_cols()} return \code{tbl_spark}
#' @name sdf_bind
NULL

#' @export
rbind.tbl_spark <- function(
  ...,
  deparse.level = 1,
  name = random_string("sparklyr_tmp_")
) {
  dots <- list(...)
  n <- length(dots)
  self <- dots[[1]]

  if (n == 1) {
    return(self)
  }

  sdf <- spark_dataframe(self)

  # NOTE: 'unionAll' was deprecated in Spark 2.0.0, but 'unionAll'
  # is not available for DataFrames in older versions of Spark, so
  # provide a bit of indirection based on Spark version
  sc <- spark_connection(sdf)
  version <- spark_version(sc)
  method <- if (version < "2.0.0") "unionAll" else "union"

  columns <- sdf %>%
    invoke("columns") %>%
    unlist()

  reorder_columns <- function(sdf) {
    cols <- columns %>%
      lapply(function(column) invoke(sdf, "col", column))
    sdf %>%
      invoke("select", cols)
  }

  for (i in 2:n) {
    sdf <- invoke(sdf, method, reorder_columns(spark_dataframe(dots[[i]])))
  }

  sdf_register(sdf, name = name)
}

#' @export
#' @importFrom rlang sym
#' @importFrom rlang :=
#' @rdname sdf_bind
sdf_bind_rows <- function(..., id = NULL) {
  id <- cast_nullable_string(id)
  dots <- Filter(length, rlang::dots_list(...))
  dots <- purrr::list_flatten(dots)
  dots <- purrr::compact(dots)
  if (!all(sapply(dots, is.tbl_spark))) {
    stop("all inputs must be tbl_spark")
  }

  n <- length(dots)
  self <- dots[[1]]

  if (n == 1) {
    return(self)
  }

  sc <- self %>%
    spark_dataframe() %>%
    spark_connection()

  schemas <- lapply(dots, function(x) {
    schema <- x %>%
      spark_dataframe() %>%
      invoke("schema")
    col_names <- schema %>%
      invoke("fieldNames") %>%
      unlist()
    col_types <- schema %>%
      invoke("fields") %>%
      lapply(function(x) invoke(x, "dataType")) %>%
      lapply(function(x) invoke(x, "typeName")) %>%
      unlist()
    dplyr::tibble(name = col_names, type = col_types)
  })

  master_schema <- schemas %>%
    dplyr::bind_rows() %>%
    dplyr::group_by(!!rlang::sym("name")) %>%
    dplyr::slice(1)

  schema_complements <- schemas %>%
    lapply(function(x) {
      dplyr::as_tibble(master_schema) %>%
        dplyr::select(!!rlang::sym("name")) %>%
        dplyr::setdiff(select(x, !!rlang::sym("name"))) %>%
        dplyr::left_join(master_schema, by = "name")
    })

  sdf_augment <- function(x, schema_complement) {
    sdf <- spark_dataframe(x)
    if (nrow(schema_complement) > 0L) {
      for (i in seq_len(nrow(schema_complement))) {
        new_col <- invoke_static(
          sc,
          "org.apache.spark.sql.functions",
          "lit",
          NA
        ) %>%
          invoke("cast", schema_complement$type[i])

        sdf <- sdf %>%
          invoke("withColumn", schema_complement$name[i], new_col)
      }
    }
    sdf
  }

  augmented_dots <- Map(sdf_augment, dots, schema_complements) %>%
    lapply(sdf_register)

  if (!is.null(id)) {
    unnamed_idx <- which(!rlang::have_name(dots))
    names(dots)[unnamed_idx] <- as.character(unnamed_idx)
    augmented_dots <- Map(
      function(x, label) {
        dplyr::mutate(x, !!sym(id) := label) %>%
          dplyr::select(!!sym(id), everything())
      },
      augmented_dots,
      names(dots)
    )
  }

  do.call(rbind, augmented_dots)
}

#' @export
cbind.tbl_spark <- function(
  ...,
  deparse.level = 1,
  name = random_string("sparklyr_tmp_")
) {
  dots <- list(...)
  n <- length(dots)
  self <- dots[[1]]

  if (n == 1) {
    return(self)
  }

  id <- random_string("id_")

  dots_with_ids <- dots %>%
    lapply(function(x) sdf_with_sequential_id(x, id = id))

  dots_num_rows <- dots_with_ids %>%
    lapply(function(x) sdf_last_index(x, id = id)) %>%
    unlist()

  if (length(unique(dots_num_rows)) > 1) {
    stop("All inputs must have the same number of rows.", call. = FALSE)
  }

  Reduce(
    function(x, y) dplyr::inner_join(x, y, by = id),
    dots_with_ids
  ) %>%
    dplyr::arrange(!!rlang::sym(id)) %>%
    spark_dataframe() %>%
    invoke("drop", id) %>%
    sdf_register()
}

#' @rdname sdf_bind
#' @export
sdf_bind_cols <- function(...) {
  dots <- Filter(length, rlang::dots_list(...))
  dots <- purrr::list_flatten(dots)
  dots <- purrr::compact(dots)
  if (!all(sapply(dots, is.tbl_spark))) {
    stop("all inputs must be tbl_spark")
  }

  do.call(cbind, dots)
}

mutate_names <- function(x, value) {
  sdf <- spark_dataframe(x)
  renamed <- invoke(sdf, "toDF", as.list(value))
  sdf_register(renamed, name = as.character(x$lazy_query$x))
}

#' @export
`names<-.tbl_spark` <- function(x, value) {
  mutate_names(x, value)
}

#' Replace Missing Values in Objects
#'
#' This S3 generic provides an interface for replacing
#' \code{\link{NA}} values within an object.
#'
#' @param object An \R object.
#' @param ... Arguments passed along to implementing methods.
#'
#' @export
na.replace <- function(object, ...) {
  UseMethod("na.replace")
}

#' @export
na.replace.tbl_spark <- function(object, ...) {
  na.replace(spark_dataframe(object), ...)
}

#' @importFrom tidyr replace_na
#' @export
replace_na.tbl_spark <- function(data, replace, ...) {
  do.call(na.replace.tbl_spark, append(list(data), replace))
}

#' @export
na.replace.spark_jobj <- function(object, ...) {
  dots <- list(...)
  enumerate(dots, function(key, val) {
    na <- invoke(object, "na")
    object <<- if (is.null(key)) {
      invoke(na, "fill", val)
    } else {
      invoke(na, "fill", val, as.list(key))
    }
  })
  sdf_register(object)
}

#' @importFrom tidyr replace_na
#' @export
replace_na.spark_jobj <- function(data, replace, ...) {
  do.call(na.replace.spark_jobj, append(list(data), replace))
}

#' @export
na.omit.tbl_spark <- function(object, columns = NULL, ...) {
  na.omit(spark_dataframe(object), columns = NULL, ...)
}

#' @export
na.omit.spark_jobj <- function(object, columns = NULL, ...) {
  sc <- spark_connection(object)

  # report number of rows dropped if requested
  verbose <- spark_config_value(
    sc$config,
    c(
      "sparklyr.verbose.na",
      "sparklyr.na.omit.verbose",
      "sparklyr.na.action.verbose",
      "sparklyr.verbose"
    ),
    TRUE
  )

  n_before <- invoke(object, "count")
  dropped <- sdf_na_omit(object, columns)
  n_after <- invoke(dropped, "count")

  if (verbose) {
    n_diff <- n_before - n_after
    if (n_diff > 0) {
      fmt <- "* Dropped %s rows with 'na.omit' (%s => %s)"
      message(sprintf(fmt, n_diff, n_before, n_after))
    } else {
      message("* No rows dropped by 'na.omit' call")
    }
  }

  # using a df created from drop actions reduces performance, see #308.
  if (identical(n_before, n_after) && getOption("na.omit.cache", TRUE)) {
    sdf_register(object)
  } else {
    result <- sdf_register(dropped)
    invoke(spark_dataframe(result), "cache")
    result
  }
}

#' @export
na.fail.tbl_spark <- function(object, columns = NULL, ...) {
  na.fail(spark_dataframe(object), ...)
}

#' @export
na.fail.spark_jobj <- function(object, columns = NULL, ...) {
  n_before <- invoke(object, "count")
  dropped <- sdf_na_omit(object, columns)
  n_after <- invoke(dropped, "count")

  if (n_before != n_after) {
    stop("* missing values in object")
  }

  object
}

# Spark DataFrame NA Routines ----

apply_na_action <- function(x, response = NULL, features = NULL, na.action) {
  # early exit for NULL, NA na.action
  if (is.null(na.action)) {
    return(x)
  }

  # attempt to resolve character na.action
  if (is.character(na.action)) {
    if (!exists(na.action, envir = parent.frame(), mode = "function")) {
      stop("no function with name '", na.action, "' found")
    }

    na.action <- get(na.action, envir = parent.frame(), mode = "function")
  }

  if (!is.function(na.action)) {
    stop("'na.action' is not a function")
  }

  # attempt to apply 'na.action'
  na.action(
    x,
    response = response,
    features = features,
    columns = c(response, features)
  )
}

sdf_na_omit <- function(object, columns = NULL) {
  na <- invoke(object, "na")
  dropped <- if (is.null(columns)) {
    invoke(na, "drop")
  } else {
    invoke(na, "drop", as.list(columns))
  }
  dropped
}
