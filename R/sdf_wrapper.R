

#' @export
#' @importFrom dbplyr sql_render
#' @importFrom dbplyr sql_build
spark_dataframe.tbl_spark <- function(x, ...) {
  x$spark_dataframe(
    x,
    function(tbl_spark) {
      sc <- spark_connection(tbl_spark)
      sql <- as.character(sql_render(sql_build(tbl_spark, con = sc), con = sc))
      hive <- hive_context(sc)

      invoke(hive, "sql", sql)
    }
  )
}

#' @export
spark_dataframe.spark_connection <- function(x, sql = NULL, ...) {
  invoke(hive_context(x), "sql", as.character(sql))
}

#' Read the Schema of a Spark DataFrame
#'
#' Read the schema of a Spark DataFrame.
#'
#' The \code{type} column returned gives the string representation of the
#' underlying Spark  type for that column; for example, a vector of numeric
#' values would be returned with the type \code{"DoubleType"}. Please see the
#' \href{http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.types.package}{Spark Scala API Documentation}
#' for information on what types are available and exposed by Spark.
#'
#' @param expand_nested_cols Whether to expand columns containing nested array
#' of structs (which are usually created by tidyr::nest on a Spark data frame)
#'
#' @param expand_struct_cols Whether to expand columns containing structs
#'
#' @return An \R \code{list}, with each \code{list} element describing the
#'   \code{name} and \code{type} of a column.
#'
#' @template roxlate-ml-x
#'
#' @export
sdf_schema <- function(x,
                       expand_nested_cols = FALSE,
                       expand_struct_cols = FALSE) {
  UseMethod("sdf_schema")
}

#' @export
sdf_schema.tbl_spark <- function(x,
                                 expand_nested_cols = FALSE,
                                 expand_struct_cols = FALSE) {
  x$schema(
    x,
    sdf_schema_impl,
    expand_nested_cols = expand_nested_cols,
    expand_struct_cols = expand_struct_cols
  )
}

#' @export
sdf_schema.default <- function(x,
                               expand_nested_cols = FALSE,
                               expand_struct_cols = FALSE) {
  sdf_schema_impl(
    x,
    expand_nested_cols = expand_nested_cols,
    expand_struct_cols = expand_struct_cols
  )
}

sdf_schema_impl <- function(x,
                            expand_nested_cols,
                            expand_struct_cols) {
  process_struct_type <- function(x) {
    fields <- x$fields
    fields_list <- lapply(
      fields,
      function(field) {
        name <- field$name
        type <- (
          if ("fields" %in% names(field$dtype)) {
            process_struct_type(field$dtype)
          } else if ("elementType" %in% names(field$dtype)) {
            dtype <- "array"
            attributes(dtype)$element_type <- process_struct_type(field$dtype$elementType)

            dtype
          } else {
            field$dtype$repr
          }
        )

        list(name = name, type = type)
      }
    )
    names(fields_list) <- unlist(lapply(fields_list, `[[`, "name"))

    fields_list
  }

  invoke_static(
    spark_connection(x),
    "sparklyr.SchemaUtils",
    "sdfSchema",
    spark_dataframe(x),
    expand_nested_cols,
    expand_struct_cols
  ) %>%
    jsonlite::fromJSON(simplifyDataFrame = FALSE, simplifyMatrix = FALSE) %>%
    process_struct_type()
}

sdf_deserialize_column <- function(column, sc) {
  separator <- split_separator(sc)

  if (is.character(column)) {
    splat <- strsplit(column, separator$regexp, fixed = TRUE)[[1]]
    splat[splat == "<NA>"] <- NA
    Encoding(splat) <- "UTF-8"
    return(splat)
  }

  column
}

#' Read a Column from a Spark DataFrame
#'
#' Read a single column from a Spark DataFrame, and return
#' the contents of that column back to \R.
#'
#' It is expected for this operation to preserve row order.
#'
#' @template roxlate-ml-x
#' @param column The name of a column within \code{x}.
#' @export
sdf_read_column <- function(x, column) {
  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)

  schema <- sdf_schema(sdf)
  colType <- schema[[column]]$type

  separator <- split_separator(sc)

  column <- sc %>%
    invoke_static("sparklyr.Utils", "collectColumn", sdf, column, colType, separator$regexp) %>%
    sdf_deserialize_column(sc)

  column
}

#' Collect a Spark DataFrame into R.
#'
#' Collects a Spark dataframe into R.
#'
#' @param object Spark dataframe to collect
#' @param impl Which implementation to use while collecting Spark dataframe
#'        - row-wise: fetch the entire dataframe into memory and then process it row-by-row
#'        - row-wise-iter: iterate through the dataframe using RDD local iterator, processing one row at
#'                         a time (hence reducing memory footprint)
#'        - column-wise: fetch the entire dataframe into memory and then process it column-by-column
#'        NOTE: (1) this will not apply to streaming or arrow use cases (2) this parameter will only affect
#'        implementation detail, and will not affect result of `sdf_collect`, and should only be set if
#'        performance profiling indicates any particular choice will be significantly better than the default
#'        choice ("row-wise")
#' @param ... Additional options.
#'
#' @export
sdf_collect <- function(object, impl = c("row-wise", "row-wise-iter", "column-wise"), ...) {
  args <- list(...)
  impl <- match.arg(impl)
  sc <- spark_connection(object)

  if (sdf_is_streaming(object)) {
    sdf_collect_stream(object, ...)
  } else if (arrow_enabled(sc, object) && !identical(args$arrow, FALSE)) {
    arrow_collect(object, ...)
  } else {
    sdf_collect_static(object, impl, ...)
  }
}

#' Collect Spark data serialized in RDS format into R
#'
#' Deserialize Spark data that is serialized using `spark_write_rds()` into a R
#' dataframe.
#'
#' @param path Path to a local RDS file that is produced by `spark_write_rds()`
#'   (RDS files stored in HDFS will need to be downloaded to local filesystem
#'   first (e.g., by running `hadoop fs -copyToLocal ...` or similar)
#'
#' @family Spark serialization routines
#' @export
collect_from_rds <- function(path) {
  data <- readRDS(path)
  col_names <- data[[1]]
  timestamp_col_idxes <- data[[2]]
  date_col_idxes <- data[[3]]
  struct_col_idxes <- data[[4]]
  col_data <- data[[5]]
  names(col_data) <- col_names
  df <- tibble::as_tibble(col_data)

  apply_conversion <- function(df, idx, fn) {
    if ("list" %in% class(df[[idx]])) {
      df[[idx]] <- lapply(df[[idx]], fn)
    } else {
      df[[idx]] <- fn(df[[idx]])
    }

    df
  }
  for (idx in timestamp_col_idxes) {
    df <- df %>%
      apply_conversion(
        idx,
        function(x) {
          as.POSIXct(x, origin = "1970-01-01")
        }
      )
  }
  for (idx in date_col_idxes) {
    df <- df %>%
      apply_conversion(
        idx,
        function(x) {
          as.Date(x, origin = "1970-01-01")
        }
      )
  }
  for (idx in struct_col_idxes) {
    df <- df %>%
      apply_conversion(
        idx,
        function(x) {
          x %>%
            lapply(
              function(v) {
                jsonlite::fromJSON(
                  v,
                  simplifyDataFrame = FALSE,
                  simplifyMatrix = FALSE
                )
              }
            )
        }
      )
  }

  df
}

sdf_collect_data_frame <- function(sdf, collected) {
  if (identical(collected, NULL)) {
    return(invisible(NULL))
  }
  sc <- spark_connection(sdf)

  # deserialize columns as needed (string columns will enter as
  # a single newline-delimited string)
  transformed <- lapply(collected, function(e) {
    sdf_deserialize_column(e, sc)
  })

  # fix an issue where sometimes columns in a Spark DataFrame are empty
  # in such a case, we fill those with NAs of the same type (#477)
  n <- vapply(transformed, length, numeric(1))
  rows <- if (length(n) > 0) max(n) else 0
  fixed <- lapply(transformed, function(column) {
    if (length(column) == 0) {
      converter <- switch(
        typeof(column),
        character = as.character,
        logical   = as.logical,
        integer   = as.integer,
        double    = as.double,
        identity
      )
      return(converter(rep(NA, rows)))
    }
    column
  })

  # set column names and return dataframe
  colNames <- invoke(sdf, "columns")
  names(fixed) <- as.character(colNames)
  fixed <- as_tibble(fixed, stringsAsFactors = FALSE, optional = TRUE)

  # fix booleans
  schema <- sdf_schema(sdf)
  for (field in schema) {
    if (field$type == "BooleanType" && field$name %in% names(fixed)) {
      fixed[[field$name]] <- as.logical(fixed[[field$name]])
    }
  }

  fixed
}

# Read a Spark Dataset into R.
#' @importFrom dplyr as_tibble
sdf_collect_static <- function(object, impl, ...) {
  args <- list(...)
  sc <- spark_connection(object)
  sdf <- spark_dataframe(object)

  separator <- split_separator(sc)

  # for some reason, we appear to receive invalid results when
  # collecting Spark DataFrames with many columns. empirically,
  # having more than 50 columns seems to trigger the buggy behavior
  # collect the data set in chunks, and then join those chunks.
  # note that this issue should be resolved with Spark >2.0.0
  collected <- if (spark_version(sc) > "2.0.0") {
    if (!identical(args$callback, NULL)) {
      batch_size <- spark_config_value(sc$config, "sparklyr.collect.batch", as.integer(10^5L))
      ctx <- invoke_static(sc, "sparklyr.DFCollectionUtils", "prepareDataFrameForCollection", sdf)
      sdf <- invoke(ctx, "_1")
      dtypes <- invoke(ctx, "_2")
      sdf_iter <- invoke(sdf, "toLocalIterator")

      iter <- 1
      while (invoke(sdf_iter, "hasNext")) {
        raw_df <- invoke_static(
          sc,
          "sparklyr.Utils",
          "collectIter",
          invoke(sdf_iter, "underlying"),
          dtypes,
          batch_size,
          separator$regexp
        )
        df <- sdf_collect_data_frame(sdf, raw_df)
        cb <- args$callback
        if (is.language(cb)) cb <- rlang::as_closure(cb)

        if (length(formals(cb)) >= 2) cb(df, iter) else cb(df)
        iter <- iter + 1
      }

      NULL
    } else {
      invoke_static(
        sc,
        "sparklyr.Utils",
        "collect",
        sdf,
        separator$regexp,
        impl
      )
    }
  } else {
    if (!identical(args$callback, NULL)) stop("Parameter 'callback' requires Spark 2.0+")

    columns <- invoke(sdf, "columns") %>% as.character()
    chunk_size <- getOption("sparklyr.collect.chunk.size", default = 50L)
    chunks <- split_chunks(columns, as.integer(chunk_size))
    pieces <- lapply(chunks, function(chunk) {
      subset <- sdf %>% invoke("selectExpr", as.list(chunk))
      invoke_static(
        sc,
        "sparklyr.Utils",
        "collect",
        subset,
        separator$regexp,
        impl
      )
    })
    do.call(c, pieces)
  }

  sdf_collect_data_frame(sdf, collected)
}

# Split a Spark DataFrame
sdf_split <- function(object,
                      weights = c(0.5, 0.5),
                      seed = sample(.Machine$integer.max, 1)) {
  jobj <- spark_dataframe(object)
  invoke(jobj, "randomSplit", as.list(weights), as.integer(seed))
}

#' Pivot a Spark DataFrame
#'
#' Construct a pivot table over a Spark Dataframe, using a syntax similar to
#' that from \code{reshape2::dcast}.
#'
#' @template roxlate-ml-x
#' @param formula A two-sided \R formula of the form \code{x_1 + x_2 + ... ~ y_1}.
#'   The left-hand side of the formula indicates which variables are used for grouping,
#'   and the right-hand side indicates which variable is used for pivoting. Currently,
#'   only a single pivot column is supported.
#' @param fun.aggregate How should the grouped dataset be aggregated? Can be
#'   a length-one character vector, giving the name of a Spark aggregation function
#'   to be called; a named \R list mapping column names to an aggregation method,
#'   or an \R function that is invoked on the grouped dataset.
#'
#' @examples
#' \dontrun{
#' library(sparklyr)
#' library(dplyr)
#'
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' # aggregating by mean
#' iris_tbl %>%
#'   mutate(Petal_Width = ifelse(Petal_Width > 1.5, "High", "Low")) %>%
#'   sdf_pivot(Petal_Width ~ Species,
#'     fun.aggregate = list(Petal_Length = "mean")
#'   )
#'
#' # aggregating all observations in a list
#' iris_tbl %>%
#'   mutate(Petal_Width = ifelse(Petal_Width > 1.5, "High", "Low")) %>%
#'   sdf_pivot(Petal_Width ~ Species,
#'     fun.aggregate = list(Petal_Length = "collect_list")
#'   )
#' }
#'
#' @export
sdf_pivot <- function(x, formula, fun.aggregate = "count") {
  sdf <- spark_dataframe(x)

  # parse formulas of form "abc + def ~ ghi + jkl"
  deparsed <- paste(deparse(formula), collapse = " ")
  splat <- strsplit(deparsed, "~", fixed = TRUE)[[1]]
  if (length(splat) != 2) {
    stop("expected a two-sided formula; got '", deparsed, "'")
  }

  grouped_cols <- trim_whitespace(strsplit(splat[[1]], "[+*]")[[1]])
  pivot_cols <- trim_whitespace(strsplit(splat[[2]], "[+*]", fixed = TRUE)[[1]])

  # ensure no duplication of variables on each side
  intersection <- intersect(grouped_cols, pivot_cols)
  if (length(intersection)) {
    stop("variables on both sides of forumla: ", paste(deparse(intersection), collapse = " "))
  }

  # ensure variables exist in dataset
  nm <- as.character(invoke(sdf, "columns"))
  all_cols <- c(grouped_cols, pivot_cols)
  missing_cols <- setdiff(all_cols, nm)
  if (length(missing_cols)) {
    stop("missing variables in dataset: ", paste(deparse(missing_cols), collapse = " "))
  }

  # ensure pivot is length one (for now)
  if (length(pivot_cols) != 1) {
    stop("pivot column is not length one")
  }

  # generate pivoted dataset
  grouped <- sdf %>%
    invoke(
      "%>%",
      list("groupBy", grouped_cols[[1]], as.list(grouped_cols[-1])),
      list("pivot", pivot_cols[[1]])
    )

  # perform aggregation
  fun.aggregate <- fun.aggregate %||% "count"
  result <- if (is.function(fun.aggregate)) {
    fun.aggregate(grouped)
  } else if (is.character(fun.aggregate)) {
    if (length(fun.aggregate) == 1) {
      invoke(grouped, fun.aggregate[[1]])
    } else {
      invoke(grouped, fun.aggregate[[1]], as.list(fun.aggregate[-1]))
    }
  } else if (is.list(fun.aggregate)) {
    invoke(grouped, "agg", list2env(fun.aggregate))
  } else {
    stop("unsupported 'fun.aggregate' type '", class(fun.aggregate)[[1]], "'")
  }

  sdf_register(result)
}

#' Separate a Vector Column into Scalar Columns
#'
#' Given a vector column in a Spark DataFrame, split that
#' into \code{n} separate columns, each column made up of
#' the different elements in the column \code{column}.
#'
#' @template roxlate-ml-x
#' @param column The name of a (vector-typed) column.
#' @param into A specification of the columns that should be
#'   generated from \code{column}. This can either be a
#'   vector of column names, or an \R list mapping column
#'   names to the (1-based) index at which a particular
#'   vector element should be extracted.
#' @export
sdf_separate_column <- function(x,
                                column,
                                into = NULL) {
  column <- cast_string(column)

  # extract spark dataframe reference, connection
  sdf <- spark_dataframe(x)
  sc <- spark_connection(x)

  into_is_set <- cast_scalar_logical(!is.null(into))

  # when 'into' is NULL, we auto-generate a names -> index map
  if (is.null(into)) {

    # determine the length of vector elements (assume all
    # elements have the same length as the first) and
    # generate indices
    indices <- x %>%
      head(1) %>%
      dplyr::pull(!!rlang::sym(column)) %>%
      rlang::flatten() %>%
      length() %>%
      seq_len(.)

    names <- sprintf("%s_%i", column, indices)

    # construct our into map
    into <- as.list(indices)
    names(into) <- names
  }

  # when 'into' is a character vector, generate a default
  # names -> index map
  if (is.character(into)) {
    indices <- seq_along(into)
    names <- into
    into <- as.list(indices)
    names(into) <- names
  }

  # extract names, indices from map
  names <- names(into)
  indices <- as.integer(unlist(into, use.names = FALSE)) - 1L

  # call split routine
  splat <- invoke_static(
    sc,
    "sparklyr.Utils",
    "separateColumn",
    sdf,
    column,
    as.list(names),
    as.list(indices),
    into_is_set
  )

  # and give it back
  sdf_register(splat)
}
