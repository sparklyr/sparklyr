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

#' @rawNamespace S3method(rbind,tbl_spark)
rbind.tbl_spark <- function(..., deparse.level = 1, name = random_string("sparklyr_tmp_")) {
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
  dots <- Filter(length, rlang::dots_splice(...))
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
    if (!all(rlang::have_name(dots))) {
      names(dots) <- as.character(seq_along(dots))
    }
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

#' @rawNamespace S3method(cbind,tbl_spark)
cbind.tbl_spark <- function(..., deparse.level = 1, name = random_string("sparklyr_tmp_")) {
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
  dots <- Filter(length, rlang::dots_splice(...))
  if (!all(sapply(dots, is.tbl_spark))) {
    stop("all inputs must be tbl_spark")
  }

  do.call(cbind, dots)
}

mutate_names <- function(x, value) {
  sdf <- spark_dataframe(x)
  renamed <- invoke(sdf, "toDF", as.list(value))
  sdf_register(renamed, name = as.character(x$ops$x))
}

#' @export
`names<-.tbl_spark` <- function(x, value) {
  mutate_names(x, value)
}
