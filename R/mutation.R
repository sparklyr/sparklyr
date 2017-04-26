#' @rawNamespace S3method(rbind,tbl_spark)
rbind.tbl_spark <- function(..., deparse.level = 1, name = random_string("sparklyr_tmp_"))
{
  dots <- list(...)
  n <- length(dots)
  self <- dots[[1]]

  if (n == 1)
    return(self)

  sdf <- spark_dataframe(self)

  # NOTE: 'unionAll' was deprecated in Spark 2.0.0, but 'unionAll'
  # is not available for DataFrames in older versions of Spark, so
  # provide a bit of indirection based on Spark version
  sc <- spark_connection(sdf)
  version <- spark_version(sc)
  method <- if (version < "2.0.0") "unionAll" else "union"

  columns <- sdf %>%
    invoke("columns") %>%
    unlist

  reorder_columns <- function(sdf) {
    cols <- columns %>%
      lapply(function(column) invoke(sdf, "col", column))
    sdf %>%
      invoke("select", cols)
  }

  for (i in 2:n)
    sdf <- invoke(sdf, method, reorder_columns(spark_dataframe(dots[[i]])))

  sdf_register(sdf, name = name)
}

#' @rawNamespace S3method(cbind,tbl_spark)
cbind.tbl_spark <- function(..., deparse.level = 1, name = random_string("sparklyr_tmp_")) {
  dots <- list(...)
  n <- length(dots)
  self <- dots[[1]]

  if (n == 1)
    return(self)

  id <- random_string("id_")

  dots_with_ids <- dots %>%
    lapply(function(x) sdf_with_sequential_id(x, id = id))

  dots_num_rows <- dots_with_ids %>%
    lapply(function(x) sdf_last_index(x, id = id)) %>%
    unlist %>%
    (function(x) x + 1)

  if (length(unique(dots_num_rows)) > 1) {
    names_tbls <- substitute(list(...))[-1] %>%
      sapply(deparse)
    output_table <- dplyr::data_frame(tbl = names_tbls,
                               nrow = dots_num_rows) %>%
      dplyr::group_by(nrow) %>%
      dplyr::slice(1) %>%
      as.data.frame()
    output_table <- paste(capture.output(print(output_table)), collapse = "\n")

    stop("Not all inputs have the same number of rows, for example:\n",
         output_table)
  }

    Reduce(function(x, y) dplyr::inner_join(x, y, by = id),
           dots_with_ids) %>%
    select(- !!! rlang::sym(id))
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
