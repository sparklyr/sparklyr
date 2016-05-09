#' Dplyr table definitions for Spark
#'
#' @import dplyr
#' @name dplyr-spark-table
NULL

#' @export
#' @import assertthat
#' @name dplyr-spark-table
collect.tbl_spark <- function(x, ..., n = 1e5, warn_incomplete = TRUE) {
  assert_that(length(n) == 1, n > 0L)
  if (n == Inf) {
    n <- -1
  }

  limit <- NULL
  if (n != Inf && n > 0) {
    limit <- n
  }

  con <- x$src$con

  query <- select_spark_query(
    sql_build(x, con = con),
    limit = limit
  )

  sql <- sql_render(query, con = con)
  res <- dbSendQuery(con, sql)
  on.exit(dbClearResult(res))

  out <- dbFetch(res, n)
  if (warn_incomplete) {
    dplyr:::res_warn_incomplete(res, "n = Inf")
  }

  grouped_df(out, groups(x))
}

#' @name dplyr-spark-table
sql_build.tbl_spark <- function(op, con, ...) {
  sql_build(op$ops, con, ...)
}

#' @export
#' @name dplyr-spark-table
sample_n.tbl_spark <- function(.data,
                               size,
                               replace = FALSE,
                               weight = NULL,
                               .env = parent.frame(),
                               ...,
                               .dots) {

  dots <- lazyeval::all_dots(.dots, ..., all_named = TRUE)
  dplyr:::add_op_single("sample_n", .data = .data, dots = dots, args = list(
    size = size,
    replace = replace,
    weight = weight,
    .env = .env
  ))
}

#' @export
#' @name dplyr-spark-table
sample_frac.tbl_spark <- function(.data,
                                  size = 1,
                                  replace = FALSE,
                                  weight = NULL,
                                  .env = parent.frame(),
                                  ...,
                                  .dots) {

  dots <- lazyeval::all_dots(.dots, ..., all_named = TRUE)
  dplyr:::add_op_single("sample_frac", .data = .data, dots = dots, args = list(
    size = size,
    replace = replace,
    weight = weight,
    .env = .env
  ))
}
