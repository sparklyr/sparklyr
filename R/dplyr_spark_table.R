

#' @export
#' @import assertthat
collect.tbl_spark <- function(x, ..., n = Inf, warn_incomplete = TRUE) {
  assert_that(length(n) == 1, n > 0L)
  if (n == Inf) {
    n <- -1
  }

  limit <- NULL
  if (n != Inf && n > 0) {
    limit <- n
  }

  con <- x$src$con

  if (n == -1) {
    sql <- sql_render(
      sql_build(x, con = con),
      con = con)
  }
  else {
    query <- select_spark_query(
      sql_build(x, con = con),
      limit = limit
    )

    sql <- sql_render(query, con = con)
  }


  res <- dbSendQuery(con, sql)
  on.exit(dbClearResult(res))

  out <- dbFetch(res, n)
  grouped_df(out, groups(x))
}

#' @export
sql_build.tbl_spark <- function(op, con, ...) {
  sql_build(op$ops, con, ...)
}


#' @export
sample_n.tbl_spark <- function(.data,
                               size,
                               replace = FALSE,
                               weight = NULL,
                               .env = parent.frame(),
                               ...,
                               .dots) {

  dots <- lazyeval::all_dots(.dots, ..., all_named = TRUE)
  dplyr::add_op_single("sample_n", .data = .data, dots = dots, args = list(
    size = size,
    replace = replace,
    weight = weight,
    .env = .env
  ))
}

#' @export
sample_frac.tbl_spark <- function(.data,
                                  size = 1,
                                  replace = FALSE,
                                  weight = NULL,
                                  .env = parent.frame(),
                                  ...,
                                  .dots) {

  dots <- lazyeval::all_dots(.dots, ..., all_named = TRUE)
  dplyr::add_op_single("sample_frac", .data = .data, dots = dots, args = list(
    size = size,
    replace = replace,
    weight = weight,
    .env = .env
  ))
}
