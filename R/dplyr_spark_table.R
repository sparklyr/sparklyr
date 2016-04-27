#' @export
#' @import assertthat
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
    res_warn_incomplete(res, "n = Inf")
  }

  grouped_df(out, groups(x))
}

#' @export
sql_build.tbl_spark <- function(op, con, ...) {
  sql_build(op$ops, con, ...)
}
