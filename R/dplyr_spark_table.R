

#' Collect results of dplyr expression
#'
#' Force computation of remote Spark DataFrame and copy the results back
#' to the client as an R data.frame
#'
#' @param x A \code{tbl_spark}.
#' @param n Limit on the number of records to copy
#' @param ... Unused
#' @return An R data frame with classes \code{data.frame} and \code{tbl_df}.
#' 
#' @family dplyr
#'
#' @import assertthat
#'
#' @name collect
#' @export
collect.tbl_spark <- function(x, ..., n = Inf) {
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
collect.spark_jobj <- function(x, ...) {
  spark_dataframe_collect(x)
}

#' @export
sql_build.tbl_spark <- function(op, con, ...) {
  sql_build(op$ops, con, ...)
}


#' @export
sample_n.tbl_spark <- function(tbl,
                               size,
                               replace = FALSE,
                               weight = NULL,
                               .env = parent.frame()) {
  dplyr::add_op_single("sample_n", .data = tbl, args = list(
    size = size,
    replace = replace,
    weight = weight,
    .env = .env
  ))
}

#' @export
sample_frac.tbl_spark <- function(tbl,
                                  size = 1,
                                  replace = FALSE,
                                  weight = NULL,
                                  .env = parent.frame()) {
  dplyr::add_op_single("sample_frac", .data = tbl, args = list(
    size = size,
    replace = replace,
    weight = weight,
    .env = .env
  ))
}
