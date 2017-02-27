#' @export
collect.spark_jobj <- function(x, ...) {
  sdf_collect(x)
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

#' @export
slice_.tbl_spark <- function(x, ...) {
  stop("Slice is not supported in this version of sparklyr")
}
