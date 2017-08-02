#' Support for Dimension Operations
#'
#' \code{sdf_dim()},  \code{sdf_nrow()} and \code{sdf_ncol()} provide similar
#' functionality to \code{dim()}, \code{nrow()} and \code{ncol()}.
#'
#' @param x An object (usually a \code{spark_tbl}).
#' @name sdf_dim
NULL

#' @export
#' @rdname sdf_dim
sdf_dim <- function(x) {
  sdf <- spark_dataframe(x)
  rows <- invoke(sdf, "count")
  columns <- invoke(sdf, "columns")
  c(rows, length(columns))
}

#' @export
#' @rdname sdf_dim
sdf_nrow <- function(x) sdf_dim(x)[[1]]

#' @export
#' @rdname sdf_dim
sdf_ncol <- function(x) sdf_dim(x)[[2]]
