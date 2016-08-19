#' Provide Options for Spark.ML Routines
#'
#' @param feature.col The name to assign to the feature column.
#' @param Optional arguments, reserved for future expansion.
#'
#' @export
ml_options <- function(feature.col, ...) {
  list(feature.col = feature.col,
       ...)
}
