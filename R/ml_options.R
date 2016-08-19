#' Provide Options for Spark.ML Routines
#'
#' @param feature.col The name to assign to the feature column.
#' @param only.model Boolean; should the Spark model object itself be returned
#'   without fitting the actual model? Useful for \code{\link{ml_one_vs_rest}}.
#' @param ... Optional arguments, reserved for future expansion.
#'
#' @export
ml_options <- function(feature.col = NULL,
                       only.model = NULL,
                       ...)
{
  options <- list(
    feature.col = feature.col,
    only.model  = only.model,
    ...
  )

  class(options) <- "ml_options"
  options
}
