#' Provide Options for Spark.ML Routines
#'
#' @param id The name to assign to the id column.
#' @param response.column The name to assign to the generated response column.
#' @param features.column The name to assign to the generated features column.
#' @param only.model Boolean; should the Spark model object itself be returned
#'   without fitting the actual model? Useful for \code{\link{ml_one_vs_rest}}.
#' @param ... Optional arguments, reserved for future expansion.
#'
#' @export
ml_options <- function(id              = random_string("id"),
                       response.column = random_string("response"),
                       features.column = random_string("features"),
                       only.model      = FALSE,
                       ...)
{
  options <- list(
    id              = ensure_scalar_character(id),
    response.column = ensure_scalar_character(response.column),
    features.column = ensure_scalar_character(features.column),
    only.model      = ensure_scalar_boolean(only.model),
    ...
  )

  class(options) <- "ml_options"
  options
}
