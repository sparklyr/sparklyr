#' Provide Options for Spark.ML Routines
#'
#' @param id.column The name to assign to the generated id column.
#' @param response.column The name to assign to the generated response column.
#' @param features.column The name to assign to the generated features column.
#' @param model.transform An optional \R function that accepts a Spark model
#'   and returns a Spark model. This can be used to supply optional Spark model
#'   fitting parameters not made available in the \code{sparklyr} APIs.
#' @param only.model Boolean; should the Spark model object itself be returned
#'   without fitting the actual model? Useful for \code{\link{ml_one_vs_rest}}.
#' @param ... Optional arguments, reserved for future expansion.
#'
#' @export
ml_options <- function(id.column       = random_string("id"),
                       response.column = random_string("response"),
                       features.column = random_string("features"),
                       model.transform  = NULL,
                       only.model      = FALSE,
                       ...)
{
  options <- list(
    id.column       = ensure_scalar_character(id.column),
    response.column = ensure_scalar_character(response.column),
    features.column = ensure_scalar_character(features.column),
    model.transform = model.transform,
    only.model      = ensure_scalar_boolean(only.model),
    ...
  )

  class(options) <- "ml_options"
  options
}
