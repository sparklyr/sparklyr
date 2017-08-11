#' Options for Spark ML Routines
#'
#' Provide this object to the various Spark ML methods, to control certain
#' facets of the model outputs produced.
#'
#' @param id.column The name to assign to the generated id column.
#' @param response.column The name to assign to the generated response column.
#' @param features.column The name to assign to the generated features column.
#' @param output.column The name to assign to the generated output column.
#' @param model.transform An optional \R function that accepts a Spark model
#'   and returns a Spark model. This can be used to supply optional Spark model
#'   fitting parameters not made available in the \code{sparklyr} APIs.
#' @param only.model Boolean; should the Spark model object itself be returned
#'   without fitting the actual model? Useful for \code{\link{ml_one_vs_rest}}.
#' @param na.action An \R function, or the name of an \R function, indicating
#'   how missing values should be handled.
#' @param ... Optional arguments, reserved for future expansion.
#'
#' @export
ml_options <- function(id.column       = random_string("id"),
                       response.column = random_string("response"),
                       features.column = random_string("features"),
                       output.column   = random_string("output"),
                       model.transform = NULL,
                       only.model      = FALSE,
                       na.action       = getOption("na.action", "na.omit"),
                       ...)
{
  options <- list(
    id.column       = ensure_scalar_character(id.column),
    response.column = ensure_scalar_character(response.column),
    features.column = ensure_scalar_character(features.column),
    output.column   = ensure_scalar_character(output.column),
    model.transform = model.transform,
    only.model      = ensure_scalar_boolean(only.model),
    na.action       = na.action,
    ...
  )

  class(options) <- "ml_options"
  options
}
