#' Tidying methods for Spark ML Isotonic Regression
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_isotonic_regression_tidiers
NULL

#' @rdname ml_isotonic_regression_tidiers
#' @export
tidy.ml_model_isotonic_regression <- function(x,
                                                  ...){

  tibble::data_frame(boundaries = x$model$boundaries(),
                     predictions = x$model$predictions())

}

#' @rdname ml_isotonic_regression_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @export
augment.ml_model_isotonic_regression <- function(x, newdata = NULL,
                                              ...){

  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_isotonic_regression_tidiers
#' @export
glance.ml_model_isotonic_regression <- function(x, ...) {

  isotonic <- x$model$param_map$isotonic
  num_boundaries <- length(x$model$boundaries())

  dplyr::tibble(isotonic = isotonic,
                num_boundaries = num_boundaries)
}

