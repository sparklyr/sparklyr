#' Tidying methods for Spark ML Survival Regression
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_survival_regression_tidiers
NULL

#' @rdname ml_survival_regression_tidiers
#' @export
tidy.ml_model_aft_survival_regression <- function(x,
                                 ...){

  as.data.frame(x$coefficients) %>%
    dplyr::as_tibble(rownames = "features") %>%
    dplyr::rename(coefficients = !!"x$coefficients")

}

#' @rdname ml_survival_regression_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @export
augment.ml_model_aft_survival_regression <- function(x, newdata = NULL,
                                              ...){

  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_survival_regression_tidiers
#' @export
glance.ml_model_aft_survival_regression <- function(x, ...) {

  scale <- x$model$scale
  aggregation_depth <- x$model$param_map$aggregation_depth

  dplyr::tibble(scale = scale,
                aggregation_depth = aggregation_depth)
}

