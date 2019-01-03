#' Tidying methods for Spark ML linear svc
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_linear_svc_tidiers
NULL

#' @rdname ml_linear_svc_tidiers
#' @export
tidy.ml_model_linear_svc <- function(x, ...){

    as.data.frame(x$coefficients) %>%
      dplyr::as_tibble(rownames = "features") %>%
      dplyr::rename(coefficients = !!"x$coefficients")
}

#' @rdname ml_linear_svc_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @export
augment.ml_model_linear_svc <- function(x, newdata = NULL, ...) {

  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_linear_svc_tidiers
#' @export
glance.ml_model_linear_svc <- function(x, ...) {

  reg_param <- x$model$param_map$reg_param
  standardization <- x$model$param_map$standardization
  aggregation_depth <-x$model$param_map$aggregation_depth

  dplyr::tibble(reg_param = reg_param,
                standardization = standardization,
                aggregation_depth = aggregation_depth)
}

