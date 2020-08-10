#' Tidying methods for Spark ML Logistic Regression
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_logistic_regression_tidiers
NULL

#' @rdname ml_logistic_regression_tidiers
#' @export
tidy.ml_model_logistic_regression <- function(x, ...) {
  num_classes <- x$model$num_classes

  if (num_classes == 2) {
    as.data.frame(x$coefficients) %>%
      dplyr::as_tibble(rownames = "features") %>%
      dplyr::rename(coefficients = !!"x$coefficients")
  } else {
    coefficients <- as.data.frame(x$coefficients) %>%
      t() %>%
      dplyr::as_tibble(rownames = "features")

    # add "_coef" to column name
    names(coefficients)[2:ncol(coefficients)] <-
      purrr::map_chr(
        names(coefficients)[2:ncol(coefficients)],
        function(e) paste0(e, "_coef")
      )

    coefficients
  }
}

#' @rdname ml_logistic_regression_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @export
augment.ml_model_logistic_regression <- function(x, newdata = NULL,
                                                 ...) {
  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_logistic_regression_tidiers
#' @export
glance.ml_model_logistic_regression <- function(x, ...) {
  elastic_net_param <- x$model$param_map$elastic_net_param
  lambda <- x$model$param_map$reg_param

  dplyr::tibble(
    elastic_net_param = elastic_net_param,
    lambda = lambda
  )
}
