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
augment.ml_model_logistic_regression <- function(x, newdata = NULL, ...) {
  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_logistic_regression_tidiers
#' @param new_data a tbl_spark of new data to use for prediction.
#' @export
augment._ml_model_logistic_regression <- function(x, new_data = NULL, ...) {
  check_newdata(... = ...)
  augment(x = x$fit, newdata = new_data, ... = ...)
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

#' Tidying methods for Spark ML Naive Bayes
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_naive_bayes_tidiers
NULL

#' @rdname ml_naive_bayes_tidiers
#' @export
tidy.ml_model_naive_bayes <- function(x, ...) {
  theta <- fix_data_frame(x$theta) %>%
    dplyr::rename(.label = !!"term")

  pi <- as.data.frame(x$pi) %>%
    dplyr::rename(.pi = !!"x$pi")

  res <- dplyr::bind_cols(theta, pi)
  res <- as.data.frame(res)
  rownames(res) <- seq(nrow(res))

  res
}

#' @rdname ml_naive_bayes_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @export
augment.ml_model_naive_bayes <- function(x, newdata = NULL, ...) {
  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_naive_bayes_tidiers
#' @export
glance.ml_model_naive_bayes <- function(x, ...) {
  model_type <- x$model$param_map$model_type
  smoothing <- x$model$param_map$smoothing

  dplyr::tibble(
    model_type = model_type,
    smoothing = smoothing
  )
}

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
tidy.ml_model_linear_svc <- function(x, ...) {
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
  aggregation_depth <- x$model$param_map$aggregation_depth

  dplyr::tibble(
    reg_param = reg_param,
    standardization = standardization,
    aggregation_depth = aggregation_depth
  )
}

#' Tidying methods for Spark ML MLP
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_multilayer_perceptron_tidiers
NULL

#' @rdname ml_multilayer_perceptron_tidiers
#' @export
tidy.ml_model_multilayer_perceptron_classification <- function(x, ...) {
  num_layers <- length(x$model$layers)
  weight_param <- NULL

  # how many parameters in each layer
  weight_param <- purrr::map_dbl(seq_len(num_layers - 1), function(e) {
    (x$model$layers[e] + 1) * x$model$layers[e + 1]
  })

  # cuts in x$model$weights
  weight_param <- c(0, cumsum(weight_param))

  # transform the vector x$model$weights in a list of
  # matrix
  weight_matrix <- list()
  weight_matrix <- purrr::map(seq_len(length(weight_param) - 1), function(e) {
    matrix(
      x$model$weights[(weight_param[e] + 1):weight_param[e + 1]],
      nrow = x$model$layers[e] + 1,
      ncol = x$model$layers[e + 1],
      byrow = TRUE
    )
  })

  layers <- purrr::map_chr(seq_len(num_layers - 1), function(e) {
    paste0("layer_", e)
  })

  dplyr::tibble(layers, weight_matrix)
}

#' @rdname ml_multilayer_perceptron_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @export
augment.ml_model_multilayer_perceptron_classification <- function(
  x,
  newdata = NULL,
  ...
) {
  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_multilayer_perceptron_tidiers
#' @export
glance.ml_model_multilayer_perceptron_classification <- function(x, ...) {
  num_layers <- length(x$model$layers)
  input <- x$model$layers[1]
  output <- x$model$layers[num_layers]
  hidden <- x$model$layers[c(-1, -num_layers)]

  names(hidden) <- purrr::map_chr(1:(num_layers - 2), function(e) {
    paste0("hidden_", e, "_units")
  })

  c(
    input_units = input,
    hidden,
    output_units = output
  ) %>%
    as.list() %>%
    dplyr::as_tibble()
}
