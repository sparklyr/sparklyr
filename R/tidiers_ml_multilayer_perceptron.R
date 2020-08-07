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
tidy.ml_model_multilayer_perceptron_classification <- function(x,
                                                               ...) {
  num_layers <- length(x$model$layers)
  weight_param <- NULL

  # how many parameters in each layer
  weight_param <- purrr::map_dbl(1:(num_layers - 1), function(e) {
    (x$model$layers[e] + 1) * x$model$layers[e + 1]
  })

  # cuts in x$model$weights
  weight_param <- c(0, cumsum(weight_param))

  # transform the vector x$model$weights in a list of
  # matrix
  weight_matrix <- list()
  weight_matrix <- purrr::map(1:(length(weight_param) - 1), function(e) {
    matrix(x$model$weights[(weight_param[e] + 1):weight_param[e + 1]],
      nrow = x$model$layers[e] + 1,
      ncol = x$model$layers[e + 1],
      byrow = TRUE
    )
  })

  layers <- purrr::map_chr(1:(num_layers - 1), function(e) {
    paste0("layer_", e)
  })

  dplyr::tibble(layers, weight_matrix)
}

#' @rdname ml_multilayer_perceptron_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @export
augment.ml_model_multilayer_perceptron_classification <- function(x, newdata = NULL,
                                                                  ...) {
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
