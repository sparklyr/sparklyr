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
tidy.ml_model_naive_bayes <- function(x,
                                      ...) {
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
augment.ml_model_naive_bayes <- function(x, newdata = NULL,
                                         ...) {
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
