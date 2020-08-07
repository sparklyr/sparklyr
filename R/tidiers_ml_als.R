#' Tidying methods for Spark ML ALS
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_als_tidiers
NULL

#' @rdname ml_als_tidiers
#' @export
tidy.ml_model_als <- function(x, ...) {
  user_factors <- x$model$user_factors %>%
    dplyr::select(!!"id", !!"features") %>%
    dplyr::rename(user_factors = !!"features")

  item_factors <- x$model$item_factors %>%
    dplyr::select(!!"id", !!"features") %>%
    dplyr::rename(item_factors = !!"features")

  dplyr::full_join(user_factors, item_factors, by = "id")
}

#' @rdname ml_als_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @export
augment.ml_model_als <- function(x, newdata = NULL, ...) {

  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)) {
    newdata <- x$dataset
  }

  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_als_tidiers
#' @export
glance.ml_model_als <- function(x, ...) {
  rank <- x$model$rank
  cold_start_strategy <- x$model$param_map$cold_start_strategy

  dplyr::tibble(
    rank = rank,
    cold_start_strategy = cold_start_strategy
  )
}
