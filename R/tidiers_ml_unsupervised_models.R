#' Tidying methods for Spark ML unsupervised models
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_unsupervised_tidiers
NULL

#' @rdname ml_unsupervised_tidiers
#'
#' @export
tidy.ml_model_kmeans <- function(x,
                                 ...){
  model <- x$model
  center <- x$centers

  stats <- c("cluster_sizes", "k")
  statistics <- stats %>%
    lapply(function(x) ml_summary(model, x, allow_null = TRUE))

  cbind(center,
        sizes = statistics[[1]],
        cluster = 1:statistics[[2]]) %>%
    dplyr::as_tibble()
}

#' @rdname ml_unsupervised_tidiers
#' @param x a ml_kmeans model
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @export
augment.ml_model_kmeans <- function(x, newdata = NULL,
                                    ...){

  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)){
    newdata <- x$dataset
  }

  ml_predict(x, newdata) %>%
    dplyr::select(-label, -features) %>%
    dplyr::mutate(prediction = prediction + 1) %>%
    dplyr::rename(.cluster = !!"prediction")
}

#' @rdname ml_unsupervised_tidiers
#' @export
glance.ml_model_kmeans <- function(x,
                                   ...) {

  max.iter <- x$pipeline_model$stages[[2]]$param_map$max_iter
  tol <- x$pipeline_model$stages[[2]]$param_map$tol
  wssse <- x$cost

  tibble(wssse = wssse,
         max.iter = max.iter,
         tol = tol)
}
