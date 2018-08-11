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
  size <- x$summary$cluster_sizes()
  k <- x$summary$k

  cbind(center,
        size = size,
        cluster = 0:(k - 1) ) %>%
    dplyr::as_tibble()
}

#' @rdname ml_unsupervised_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_kmeans <- function(x, newdata = NULL,
                                    ...){

  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)){
    newdata <- x$dataset
  }
  vars <- c(dplyr::tbl_vars(newdata), "prediction")

  ml_predict(x, newdata) %>%
    dplyr::select(!!!syms(vars)) %>%
    dplyr::rename(.cluster = !!"prediction")
}

#' @rdname ml_unsupervised_tidiers
#' @export
glance.ml_model_kmeans <- function(x,
                                   ...) {

  # max.iter <- x$pipeline_model$stages[[2]]$param_map$max_iter
  # tol <- x$pipeline_model$stages[[2]]$param_map$tol
  wssse <- x$cost

  dplyr::tibble(wssse = wssse)
}
