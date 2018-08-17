#' Tidying methods for Spark ML unsupervised models
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_unsupervised_tidiers
NULL

#' @rdname ml_unsupervised_tidiers
#' @export
tidy.ml_model_kmeans <- function(x,
                                 ...){

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

  k <- x$summary$k
  wssse <- x$cost
  silhouette <- ml_clustering_evaluator(x$summary$predictions)

  dplyr::tibble(k = k,
                wssse = wssse,
                silhouette = silhouette)
}

#' @rdname ml_unsupervised_tidiers
#' @export
tidy.ml_model_bisecting_kmeans <- function(x,
                                 ...){

  center <- x$centers
  size <- x$summary$cluster_sizes()
  k <- x$summary$k

  cbind(center,
        size = size,
        cluster = 0:(k - 1) ) %>%
    dplyr::as_tibble()
}

#' @rdname ml_unsupervised_tidiers
#'
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_bisecting_kmeans <- function(x, newdata = NULL,
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
glance.ml_model_bisecting_kmeans <- function(x,
                                   ...) {
  k <- x$summary$k
  wssse <- x$cost
  silhouette <- ml_clustering_evaluator(x$summary$predictions)
  dplyr::tibble(k = k,
                wssse = wssse,
                silhouette = silhouette)
}

#' @rdname ml_unsupervised_tidiers
#' @export
tidy.ml_model_gaussian_mixture <- function(x,
                                           ...){

  center <- x$gaussians_df$mean %>%
    as.data.frame() %>%
    t() %>%
    broom::fix_data_frame() %>%
    dplyr::select(-!!"term")

  names(center) <- x$.features

  weight <- x$weights
  size <- x$summary$cluster_sizes()
  k <- x$summary$k

  cbind(center,
        weight = weight,
        size = size,
        cluster = 0:(k - 1) ) %>%
    dplyr::as_tibble()
}

#' @rdname ml_unsupervised_tidiers
#'
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_gaussian_mixture <- function(x, newdata = NULL,
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
glance.ml_model_gaussian_mixture <- function(x,
                                             ...) {

  k <- x$summary$k
  silhouette <- ml_clustering_evaluator(x$summary$predictions)
  dplyr::tibble(k = k,
                silhouette = silhouette)
}
