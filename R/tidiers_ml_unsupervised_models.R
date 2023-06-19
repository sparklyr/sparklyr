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
                                 ...) {
  center <- x$centers
  size <- x$summary$cluster_sizes()
  k <- x$summary$k

  cbind(center,
    size = size,
    cluster = 0:(k - 1)
  ) %>%
    dplyr::as_tibble()
}

#' @rdname ml_unsupervised_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_kmeans <- function(x, newdata = NULL,
                                    ...) {

  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)) {
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
  k <- x$summary$k
  wssse <- compute_wssse(x)

  glance_tbl <- dplyr::tibble(
    k = k,
    wssse = wssse
  )

  add_silhouette(x, glance_tbl)
}

#' @rdname ml_unsupervised_tidiers
#' @export
tidy.ml_model_bisecting_kmeans <- function(x,
                                           ...) {
  center <- x$centers
  size <- x$summary$cluster_sizes()
  k <- x$summary$k

  cbind(center,
    size = size,
    cluster = 0:(k - 1)
  ) %>%
    dplyr::as_tibble()
}

#' @rdname ml_unsupervised_tidiers
#'
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_bisecting_kmeans <- function(x, newdata = NULL,
                                              ...) {

  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)) {
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
  wssse <- compute_wssse(x)

  glance_tbl <- dplyr::tibble(
    k = k,
    wssse = wssse
  )

  add_silhouette(x, glance_tbl)
}

#' @rdname ml_unsupervised_tidiers
#' @importFrom dplyr .data
#' @export
tidy.ml_model_gaussian_mixture <- function(x, ...) {
  center <- x$gaussians_df()$mean %>%
    as.data.frame() %>%
    t() %>%
    fix_data_frame() %>%
    dplyr::select(-"term")

  names(center) <- x$feature_names

  weight <- x$weights
  size <- x$summary$cluster_sizes()
  k <- x$summary$k

  cbind(center,
    weight = weight,
    size = size,
    cluster = 0:(k - 1)
  ) %>%
    dplyr::as_tibble()
}

#' @rdname ml_unsupervised_tidiers
#'
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_gaussian_mixture <- function(x, newdata = NULL,
                                              ...) {

  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)) {
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
  glance_tbl <- dplyr::tibble(k = k)
  add_silhouette(x, glance_tbl)
}

# this function add silhouette to glance if
# spark version is even or greater than 2.3.0
add_silhouette <- function(x, glance_tbl) {
  sc <- spark_connection(x$dataset)
  version <- spark_version(sc)

  if (version >= "2.3.0") {
    silhouette <- ml_clustering_evaluator(x$summary$predictions)
    glance_tbl <- dplyr::bind_cols(glance_tbl,
      silhouette = silhouette
    )
  }
  glance_tbl
}

compute_wssse <- function(x) {
  if (is_required_spark(spark_connection(x$dataset), "3.0.0")) {
    wssse <- x$summary$training_cost
  } else {
    wssse <- x$cost
  }
  wssse
}
