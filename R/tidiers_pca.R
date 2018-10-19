#' Tidying methods for Spark ML Principal Component Analysis
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_pca_tidiers
NULL

#' @rdname ml_pca_tidiers
#' @export
tidy.ml_model_pca <- function(x, ...){

  dplyr::as_tibble(x$pc, rownames = "features")
}

#' @rdname ml_pca_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#'
#' @export
augment.ml_model_pca <- function(x, newdata = NULL,
                                                 ...){

  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)){
    newdata <- x$dataset
  }

  sdf_project(x, newdata)
}

#' @rdname ml_pca_tidiers
#' @export
glance.ml_model_pca <- function(x, ...) {

  explained_variance <- x$explained_variance
  names(explained_variance) <- purrr::map_chr(names(explained_variance),
                 function(e) paste0("explained_variance_", e))

  k <- x$k

  c(k, explained_variance) %>%
    t() %>%
    dplyr::as_tibble() %>%
    dplyr::rename(k = !!"V1")

}

