#' Tidying methods for Spark ML tree models
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_tree_tidiers
NULL

#' @rdname ml_tree_tidiers
#' @export
tidy.ml_model_decision_tree_classification <- function(x,
                                 ...){

  dplyr::as_tibble(ml_feature_importances(x))
}

#' @rdname ml_tree_tidiers
#' @export
tidy.ml_model_decision_tree_regression <- function(x,
                                                   ...){

  dplyr::as_tibble(ml_feature_importances(x))
}

