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

#' @rdname ml_tree_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_decision_tree_classification <- function(x, newdata = NULL,
                                              ...){

  broom_augment_supervised(x, newdata = newdata)

}

#' @rdname ml_tree_tidiers
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_decision_tree_regression <- function(x, newdata = NULL,
                                                          ...){
  broom_augment_supervised(x, newdata = newdata)

}

#' @rdname ml_tree_tidiers
#' @export
glance.ml_model_decision_tree_classification <- function(x,
                                             ...) {

  num_nodes <- x$model$num_nodes()
  depth <- x$model$depth()
  impurity <- x$model$param_map$impurity

  dplyr::tibble(num_nodes = num_nodes,
                depth = depth,
                impurity = impurity)
}

#' @rdname ml_tree_tidiers
#' @export
glance.ml_model_decision_tree_regression <- function(x,
                                             ...) {

  num_nodes <- x$model$num_nodes()
  depth <- x$model$depth()
  impurity <- x$model$param_map$impurity

  dplyr::tibble(num_nodes = num_nodes,
                depth = depth,
                impurity = impurity)
}
