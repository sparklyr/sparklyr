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
  glance_decision_tree(x)
}

#' @rdname ml_tree_tidiers
#' @export
glance.ml_model_decision_tree_regression <- function(x,
                                             ...) {
  glance_decision_tree(x)
}

# glance() code for decision tree is equal for regression
# and classification
glance_decision_tree <- function(x){

  num_nodes <- x$model$num_nodes()
  depth <- x$model$depth()
  impurity <- x$model$param_map$impurity

  dplyr::tibble(num_nodes = num_nodes,
                depth = depth,
                impurity = impurity)
}


#' @rdname ml_tree_tidiers
#' @export
tidy.ml_model_random_forest_classification <- function(x,
                                                       ...){
  dplyr::as_tibble(ml_feature_importances(x))
}

#' @rdname ml_tree_tidiers
#' @export
tidy.ml_model_random_forest_regression <- function(x,
                                                   ...){
  dplyr::as_tibble(ml_feature_importances(x))
}

#' @rdname ml_tree_tidiers
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_random_forest_classification <- function(x, newdata = NULL,
                                                          ...){
  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_tree_tidiers
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_random_forest_regression <- function(x, newdata = NULL,
                                                      ...){
  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_tree_tidiers
#' @export
glance.ml_model_random_forest_classification <- function(x,
                                                         ...) {
  glance_random_forest(x)
}

#' @rdname ml_tree_tidiers
#' @export
glance.ml_model_random_forest_regression <- function(x,
                                                     ...) {
  glance_random_forest(x)
}

# glance() code for random forest is equal for regression
# and classification
glance_random_forest <- function(x){

  num_trees <- x$model$param_map$num_trees
  total_num_nodes <- x$model$total_num_nodes()
  max_depth <- x$model$param_map$max_depth
  impurity <- x$model$param_map$impurity
  subsampling_rate <- x$model$param_map$subsampling_rate

  dplyr::tibble(num_trees = num_trees,
                total_num_nodes = total_num_nodes,
                max_depth = max_depth,
                impurity = impurity,
                subsampling_rate = subsampling_rate)
}

