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

# glance() code for decision tree is the same
# for regression and classification
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

# glance() code for random forest the same
# for regression and classification
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


#' @rdname ml_tree_tidiers
#' @export
tidy.ml_model_gbt_classification <- function(x,
                                             ...){
  dplyr::as_tibble(ml_feature_importances(x))
}

#' @rdname ml_tree_tidiers
#' @export
tidy.ml_model_gbt_regression <- function(x,
                                         ...){

  dplyr::as_tibble(ml_feature_importances(x))
}

#' @rdname ml_tree_tidiers
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_gbt_classification <- function(x, newdata = NULL,
                                                ...){

  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_tree_tidiers
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_gbt_regression <- function(x, newdata = NULL,
                                            ...){

  broom_augment_supervised(x, newdata = newdata)
}

#' @rdname ml_tree_tidiers
#' @export
glance.ml_model_gbt_classification <- function(x,
                                              ...) {
  glance_gbt(x)
}

#' @rdname ml_tree_tidiers
#' @export
glance.ml_model_gbt_regression <- function(x,
                                           ...) {
  glance_gbt(x)
}

# glance() code for gradient boosted trees is almost
#  the same for regression and classification
glance_gbt <- function(x){

  # in gbt models, the total number of nodes is
  # model$total_num_nodes() in classification and
  # model$total_num_nodes in regression
  if (any(class(x) == "ml_model_gbt_regression")){
    total_num_nodes <- x$model$total_num_nodes
  } else {total_num_nodes <- x$model$total_num_nodes()}

  num_trees <- length(x$model$trees())
  max_depth <- x$model$param_map$max_depth
  impurity <- x$model$param_map$impurity
  step_size <- x$model$param_map$step_size
  loss_type <- x$model$param_map$loss_type
  subsampling_rate <- x$model$param_map$subsampling_rate

  dplyr::tibble(num_trees = num_trees,
                total_num_nodes = total_num_nodes,
                max_depth = max_depth,
                impurity = impurity,
                step_size = step_size,
                loss_type = loss_type,
                subsampling_rate = subsampling_rate)
}

