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

  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)){
    newdata <- x$dataset
  }

  vars <- c(dplyr::tbl_vars(newdata), "predicted_label")

  ml_predict(x, newdata) %>%
    dplyr::select(!!!syms(vars)) %>%
    dplyr::rename(.predicted_label = !!"predicted_label")
}

#' @rdname ml_tree_tidiers
#' @importFrom rlang syms
#'
#' @export
augment.ml_model_decision_tree_regression <- function(x, newdata = NULL,
                                                          ...){

  # if the user doesn't provide a new data, this funcion will
  # use the training set
  if (is.null(newdata)){
    newdata <- x$dataset
  }

  vars <- c(dplyr::tbl_vars(newdata), "prediction")

  ml_predict(x, newdata) %>%
    dplyr::select(!!!syms(vars)) %>%
    dplyr::rename(.prediction = !!"prediction")
}
