#' Spark ML - Feature Importance for Tree Models
#'
#' @param model A decision tree-based model.
#' @template roxlate-ml-dots
#'
#' @return For \code{ml_model}, a sorted data frame with feature labels and their relative importance.
#'   For \code{ml_prediction_model}, a vector of relative importances.
#' @export
ml_feature_importances <- function(model, ...) {
  UseMethod("ml_feature_importances")
}

#' @export
ml_feature_importances.ml_prediction_model <- function(model, ...) {
  model_class <- class(model)[[1]]
  if (grepl("ml_gbt|ml_decision_tree", model_class))
    spark_require_version(spark_connection(spark_jobj(model)), "2.0.0")
  if (!model_class %in% c(
    "ml_decision_tree_regression_model",
    "ml_decision_tree_classifcation_model",
    "ml_gbt_regression_model",
    "ml_gbt_classification_model",
    "ml_random_forest_regression_model",
    "ml_random_forest_classification_model")
  ) {
    stop("Cannot extract feature importances from ", model_class,
         call. = FALSE)
  }
  model$feature_importances()
}

#' @export
ml_feature_importances.ml_model <- function(model, ...) {
  # backwards compat, old signature was function(sc, model)
  if (inherits(model, "spark_connection")) {
    warning("The signature function(sc, model) for ml_feature_importances() is deprecated",
            " and will be removed in a future version.",
            call. = FALSE)
    model <- rlang::dots_list(...) %>% unlist()
  }


  supported <- grepl(
    "ml_model_decision_tree|ml_model_gbt|ml_model_random_forest",
    class(model)[1])

  if (!supported)
    stop("ml_tree_feature_importance() not supported for ", class(model)[1])

  # enforce Spark 2.0.0 for certain models
  requires_spark_2 <- grepl(
    "ml_model_decision_tree|ml_model_gbt",
    class(model)[1])

  if (requires_spark_2)
    spark_require_version(spark_connection(spark_jobj(model)), "2.0.0")

  data.frame(
    feature = model$.features,
    importance = model$model$feature_importances(),
    stringsAsFactors = FALSE
  ) %>%
    rlang::set_names(c("feature", "importance")) %>%
    dplyr::arrange(dplyr::desc(!!rlang::sym("importance")))
}

#' @rdname ml_feature_importances
#' @export
ml_tree_feature_importance <- function(model, ...) {
  UseMethod("ml_feature_importances")
}
