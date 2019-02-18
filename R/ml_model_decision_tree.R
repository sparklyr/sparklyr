#' Spark ML -- Decision Trees
#'
#' Perform classification and regression using decision trees.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-decision-trees-base-params
#' @template roxlate-ml-old-feature-response
#' @template roxlate-ml-formula-params
#' @param impurity Criterion used for information gain calculation. Supported: "entropy"
#'   and "gini" (default) for classification and "variance" (default) for regression. For
#'   \code{ml_decision_tree}, setting \code{"auto"} will default to the appropriate
#'   criterion based on model type.
#' @name ml_decision_tree
NULL

#' @rdname ml_decision_tree
#' @template roxlate-ml-decision-trees-type
#' @details \code{ml_decision_tree} is a wrapper around \code{ml_decision_tree_regressor.tbl_spark} and \code{ml_decision_tree_classifier.tbl_spark} and calls the appropriate method based on model type.
#' @template roxlate-ml-predictor-params
#'
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' partitions <- iris_tbl %>%
#'   sdf_partition(training = 0.7, test = 0.3, seed = 1111)
#'
#' iris_training <- partitions$training
#' iris_test <- partitions$test
#'
#' dt_model <- iris_training %>%
#'   ml_decision_tree(Species ~ .)
#'
#' pred <- ml_predict(dt_model, iris_test)
#'
#' ml_multiclass_classification_evaluator(pred)
#' }
#'
#' @export
ml_decision_tree <- function(x, formula = NULL, type = c("auto", "regression", "classification"),
                             features_col = "features", label_col = "label",
                             prediction_col = "prediction", variance_col = NULL, probability_col = "probability",
                             raw_prediction_col = "rawPrediction", checkpoint_interval = 10L,
                             impurity = "auto", max_bins = 32L, max_depth = 5L, min_info_gain = 0,
                             min_instances_per_node = 1L, seed = NULL, thresholds = NULL,
                             cache_node_ids = FALSE, max_memory_in_mb = 256L, uid = random_string("decision_tree_"),
                             response = NULL, features = NULL, ...) {
  formula <- ml_standardize_formula(formula, response, features)
  response_col <- gsub("~.+$", "", formula) %>% trimws()

  sdf <- spark_dataframe(x)
  # choose classification vs. regression model based on column type
  schema <- sdf_schema(sdf)
  response_type <- schema[[response_col]]$type

  type <- rlang::arg_match(type)
  model_type <- if (!identical(type, "auto")) type else {
    if (response_type %in% c("DoubleType", "IntegerType"))
      "regression"
    else
      "classification"
  }

  impurity <- if (identical(impurity, "auto")) {
    if (identical(model_type, "regression")) "variance" else "gini"
  } else if (identical(model_type, "classification")) {
    if (!impurity %in% c("gini", "entropy"))
      stop("`impurity` must be \"gini\" or \"entropy\" for classification.", call. = FALSE)
    impurity
  } else {
    if (!identical(impurity, "variance"))
      stop("`impurity` must be \"variance\" for regression.", call. = FALSE)
    impurity
  }

  switch(
    model_type,
    regression = ml_decision_tree_regressor(
      x = x,
      formula = formula,
      max_depth = max_depth,
      max_bins = max_bins,
      min_instances_per_node = min_instances_per_node,
      min_info_gain = min_info_gain,
      impurity = impurity,
      seed = seed,
      cache_node_ids = cache_node_ids,
      checkpoint_interval = checkpoint_interval,
      max_memory_in_mb = max_memory_in_mb,
      variance_col = variance_col,
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      uid = uid,
      ...
    ),
    classification = ml_decision_tree_classifier(
      x = x,
      formula = formula,
      max_depth = max_depth,
      max_bins = max_bins,
      min_instances_per_node = min_instances_per_node,
      min_info_gain = min_info_gain,
      impurity = impurity,
      seed = seed,
      thresholds = thresholds,
      cache_node_ids = cache_node_ids,
      checkpoint_interval = checkpoint_interval,
      max_memory_in_mb = max_memory_in_mb,
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      probability_col = probability_col,
      raw_prediction_col = raw_prediction_col,
      uid = uid,
      ...
    )
  )
}

new_ml_model_decision_tree_classification <- function(pipeline_model, formula, dataset, label_col,
                                                      features_col, predicted_label_col) {
  new_ml_model_classification(
    pipeline_model, formula, dataset = dataset,
    label_col = label_col, features_col = features_col,
    predicted_label_col = predicted_label_col,
    class = "ml_model_decision_tree_classification"
  )
}

new_ml_model_decision_tree_regression <- function(pipeline_model, formula, dataset, label_col,
                                                  features_col) {
  new_ml_model_regression(
    pipeline_model, formula, dataset = dataset,
    label_col = label_col, features_col = features_col,
    class = "ml_model_decision_tree_regression"
  )
}
