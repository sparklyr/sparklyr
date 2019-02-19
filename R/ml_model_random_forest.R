#' Spark ML -- Random Forest
#'
#' Perform classification and regression using random forests.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-decision-trees-base-params
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-predictor-params
#' @template roxlate-ml-old-feature-response
#' @param impurity Criterion used for information gain calculation. Supported: "entropy"
#'   and "gini" (default) for classification and "variance" (default) for regression. For
#'   \code{ml_decision_tree}, setting \code{"auto"} will default to the appropriate
#'   criterion based on model type.
#' @template roxlate-ml-feature-subset-strategy
#' @param num_trees Number of trees to train (>= 1). If 1, then no bootstrapping is used. If > 1, then bootstrapping is done.
#' @param subsampling_rate Fraction of the training data used for learning each decision tree, in range (0, 1]. (default = 1.0)
#' @name ml_random_forest
NULL

#' @rdname ml_random_forest
#' @template roxlate-ml-decision-trees-type
#' @details \code{ml_random_forest} is a wrapper around \code{ml_random_forest_regressor.tbl_spark} and \code{ml_random_forest_classifier.tbl_spark} and calls the appropriate method based on model type.
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
#' rf_model <- iris_training %>%
#'   ml_random_forest(Species ~ ., type = "classification")
#'
#' pred <- ml_predict(rf_model, iris_test)
#'
#' ml_multiclass_classification_evaluator(pred)
#' }
#' @export
ml_random_forest <- function(x, formula = NULL, type = c("auto", "regression", "classification"),
                             features_col = "features", label_col = "label", prediction_col = "prediction",
                             probability_col = "probability", raw_prediction_col = "rawPrediction",
                             feature_subset_strategy = "auto", impurity = "auto", checkpoint_interval = 10,
                             max_bins = 32, max_depth = 5, num_trees = 20, min_info_gain = 0,
                             min_instances_per_node = 1, subsampling_rate = 1, seed = NULL,
                             thresholds = NULL, cache_node_ids = FALSE, max_memory_in_mb = 256,
                             uid = random_string("random_forest_"), response = NULL, features = NULL, ...) {
  formula <- ml_standardize_formula(formula, response, features)
  response_col <- gsub("~.+$", "", formula) %>% trimws()

  sdf <- spark_dataframe(x)
  # choose classification vs. regression model based on column type
  schema <- sdf_schema(sdf)
  if (!response_col %in% names(schema)) {
    stop("`", response_col, "` is not a column in the input dataset.")
  }

  response_type <- schema[[response_col]]$type

  type <- rlang::arg_match(type)
  model_type <- if (!identical(type, "auto")) {
    type
  } else {
    if (response_type %in% c("DoubleType", "IntegerType")) {
      "regression"
    } else {
      "classification"
    }
  }

  impurity <- if (identical(impurity, "auto")) {
    if (identical(model_type, "regression")) "variance" else "gini"
  } else if (identical(model_type, "classification")) {
    if (!impurity %in% c("gini", "entropy")) {
      stop("`impurity` must be \"gini\" or \"entropy\" for classification.")
    }
    impurity
  } else {
    if (!identical(impurity, "variance")) {
      stop("`impurity` must be \"variance\" for regression.")
    }
    impurity
  }

  switch(
    model_type,
    regression = ml_random_forest_regressor(
      x = x,
      formula = formula,
      num_trees = num_trees,
      subsampling_rate = subsampling_rate,
      max_depth = max_depth,
      min_instances_per_node = min_instances_per_node,
      feature_subset_strategy = feature_subset_strategy,
      impurity = impurity,
      min_info_gain = min_info_gain,
      max_bins = max_bins,
      seed = seed,
      checkpoint_interval = checkpoint_interval,
      cache_node_ids = cache_node_ids,
      max_memory_in_mb = max_memory_in_mb,
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      uid = uid,
      ...
    ),
    classification = ml_random_forest_classifier(
      x = x,
      formula = formula,
      num_trees = num_trees,
      subsampling_rate = subsampling_rate,
      max_depth = max_depth,
      min_instances_per_node = min_instances_per_node,
      feature_subset_strategy = feature_subset_strategy,
      impurity = impurity,
      min_info_gain = min_info_gain,
      max_bins = max_bins,
      seed = seed,
      thresholds = thresholds,
      checkpoint_interval = checkpoint_interval,
      cache_node_ids = cache_node_ids,
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

new_ml_model_random_forest_classification <- function(pipeline_model, formula, dataset, label_col,
                                                      features_col, predicted_label_col) {
  new_ml_model_classification(
    pipeline_model, formula,
    dataset = dataset,
    label_col = label_col, features_col = features_col,
    predicted_label_col = predicted_label_col,
    class = "ml_model_random_forest_classification"
  )
}

new_ml_model_random_forest_regression <- function(pipeline_model, formula, dataset, label_col,
                                                  features_col) {
  new_ml_model_regression(
    pipeline_model, formula,
    dataset = dataset,
    label_col = label_col, features_col = features_col,
    class = "ml_model_random_forest_regression"
  )
}
