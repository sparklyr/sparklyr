#' @rdname ml_random_forest
#' @template roxlate-ml-probabilistic-classifier-params
#' @export
ml_random_forest_classifier <- function(
  x,
  formula = NULL,
  num_trees = 20,
  subsampling_rate = 1,
  max_depth = 5,
  min_instances_per_node = 1,
  feature_subset_strategy = "auto",
  impurity = "gini",
  min_info_gain = 0,
  max_bins = 32,
  seed = NULL,
  thresholds = NULL,
  checkpoint_interval = 10,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("random_forest_classifier_"),
  ...
) {
  check_dots_used()
  UseMethod("ml_random_forest_classifier")
}

ml_random_forest_classifier_impl <- function(
  x,
  formula = NULL,
  num_trees = 20,
  subsampling_rate = 1,
  max_depth = 5,
  min_instances_per_node = 1,
  feature_subset_strategy = "auto",
  impurity = "gini",
  min_info_gain = 0,
  max_bins = 32,
  seed = NULL,
  thresholds = NULL,
  checkpoint_interval = 10,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("random_forest_classifier_"),
  response = NULL,
  features = NULL,
  predicted_label_col = "predicted_label",
  ...
) {
  ml_process_model(
    x = x,
    r_class = "ml_random_forest_classifier",
    ml_function = new_ml_model_random_forest_classification,
    features = features,
    response = response,
    uid = uid,
    formula = formula,
    constructor_args = list(predicted_label_col = predicted_label_col),
    invoke_steps = list(
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      probability_col = probability_col,
      raw_prediction_col = raw_prediction_col,
      checkpoint_interval = checkpoint_interval,
      max_bins = max_bins,
      max_depth = max_depth,
      min_info_gain = min_info_gain,
      min_instances_per_node = min_instances_per_node,
      cache_node_ids = cache_node_ids,
      max_memory_in_mb = max_memory_in_mb,
      num_trees = num_trees,
      subsampling_rate = subsampling_rate,
      feature_subset_strategy = feature_subset_strategy,
      impurity = impurity,
      thresholds = thresholds,
      seed = seed
    )
  )
}

# ------------------------------- Methods --------------------------------------
#' @export
ml_random_forest_classifier.spark_connection <- ml_random_forest_classifier_impl

#' @export
ml_random_forest_classifier.ml_pipeline <- ml_random_forest_classifier_impl

#' @export
ml_random_forest_classifier.tbl_spark <- ml_random_forest_classifier_impl

# Constructors

new_ml_random_forest_classifier <- function(jobj) {
  new_ml_probabilistic_classifier(jobj, class = "ml_random_forest_classifier")
}

new_ml_random_forest_classification_model <- function(jobj) {
  new_ml_probabilistic_classification_model(
    jobj,
    # `lazy val featureImportances`
    feature_importances = possibly_null(
      ~ read_spark_vector(jobj, "featureImportances")
    ),
    # `lazy val totalNumNodes`
    total_num_nodes = function() invoke(jobj, "totalNumNodes"),
    # `def treeWeights`, `def trees`
    tree_weights = function() invoke(jobj, "treeWeights"),
    trees = function() {
      invoke(jobj, "trees") %>%
        purrr::map(new_ml_decision_tree_regression_model)
    },
    class = "ml_random_forest_classification_model"
  )
}
