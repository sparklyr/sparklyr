#' @include ml_clustering.R
NULL

#' @rdname ml_decision_tree
#' @template roxlate-ml-probabilistic-classifier-params
#' @export
ml_decision_tree_classifier <- function(
  x,
  formula = NULL,
  max_depth = 5,
  max_bins = 32,
  min_instances_per_node = 1,
  min_info_gain = 0,
  impurity = "gini",
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10,
  max_memory_in_mb = 256,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("decision_tree_classifier_"),
  ...
) {
  check_dots_used()
  UseMethod("ml_decision_tree_classifier")
}

ml_decision_tree_classifier_impl <- function(
  x,
  formula = NULL,
  max_depth = 5,
  max_bins = 32,
  min_instances_per_node = 1,
  min_info_gain = 0,
  impurity = "gini",
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10,
  max_memory_in_mb = 256,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("decision_tree_classifier_"),
  response = NULL,
  features = NULL,
  predicted_label_col = "predicted_label",
  ...
) {
  ml_process_model(
    x = x,
    r_class = "ml_decision_tree_classifier",
    ml_function = new_ml_model_decision_tree_classification,
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
      impurity = impurity,
      max_bins = max_bins,
      max_depth = max_depth,
      min_info_gain = min_info_gain,
      min_instances_per_node = min_instances_per_node,
      cache_node_ids = cache_node_ids,
      max_memory_in_mb = max_memory_in_mb,
      thresholds = thresholds,
      seed = seed
    )
  )
}

# ------------------------------- Methods --------------------------------------
#' @export
ml_decision_tree_classifier.spark_connection <- ml_decision_tree_classifier_impl

#' @export
ml_decision_tree_classifier.ml_pipeline <- ml_decision_tree_classifier_impl

#' @export
ml_decision_tree_classifier.tbl_spark <- ml_decision_tree_classifier_impl

# ---------------------------- Constructors ------------------------------------
new_ml_decision_tree_classifier <- function(jobj) {
  new_ml_probabilistic_classifier(jobj, class = "ml_decision_tree_classifier")
}

new_ml_decision_tree_classification_model <- function(jobj) {
  new_ml_probabilistic_classification_model(
    jobj,
    # `depth` and `featureImportances` are lazy vals in Spark.
    depth = function() invoke(jobj, "depth"),
    feature_importances = possibly_null(
      ~ read_spark_vector(jobj, "featureImportances")
    ),
    # `numNodes` is a def in Spark.
    num_nodes = function() invoke(jobj, "numNodes"),
    class = "ml_decision_tree_classification_model"
  )
}
