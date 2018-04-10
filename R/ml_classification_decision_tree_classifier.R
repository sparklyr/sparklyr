#' @rdname ml_decision_tree
#' @template roxlate-ml-probabilistic-classifier-params
#' @export
ml_decision_tree_classifier <- function(
  x,
  formula = NULL,
  max_depth = 5L,
  max_bins = 32L,
  min_instances_per_node = 1L,
  min_info_gain = 0,
  impurity = "gini",
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10L,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("decision_tree_classifier_"), ...
) {
  UseMethod("ml_decision_tree_classifier")
}

#' @export
ml_decision_tree_classifier.spark_connection <- function(
  x,
  formula = NULL,
  max_depth = 5L,
  max_bins = 32L,
  min_instances_per_node = 1L,
  min_info_gain = 0,
  impurity = "gini",
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10L,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("decision_tree_classifier_"), ...) {

  ml_ratify_args()

  jobj <- ml_new_classifier(
    x, "org.apache.spark.ml.classification.DecisionTreeClassifier", uid,
    features_col, label_col, prediction_col, probability_col, raw_prediction_col
  ) %>%
    invoke("setCheckpointInterval", checkpoint_interval) %>%
    invoke("setImpurity", impurity) %>%
    invoke("setMaxBins", max_bins) %>%
    invoke("setMaxDepth", max_depth) %>%
    invoke("setMinInfoGain", min_info_gain) %>%
    invoke("setMinInstancesPerNode", min_instances_per_node) %>%
    invoke("setCacheNodeIds", cache_node_ids) %>%
    invoke("setMaxMemoryInMB", max_memory_in_mb)

  if(!rlang::is_null(thresholds))
    jobj <- invoke(jobj, "setThresholds", thresholds)

  if (!rlang::is_null(seed))
    jobj <- invoke(jobj, "setSeed", seed)

  new_ml_decision_tree_classifier(jobj)
}

#' @export
ml_decision_tree_classifier.ml_pipeline <- function(
  x,
  formula = NULL,
  max_depth = 5L,
  max_bins = 32L,
  min_instances_per_node = 1L,
  min_info_gain = 0,
  impurity = "gini",
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10L,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("decision_tree_classifier_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_decision_tree_classifier.tbl_spark <- function(
  x,
  formula = NULL,
  max_depth = 5L,
  max_bins = 32L,
  min_instances_per_node = 1L,
  min_info_gain = 0,
  impurity = "gini",
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10L,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("decision_tree_classifier_"),
  response = NULL,
  features = NULL,
  predicted_label_col = "predicted_label", ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(
      x, predictor, formula, features_col, label_col,
      "classification",
      new_ml_model_decision_tree_classification,
      predicted_label_col
      )
  }
}

# Validator
ml_validator_decision_tree_classifier <- function(args, nms) {
  args %>%
    ml_validate_decision_tree_args() %>%
    ml_validate_args({
      if (!rlang::is_null(thresholds))
        thresholds <- lapply(thresholds, ensure_scalar_double)
      impurity <- rlang::arg_match(impurity, c("gini", "entropy"))
    }, ml_tree_param_mapping()) %>%
    ml_extract_args(nms, ml_tree_param_mapping())
}

# Constructors

new_ml_decision_tree_classifier <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_decision_tree_classifier")
}

new_ml_decision_tree_classification_model <- function(jobj) {

  new_ml_prediction_model(
    jobj,
    depth = invoke(jobj, "depth"),
    feature_importances = try_null(read_spark_vector(jobj, "featureImportances")),
    num_features = invoke(jobj, "numFeatures"),
    num_classes = try_null(invoke(jobj, "numClasses")),
    num_nodes = invoke(jobj, "numNodes"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    probability_col = invoke(jobj, "getProbabilityCol"),
    raw_prediction_col = invoke(jobj, "getRawPredictionCol"),
    thresholds = try_null(invoke(jobj, "getThresholds")),
    subclass = "ml_decision_tree_classification_model")
}

new_ml_model_decision_tree_classification <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names,
  index_labels, call) {

  new_ml_model_classification(
    pipeline, pipeline_model, model, dataset, formula,
    subclass = "ml_model_decision_tree_classification",
    .features = feature_names,
    .index_labels = index_labels
  )
}
