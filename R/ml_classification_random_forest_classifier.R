#' @rdname ml_random_forest
#' @template roxlate-ml-probabilistic-classifier-params
#' @export
ml_random_forest_classifier <- function(
  x,
  formula = NULL,
  num_trees = 20L,
  subsampling_rate = 1,
  max_depth = 5L,
  min_instances_per_node = 1L,
  feature_subset_strategy = "auto",
  impurity = "gini",
  min_info_gain = 0,
  max_bins = 32L,
  seed = NULL,
  thresholds = NULL,
  checkpoint_interval = 10L,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("random_forest_classifier_"),  ...
) {
  UseMethod("ml_random_forest_classifier")
}

#' @export
ml_random_forest_classifier.spark_connection <- function(
  x,
  formula = NULL,
  num_trees = 20L,
  subsampling_rate = 1,
  max_depth = 5L,
  min_instances_per_node = 1L,
  feature_subset_strategy = "auto",
  impurity = "gini",
  min_info_gain = 0,
  max_bins = 32L,
  seed = NULL,
  thresholds = NULL,
  checkpoint_interval = 10L,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("random_forest_classifier_"),  ...) {

  ml_ratify_args()

  class <- "org.apache.spark.ml.classification.RandomForestClassifier"

  jobj <- ml_new_classifier(
    x, class, uid, features_col, label_col,
    prediction_col, probability_col, raw_prediction_col
  ) %>%
    invoke("setCheckpointInterval", checkpoint_interval) %>%
    invoke("setMaxBins", max_bins) %>%
    invoke("setMaxDepth", max_depth) %>%
    invoke("setMinInfoGain", min_info_gain) %>%
    invoke("setMinInstancesPerNode", min_instances_per_node) %>%
    invoke("setCacheNodeIds", cache_node_ids) %>%
    invoke("setMaxMemoryInMB", max_memory_in_mb) %>%
    invoke("setNumTrees", num_trees) %>%
    invoke("setSubsamplingRate", subsampling_rate) %>%
    invoke("setFeatureSubsetStrategy", feature_subset_strategy) %>%
    invoke("setImpurity", impurity)

  if(!rlang::is_null(thresholds))
    jobj <- invoke(jobj, "setThresholds", thresholds)

  if (!rlang::is_null(seed))
    jobj <- invoke(jobj, "setSeed", seed)

  new_ml_random_forest_classifier(jobj)
}

#' @export
ml_random_forest_classifier.ml_pipeline <- function(
  x,
  formula = NULL,
  num_trees = 20L,
  subsampling_rate = 1,
  max_depth = 5L,
  min_instances_per_node = 1L,
  feature_subset_strategy = "auto",
  impurity = "gini",
  min_info_gain = 0,
  max_bins = 32L,
  seed = NULL,
  thresholds = NULL,
  checkpoint_interval = 10L,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("random_forest_classifier_"),  ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_random_forest_classifier.tbl_spark <- function(
  x,
  formula = NULL,
  num_trees = 20L,
  subsampling_rate = 1,
  max_depth = 5L,
  min_instances_per_node = 1L,
  feature_subset_strategy = "auto",
  impurity = "gini",
  min_info_gain = 0,
  max_bins = 32L,
  seed = NULL,
  thresholds = NULL,
  checkpoint_interval = 10L,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("random_forest_classifier_"),
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
      "classification", new_ml_model_random_forest_classification,
      predicted_label_col
    )
  }
}

# Validator
ml_validator_random_forest_classifier <- function(args, nms) {
  old_new_mapping <- c(
    ml_tree_param_mapping(),
    list(
      sample.rate = "subsampling_rate",
      num.trees = "num_trees",
      col.sample.rate = "feature_subset_strategy"
    ))

  args %>%
    ml_validate_decision_tree_args() %>%
    ml_validate_args({
      if (!rlang::is_null(thresholds))
        thresholds <- lapply(thresholds, ensure_scalar_double)
      num_trees <- ensure_scalar_integer(num_trees)
      subsampling_rate <- ensure_scalar_double(subsampling_rate)
      feature_subset_strategy <- ensure_scalar_character(feature_subset_strategy)
      impurity <- rlang::arg_match(impurity, c("gini", "entropy"))
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_random_forest_classifier <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_random_forest_classifier")
}

new_ml_random_forest_classification_model <- function(jobj) {

  new_ml_prediction_model(
    jobj,
    feature_importances = try_null(read_spark_vector(jobj, "featureImportances")),
    num_classes = try_null(invoke(jobj, "numClasses")),
    num_features = invoke(jobj, "numFeatures"),
    total_num_nodes = invoke(jobj, "totalNumNodes"),
    tree_weights = invoke(jobj, "treeWeights"),
    trees = invoke(jobj, "trees") %>%
      lapply(new_ml_decision_tree_regression_model),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    probability_col = try_null(invoke(jobj, "getProbabilityCol")),
    raw_prediction_col = try_null(invoke(jobj, "getRawPredictionCol")),
    thresholds = try_null(invoke(jobj, "getThresholds")),
    subclass = "ml_random_forest_classification_model")
}

new_ml_model_random_forest_classification <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names,
  index_labels, call) {
  new_ml_model_classification(
    pipeline, pipeline_model, model, dataset, formula,
    subclass = "ml_model_random_forest_classification",
    .features = feature_names,
    .index_labels = index_labels
  )
}
