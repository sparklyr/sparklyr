#' @rdname ml_decision_tree
#' @template roxlate-ml-probabilistic-classifier-params
#' @export
ml_decision_tree_classifier <- function(x, formula = NULL, max_depth = 5, max_bins = 32,
                                        min_instances_per_node = 1, min_info_gain = 0,
                                        impurity = "gini", seed = NULL, thresholds = NULL,
                                        cache_node_ids = FALSE, checkpoint_interval = 10,
                                        max_memory_in_mb = 256, features_col = "features",
                                        label_col = "label", prediction_col = "prediction",
                                        probability_col = "probability", raw_prediction_col = "rawPrediction",
                                        uid = random_string("decision_tree_classifier_"), ...) {
  UseMethod("ml_decision_tree_classifier")
}

#' @export
ml_decision_tree_classifier.spark_connection <- function(x, formula = NULL, max_depth = 5, max_bins = 32,
                                                         min_instances_per_node = 1, min_info_gain = 0,
                                                         impurity = "gini", seed = NULL, thresholds = NULL,
                                                         cache_node_ids = FALSE, checkpoint_interval = 10,
                                                         max_memory_in_mb = 256, features_col = "features",
                                                         label_col = "label", prediction_col = "prediction",
                                                         probability_col = "probability", raw_prediction_col = "rawPrediction",
                                                         uid = random_string("decision_tree_classifier_"), ...) {

  .args <- list(
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
    raw_prediction_col = raw_prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_decision_tree_classifier()

  jobj <- ml_new_classifier(
    x, "org.apache.spark.ml.classification.DecisionTreeClassifier", uid,
    .args[["features_col"]], .args[["label_col"]], .args[["prediction_col"]],
    .args[["probability_col"]], .args[["raw_prediction_col"]]
  ) %>%
    invoke("setCheckpointInterval", .args[["checkpoint_interval"]]) %>%
    invoke("setImpurity", .args[["impurity"]]) %>%
    invoke("setMaxBins", .args[["max_bins"]]) %>%
    invoke("setMaxDepth", .args[["max_depth"]]) %>%
    invoke("setMinInfoGain", .args[["min_info_gain"]]) %>%
    invoke("setMinInstancesPerNode", .args[["min_instances_per_node"]]) %>%
    invoke("setCacheNodeIds", .args[["cache_node_ids"]]) %>%
    invoke("setMaxMemoryInMB", .args[["max_memory_in_mb"]]) %>%
    maybe_set_param("setThresholds", .args[["thresholds"]]) %>%
    maybe_set_param("setSeed", .args[["seed"]])

  new_ml_decision_tree_classifier(jobj)
}

#' @export
ml_decision_tree_classifier.ml_pipeline <- function(x, formula = NULL, max_depth = 5, max_bins = 32,
                                                    min_instances_per_node = 1, min_info_gain = 0,
                                                    impurity = "gini", seed = NULL, thresholds = NULL,
                                                    cache_node_ids = FALSE, checkpoint_interval = 10,
                                                    max_memory_in_mb = 256, features_col = "features",
                                                    label_col = "label", prediction_col = "prediction",
                                                    probability_col = "probability", raw_prediction_col = "rawPrediction",
                                                    uid = random_string("decision_tree_classifier_"), ...) {

  stage <- ml_decision_tree_classifier.spark_connection(
    x = spark_connection(x),
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

  ml_add_stage(x, stage)
}

#' @export
ml_decision_tree_classifier.tbl_spark <- function(x, formula = NULL, max_depth = 5, max_bins = 32,
                                                  min_instances_per_node = 1, min_info_gain = 0,
                                                  impurity = "gini", seed = NULL, thresholds = NULL,
                                                  cache_node_ids = FALSE, checkpoint_interval = 10,
                                                  max_memory_in_mb = 256, features_col = "features",
                                                  label_col = "label", prediction_col = "prediction",
                                                  probability_col = "probability", raw_prediction_col = "rawPrediction",
                                                  uid = random_string("decision_tree_classifier_"),
                                                  response = NULL, features = NULL,
                                                  predicted_label_col = "predicted_label", ...) {

  ml_formula_transformation()

  stage <- ml_decision_tree_classifier.spark_connection(
    x = spark_connection(x),
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

  if (is.null(formula)) {
    stage %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(
      x, stage, formula, features_col, label_col,
      "classification",
      new_ml_model_decision_tree_classification,
      predicted_label_col
    )
  }
}

ml_validator_decision_tree_classifier <- function(.args) {
  .args <- ml_validate_decision_tree_args(.args)
  .args[["thresholds"]] <- cast_nullable_double_list(.args[["thresholds"]])
  .args[["impurity"]] <- cast_choice(.args[["impurity"]], c("gini", "entropy"))
  .args
}

new_ml_decision_tree_classifier <- function(jobj) {
  new_ml_classifier(jobj, subclass = "ml_decision_tree_classifier")
}

new_ml_decision_tree_classification_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    # `depth` and `featureImportances` are lazy vals in Spark.
    depth = function() invoke(jobj, "depth"),
    feature_importances = function() try_null(read_spark_vector(jobj, "featureImportances")),
    num_features = invoke(jobj, "numFeatures"),
    num_classes = try_null(invoke(jobj, "numClasses")),
    # `numNodes` is a def in Spark.
    num_nodes = function() invoke(jobj, "numNodes"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    probability_col = invoke(jobj, "getProbabilityCol"),
    raw_prediction_col = invoke(jobj, "getRawPredictionCol"),
    thresholds = try_null(invoke(jobj, "getThresholds")),
    subclass = "ml_decision_tree_classification_model")
}
