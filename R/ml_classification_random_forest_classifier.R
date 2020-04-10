#' @rdname ml_random_forest
#' @template roxlate-ml-probabilistic-classifier-params
#' @export
ml_random_forest_classifier <- function(x, formula = NULL, num_trees = 20, subsampling_rate = 1,
                                        max_depth = 5, min_instances_per_node = 1,
                                        feature_subset_strategy = "auto", impurity = "gini",
                                        min_info_gain = 0, max_bins = 32, seed = NULL,
                                        thresholds = NULL, checkpoint_interval = 10, cache_node_ids = FALSE,
                                        max_memory_in_mb = 256, features_col = "features",
                                        label_col = "label", prediction_col = "prediction",
                                        probability_col = "probability", raw_prediction_col = "rawPrediction",
                                        uid = random_string("random_forest_classifier_"), ...) {
  check_dots_used()
  UseMethod("ml_random_forest_classifier")
}

#' @export
ml_random_forest_classifier.spark_connection <- function(x, formula = NULL, num_trees = 20, subsampling_rate = 1,
                                                         max_depth = 5, min_instances_per_node = 1,
                                                         feature_subset_strategy = "auto", impurity = "gini",
                                                         min_info_gain = 0, max_bins = 32, seed = NULL,
                                                         thresholds = NULL, checkpoint_interval = 10, cache_node_ids = FALSE,
                                                         max_memory_in_mb = 256, features_col = "features",
                                                         label_col = "label", prediction_col = "prediction",
                                                         probability_col = "probability", raw_prediction_col = "rawPrediction",
                                                         uid = random_string("random_forest_classifier_"), ...) {

  .args <- list(
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
    raw_prediction_col = raw_prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_random_forest_classifier()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.classification.RandomForestClassifier", uid,
    features_col = .args[["features_col"]], label_col = .args[["label_col"]],
    prediction_col = .args[["prediction_col"]], probability_col = .args[["probability_col"]],
    raw_prediction_col = .args[["raw_prediction_col"]]
  ) %>% (
    function(obj) {
      do.call(invoke,
              c(obj, "%>%", Filter(function(x) !is.null(x),
                              list(
                                   list("setCheckpointInterval", .args[["checkpoint_interval"]]),
                                   list("setMaxBins", .args[["max_bins"]]),
                                   list("setMaxDepth", .args[["max_depth"]]),
                                   list("setMinInfoGain", .args[["min_info_gain"]]),
                                   list("setMinInstancesPerNode", .args[["min_instances_per_node"]]),
                                   list("setCacheNodeIds", .args[["cache_node_ids"]]),
                                   list("setMaxMemoryInMB", .args[["max_memory_in_mb"]]),
                                   list("setNumTrees", .args[["num_trees"]]),
                                   list("setSubsamplingRate", .args[["subsampling_rate"]]),
                                   list("setFeatureSubsetStrategy", .args[["feature_subset_strategy"]]),
                                   list("setImpurity", .args[["impurity"]]),
                                   jobj_set_param_helper(obj, "setThresholds", .args[["thresholds"]]),
                                   jobj_set_param_helper(obj, "setSeed", .args[["seed"]])))))
    })

  new_ml_random_forest_classifier(jobj)
}

#' @export
ml_random_forest_classifier.ml_pipeline <- function(x, formula = NULL, num_trees = 20, subsampling_rate = 1,
                                                    max_depth = 5, min_instances_per_node = 1,
                                                    feature_subset_strategy = "auto", impurity = "gini",
                                                    min_info_gain = 0, max_bins = 32, seed = NULL,
                                                    thresholds = NULL, checkpoint_interval = 10, cache_node_ids = FALSE,
                                                    max_memory_in_mb = 256, features_col = "features",
                                                    label_col = "label", prediction_col = "prediction",
                                                    probability_col = "probability", raw_prediction_col = "rawPrediction",
                                                    uid = random_string("random_forest_classifier_"), ...) {
  stage <- ml_random_forest_classifier.spark_connection(
    x = spark_connection(x),
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
  ml_add_stage(x, stage)
}

#' @export
ml_random_forest_classifier.tbl_spark <- function(x, formula = NULL, num_trees = 20, subsampling_rate = 1,
                                                  max_depth = 5, min_instances_per_node = 1,
                                                  feature_subset_strategy = "auto", impurity = "gini",
                                                  min_info_gain = 0, max_bins = 32, seed = NULL,
                                                  thresholds = NULL, checkpoint_interval = 10, cache_node_ids = FALSE,
                                                  max_memory_in_mb = 256, features_col = "features",
                                                  label_col = "label", prediction_col = "prediction",
                                                  probability_col = "probability", raw_prediction_col = "rawPrediction",
                                                  uid = random_string("random_forest_classifier_"), response = NULL,
                                                  features = NULL, predicted_label_col = "predicted_label", ...) {
  formula <- ml_standardize_formula(formula, response, features)

  stage <- ml_random_forest_classifier.spark_connection(
    x = spark_connection(x),
    formula = NULL,
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

  if (is.null(formula)) {
    stage %>%
      ml_fit(x)
  } else {
    ml_construct_model_supervised(
      new_ml_model_random_forest_classification,
      predictor = stage,
      formula = formula,
      dataset = x,
      features_col = features_col,
      label_col = label_col,
      predicted_label_col = predicted_label_col
    )
  }
}

# Validator
validator_ml_random_forest_classifier <- function(.args) {
  .args <- .args %>% ml_validate_decision_tree_args()
  .args[["thresholds"]] <- cast_nullable_double_list(.args[["thresholds"]])
  .args[["num_trees"]] <- cast_scalar_integer(.args[["num_trees"]])
  .args[["subsampling_rate"]] <- cast_scalar_double(.args[["subsampling_rate"]])
  .args[["impurity"]] <- cast_choice(.args[["impurity"]], c("gini", "entropy"))
  .args
}

# Constructors

new_ml_random_forest_classifier <- function(jobj) {
  new_ml_probabilistic_classifier(jobj, class = "ml_random_forest_classifier")
}

new_ml_random_forest_classification_model <- function(jobj) {
  new_ml_probabilistic_classification_model(
    jobj,
    # `lazy val featureImportances`
    feature_importances = possibly_null(~ read_spark_vector(jobj, "featureImportances")),
    # `lazy val totalNumNodes`
    total_num_nodes = function() invoke(jobj, "totalNumNodes"),
    # `def treeWeights`, `def trees`
    tree_weights = function() invoke(jobj, "treeWeights"),
    trees = function() invoke(jobj, "trees") %>%
      purrr::map(new_ml_decision_tree_regression_model),
    class = "ml_random_forest_classification_model")
}
