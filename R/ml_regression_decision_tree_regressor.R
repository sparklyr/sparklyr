#' @rdname ml_decision_tree
#' @param variance_col (Optional) Column name for the biased sample variance of prediction.
#' @export
ml_decision_tree_regressor <- function(x, formula = NULL, max_depth = 5, max_bins = 32,
                                       min_instances_per_node = 1, min_info_gain = 0,
                                       impurity = "variance", seed = NULL, cache_node_ids = FALSE,
                                       checkpoint_interval = 10, max_memory_in_mb = 256,
                                       variance_col = NULL, features_col = "features", label_col = "label",
                                       prediction_col = "prediction", uid = random_string("decision_tree_regressor_"),
                                       ...) {
  UseMethod("ml_decision_tree_regressor")
}

#' @export
ml_decision_tree_regressor.spark_connection <- function(x, formula = NULL, max_depth = 5, max_bins = 32,
                                                        min_instances_per_node = 1, min_info_gain = 0,
                                                        impurity = "variance", seed = NULL, cache_node_ids = FALSE,
                                                        checkpoint_interval = 10, max_memory_in_mb = 256,
                                                        variance_col = NULL, features_col = "features", label_col = "label",
                                                        prediction_col = "prediction", uid = random_string("decision_tree_regressor_"),
                                                        ...) {

  .args <- list(
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
    prediction_col = prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_decision_tree_regressor()

  jobj <- ml_new_regressor(
    x, "org.apache.spark.ml.regression.DecisionTreeRegressor", uid,
    .args[["features_col"]], .args[["label_col"]], .args[["prediction_col"]]
  ) %>%
    invoke("setCheckpointInterval", .args[["checkpoint_interval"]]) %>%
    invoke("setImpurity", .args[["impurity"]]) %>%
    invoke("setMaxBins", .args[["max_bins"]]) %>%
    invoke("setMaxDepth", .args[["max_depth"]]) %>%
    invoke("setMinInfoGain", .args[["min_info_gain"]]) %>%
    invoke("setMinInstancesPerNode", .args[["min_instances_per_node"]]) %>%
    invoke("setCacheNodeIds", .args[["cache_node_ids"]]) %>%
    invoke("setMaxMemoryInMB", .args[["max_memory_in_mb"]]) %>%
    maybe_set_param("setVarianceCol", .args[["variance_col"]], "2.0.0") %>%
    maybe_set_param("setSeed", .args[["seed"]])

  new_ml_decision_tree_regressor(jobj)
}

#' @export
ml_decision_tree_regressor.ml_pipeline <- function(x, formula = NULL, max_depth = 5, max_bins = 32,
                                                   min_instances_per_node = 1, min_info_gain = 0,
                                                   impurity = "variance", seed = NULL, cache_node_ids = FALSE,
                                                   checkpoint_interval = 10, max_memory_in_mb = 256,
                                                   variance_col = NULL, features_col = "features", label_col = "label",
                                                   prediction_col = "prediction", uid = random_string("decision_tree_regressor_"),
                                                   ...) {
  stage <- ml_decision_tree_regressor.spark_connection(
    x = spark_connection(x),
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
  )
  ml_add_stage(x, stage)
}

#' @export
ml_decision_tree_regressor.tbl_spark <- function(x, formula = NULL, max_depth = 5, max_bins = 32,
                                                 min_instances_per_node = 1, min_info_gain = 0,
                                                 impurity = "variance", seed = NULL, cache_node_ids = FALSE,
                                                 checkpoint_interval = 10, max_memory_in_mb = 256,
                                                 variance_col = NULL, features_col = "features", label_col = "label",
                                                 prediction_col = "prediction", uid = random_string("decision_tree_regressor_"),
                                                 response = NULL, features = NULL, ...) {
  ml_formula_transformation()

  stage <- ml_decision_tree_regressor.spark_connection(
    x = spark_connection(x),
    formula = NULL,
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
  )

  if (is.null(formula)) {
    stage %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(
      x, stage, formula, features_col, label_col,
      "regression", new_ml_model_decision_tree_regression
    )
  }
}

# Validator
ml_validator_decision_tree_regressor <- function(.args) {
  .args <- ml_validate_decision_tree_args(.args)
  .args[["impurity"]] <- cast_choice(.args[["impurity"]], c("variance"))
  .args[["variance_col"]] <- cast_nullable_string(.args[["variance_col"]])
  .args
}

new_ml_decision_tree_regressor <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_decision_tree_regressor")
}

new_ml_decision_tree_regression_model <- function(jobj) {

  new_ml_prediction_model(
    jobj,
    # `depth` and `featureImportances` are lazy vals in Spark.
    depth = function() invoke(jobj, "depth"),
    feature_importances = function() try_null(read_spark_vector(jobj, "featureImportances")),
    num_features = invoke(jobj, "numFeatures"),
    # `numNodes` is a def in Spark.
    num_nodes = function() invoke(jobj, "numNodes"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    variance_col = try_null(invoke(jobj, "getVarianceCol")),
    subclass = "ml_decision_tree_regression_model")
}
