#' @rdname ml_random_forest
#' @export
ml_random_forest_regressor <- function(x, formula = NULL, num_trees = 20, subsampling_rate = 1,
                                       max_depth = 5, min_instances_per_node = 1, feature_subset_strategy = "auto",
                                       impurity = "variance", min_info_gain = 0, max_bins = 32,
                                       seed = NULL, checkpoint_interval = 10, cache_node_ids = FALSE,
                                       max_memory_in_mb = 256, features_col = "features", label_col = "label",
                                       prediction_col = "prediction",  uid = random_string("random_forest_regressor_"), ...) {
  UseMethod("ml_random_forest_regressor")
}

#' @export
ml_random_forest_regressor.spark_connection <- function(x, formula = NULL, num_trees = 20, subsampling_rate = 1,
                                                        max_depth = 5, min_instances_per_node = 1, feature_subset_strategy = "auto",
                                                        impurity = "variance", min_info_gain = 0, max_bins = 32,
                                                        seed = NULL, checkpoint_interval = 10, cache_node_ids = FALSE,
                                                        max_memory_in_mb = 256, features_col = "features", label_col = "label",
                                                        prediction_col = "prediction",  uid = random_string("random_forest_regressor_"), ...) {
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
    checkpoint_interval = checkpoint_interval,
    cache_node_ids = cache_node_ids,
    max_memory_in_mb = max_memory_in_mb,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_random_forest_regressor()

  jobj <- ml_new_predictor(
    x, "org.apache.spark.ml.regression.RandomForestRegressor", uid,
    .args[["features_col"]], .args[["label_col"]], .args[["prediction_col"]]
  ) %>%
    invoke("setCheckpointInterval", .args[["checkpoint_interval"]]) %>%
    invoke("setMaxBins", .args[["max_bins"]]) %>%
    invoke("setMaxDepth", .args[["max_depth"]]) %>%
    invoke("setMinInfoGain", .args[["min_info_gain"]]) %>%
    invoke("setMinInstancesPerNode", .args[["min_instances_per_node"]]) %>%
    invoke("setCacheNodeIds", .args[["cache_node_ids"]]) %>%
    invoke("setMaxMemoryInMB", .args[["max_memory_in_mb"]]) %>%
    invoke("setNumTrees", .args[["num_trees"]]) %>%
    invoke("setSubsamplingRate", .args[["subsampling_rate"]]) %>%
    invoke("setFeatureSubsetStrategy", .args[["feature_subset_strategy"]]) %>%
    invoke("setImpurity", .args[["impurity"]]) %>%
    maybe_set_param("setSeed", .args[["seed"]])

  new_ml_random_forest_regressor(jobj)
}

#' @export
ml_random_forest_regressor.ml_pipeline <- function(x, formula = NULL, num_trees = 20, subsampling_rate = 1,
                                                   max_depth = 5, min_instances_per_node = 1, feature_subset_strategy = "auto",
                                                   impurity = "variance", min_info_gain = 0, max_bins = 32,
                                                   seed = NULL, checkpoint_interval = 10, cache_node_ids = FALSE,
                                                   max_memory_in_mb = 256, features_col = "features", label_col = "label",
                                                   prediction_col = "prediction",  uid = random_string("random_forest_regressor_"), ...) {
  stage <- ml_random_forest_regressor.spark_connection(
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
    checkpoint_interval = checkpoint_interval,
    cache_node_ids = cache_node_ids,
    max_memory_in_mb = max_memory_in_mb,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_random_forest_regressor.tbl_spark <- function(x, formula = NULL, num_trees = 20, subsampling_rate = 1,
                                                 max_depth = 5, min_instances_per_node = 1, feature_subset_strategy = "auto",
                                                 impurity = "variance", min_info_gain = 0, max_bins = 32,
                                                 seed = NULL, checkpoint_interval = 10, cache_node_ids = FALSE,
                                                 max_memory_in_mb = 256, features_col = "features", label_col = "label",
                                                 prediction_col = "prediction",  uid = random_string("random_forest_regressor_"),
                                                 response = NULL, features = NULL, ...) {
  ml_formula_transformation()

  stage <- ml_random_forest_regressor.spark_connection(
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
    checkpoint_interval = checkpoint_interval,
    cache_node_ids = cache_node_ids,
    max_memory_in_mb = max_memory_in_mb,
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
    ml_generate_ml_model(x, stage, formula, features_col, label_col,
                         "regression", new_ml_model_random_forest_regression)
  }
}

ml_validator_random_forest_regressor <- function(.args) {
  .args <- .args %>%
    ml_backwards_compatibility(  list(
      sample.rate = "subsampling_rate",
      num.trees = "num_trees",
      col.sample.rate = "feature_subset_strategy"
    )) %>%
    ml_validate_decision_tree_args()

  .args[["num_trees"]] <- cast_scalar_integer(.args[["num_trees"]])
  .args[["subsampling_rate"]] <- cast_scalar_double(.args[["subsampling_rate"]])
  .args[["feature_subset_strategy"]] <- cast_string(.args[["feature_subset_strategy"]])
  .args[["impurity"]] <- cast_choice(.args[["impurity"]], "variance")
  .args
}

new_ml_random_forest_regressor <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_random_forest_regressor")
}

new_ml_random_forest_regression_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    # `lazy val featureImportances`
    feature_importances = function() try_null(read_spark_vector(jobj, "featureImportances")),
    num_features = invoke(jobj, "numFeatures"),
    # `lazy val totalNumNodes`
    total_num_nodes = function() invoke(jobj, "totalNumNodes"),
    # `def treeWeights`, `def trees`
    tree_weights = function() invoke(jobj, "treeWeights"),
    trees = function() invoke(jobj, "trees") %>%
      purrr::map(new_ml_decision_tree_regression_model),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    subclass = "ml_random_forest_regression_model")
}
