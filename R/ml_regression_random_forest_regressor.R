#' @rdname ml_random_forest
#' @export
ml_random_forest_regressor <- function(x, formula = NULL, num_trees = 20, subsampling_rate = 1,
                                       max_depth = 5, min_instances_per_node = 1, feature_subset_strategy = "auto",
                                       impurity = "variance", min_info_gain = 0, max_bins = 32,
                                       seed = NULL, checkpoint_interval = 10, cache_node_ids = FALSE,
                                       max_memory_in_mb = 256, features_col = "features", label_col = "label",
                                       prediction_col = "prediction",  uid = random_string("random_forest_regressor_"), ...) {
  check_dots_used()
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
    validator_ml_random_forest_regressor()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.regression.RandomForestRegressor", uid,
    features_col = .args[["features_col"]],
    label_col = .args[["label_col"]], prediction_col = .args[["prediction_col"]]
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
                                 jobj_set_param_helper(obj, "setSeed", .args[["seed"]])))))
    })

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
  formula <- ml_standardize_formula(formula, response, features)

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
    ml_construct_model_supervised(
      new_ml_model_random_forest_regression,
      predictor = stage,
      formula = formula,
      dataset = x,
      features_col = features_col,
      label_col = label_col
    )
  }
}

validator_ml_random_forest_regressor <- function(.args) {
  .args <- ml_validate_decision_tree_args(.args)

  .args[["num_trees"]] <- cast_scalar_integer(.args[["num_trees"]])
  .args[["subsampling_rate"]] <- cast_scalar_double(.args[["subsampling_rate"]])
  .args[["feature_subset_strategy"]] <- cast_string(.args[["feature_subset_strategy"]])
  .args[["impurity"]] <- cast_choice(.args[["impurity"]], "variance")
  .args
}

new_ml_random_forest_regressor <- function(jobj) {
  new_ml_predictor(jobj, class = "ml_random_forest_regressor")
}

new_ml_random_forest_regression_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    # `lazy val featureImportances`
    feature_importances = possibly_null(~ read_spark_vector(jobj, "featureImportances")),
    # `lazy val totalNumNodes`
    total_num_nodes = function() invoke(jobj, "totalNumNodes"),
    # `def treeWeights`, `def trees`
    tree_weights = function() invoke(jobj, "treeWeights"),
    trees = function() invoke(jobj, "trees") %>%
      purrr::map(new_ml_decision_tree_regression_model),
    class = "ml_random_forest_regression_model")
}
