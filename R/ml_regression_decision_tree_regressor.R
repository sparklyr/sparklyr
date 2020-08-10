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
  check_dots_used()
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
    validator_ml_decision_tree_regressor()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.regression.DecisionTreeRegressor", uid,
    features_col = .args[["features_col"]],
    label_col = .args[["label_col"]],
    prediction_col = .args[["prediction_col"]]
  ) %>% (
    function(obj) {
      do.call(
        invoke,
        c(obj, "%>%", Filter(
          function(x) !is.null(x),
          list(
            list("setCheckpointInterval", .args[["checkpoint_interval"]]),
            list("setImpurity", .args[["impurity"]]),
            list("setMaxBins", .args[["max_bins"]]),
            list("setMaxDepth", .args[["max_depth"]]),
            list("setMinInfoGain", .args[["min_info_gain"]]),
            list("setMinInstancesPerNode", .args[["min_instances_per_node"]]),
            list("setCacheNodeIds", .args[["cache_node_ids"]]),
            list("setMaxMemoryInMB", .args[["max_memory_in_mb"]]),
            jobj_set_param_helper(obj, "setVarianceCol", .args[["variance_col"]], "2.0.0"),
            jobj_set_param_helper(obj, "setSeed", .args[["seed"]])
          )
        ))
      )
    })
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
  formula <- ml_standardize_formula(formula, response, features)

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
    ml_construct_model_supervised(
      new_ml_model_decision_tree_regression,
      predictor = stage,
      formula = formula,
      dataset = x,
      features_col = features_col,
      label_col = label_col
    )
  }
}

# Validator
validator_ml_decision_tree_regressor <- function(.args) {
  .args <- ml_validate_decision_tree_args(.args)
  .args[["impurity"]] <- cast_choice(.args[["impurity"]], c("variance"))
  .args[["variance_col"]] <- cast_nullable_string(.args[["variance_col"]])
  .args
}

new_ml_decision_tree_regressor <- function(jobj) {
  new_ml_predictor(jobj, class = "ml_decision_tree_regressor")
}

new_ml_decision_tree_regression_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    # `depth` and `featureImportances` are lazy vals in Spark.
    depth = function() invoke(jobj, "depth"),
    feature_importances = possibly_null(~ read_spark_vector(jobj, "featureImportances")),
    # `numNodes` is a def in Spark.
    num_nodes = function() invoke(jobj, "numNodes"),
    variance_col = possibly_null(invoke)(jobj, "getVarianceCol"),
    class = "ml_decision_tree_regression_model"
  )
}
