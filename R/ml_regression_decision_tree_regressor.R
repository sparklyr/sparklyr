#' @rdname ml_decision_tree
#' @param variance_col (Optional) Column name for the biased sample variance of prediction.
#' @export
ml_decision_tree_regressor <- function(
  x,
  formula = NULL,
  max_depth = 5L,
  max_bins = 32L,
  min_instances_per_node = 1L,
  min_info_gain = 0,
  impurity = "variance",
  seed = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10L,
  max_memory_in_mb = 256L,
  variance_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("decision_tree_regressor_"), ...
) {
  UseMethod("ml_decision_tree_regressor")
}

#' @export
ml_decision_tree_regressor.spark_connection <- function(
  x,
  formula = NULL,
  max_depth = 5L,
  max_bins = 32L,
  min_instances_per_node = 1L,
  min_info_gain = 0,
  impurity = "variance",
  seed = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10L,
  max_memory_in_mb = 256L,
  variance_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("decision_tree_regressor_"), ...) {

  ml_ratify_args()

  jobj <- ml_new_regressor(
    x, "org.apache.spark.ml.regression.DecisionTreeRegressor", uid,
    features_col, label_col, prediction_col
  ) %>%
    invoke("setCheckpointInterval", checkpoint_interval) %>%
    invoke("setImpurity", impurity) %>%
    invoke("setMaxBins", max_bins) %>%
    invoke("setMaxDepth", max_depth) %>%
    invoke("setMinInfoGain", min_info_gain) %>%
    invoke("setMinInstancesPerNode", min_instances_per_node) %>%
    invoke("setCacheNodeIds", cache_node_ids) %>%
    invoke("setMaxMemoryInMB", max_memory_in_mb)

  if (!rlang::is_null(variance_col))
    jobj <- jobj_set_param(jobj, "setVarianceCol", variance_col,
                           NULL, "2.0.0")

  if (!rlang::is_null(seed))
    jobj <- invoke(jobj, "setSeed", seed)

  new_ml_decision_tree_regressor(jobj)
}

#' @export
ml_decision_tree_regressor.ml_pipeline <- function(
  x,
  formula = NULL,
  max_depth = 5L,
  max_bins = 32L,
  min_instances_per_node = 1L,
  min_info_gain = 0,
  impurity = "variance",
  seed = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10L,
  max_memory_in_mb = 256L,
  variance_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("decision_tree_regressor_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_decision_tree_regressor.tbl_spark <- function(
  x,
  formula = NULL,
  max_depth = 5L,
  max_bins = 32L,
  min_instances_per_node = 1L,
  min_info_gain = 0,
  impurity = "variance",
  seed = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10L,
  max_memory_in_mb = 256L,
  variance_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("decision_tree_regressor_"),
  response = NULL,
  features = NULL, ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(
      x, predictor, formula, features_col, label_col,
      "regression", new_ml_model_decision_tree_regression
    )
  }
}

# Validator
ml_validator_decision_tree_regressor <- function(args, nms) {
  args %>%
    ml_validate_decision_tree_args() %>%
    ml_validate_args({
      impurity <- rlang::arg_match(impurity, "variance")
      variance_col <- ensure_scalar_character(variance_col, allow.null = TRUE)
    }, ml_tree_param_mapping()) %>%
    ml_extract_args(nms, ml_tree_param_mapping())
}

# Constructors

new_ml_decision_tree_regressor <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_decision_tree_regressor")
}

new_ml_decision_tree_regression_model <- function(jobj) {

  new_ml_prediction_model(
    jobj,
    depth = invoke(jobj, "depth"),
    feature_importances = try_null(read_spark_vector(jobj, "featureImportances")),
    num_features = invoke(jobj, "numFeatures"),
    num_nodes = invoke(jobj, "numNodes"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    variance_col = try_null(invoke(jobj, "getVarianceCol")),
    subclass = "ml_decision_tree_regression_model")
}

new_ml_model_decision_tree_regression <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names, call) {

  new_ml_model_regression(
    pipeline, pipeline_model, model, dataset, formula,
    subclass = "ml_model_decision_tree_regression",
    .features = feature_names
  )
}

