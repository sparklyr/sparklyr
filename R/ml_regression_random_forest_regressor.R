#' @rdname ml_random_forest
#' @export
ml_random_forest_regressor <- function(
  x,
  formula = NULL,
  num_trees = 20L,
  subsampling_rate = 1,
  max_depth = 5L,
  min_instances_per_node = 1L,
  feature_subset_strategy = "auto",
  impurity = "variance",
  min_info_gain = 0,
  max_bins = 32L,
  seed = NULL,
  checkpoint_interval = 10L,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("random_forest_regressor_"), ...
) {
  UseMethod("ml_random_forest_regressor")
}

#' @export
ml_random_forest_regressor.spark_connection <- function(
  x,
  formula = NULL,
  num_trees = 20L,
  subsampling_rate = 1,
  max_depth = 5L,
  min_instances_per_node = 1L,
  feature_subset_strategy = "auto",
  impurity = "variance",
  min_info_gain = 0,
  max_bins = 32L,
  seed = NULL,
  checkpoint_interval = 10L,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("random_forest_regressor_"), ...) {

  ml_ratify_args()

  class <- "org.apache.spark.ml.regression.RandomForestRegressor"

  jobj <- ml_new_predictor(x, class, uid, features_col,
                           label_col, prediction_col) %>%
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

  if (!rlang::is_null(seed))
    jobj <- invoke(jobj, "setSeed", seed)

  new_ml_random_forest_regressor(jobj)
}

#' @export
ml_random_forest_regressor.ml_pipeline <- function(
  x,
  formula = NULL,
  num_trees = 20L,
  subsampling_rate = 1,
  max_depth = 5L,
  min_instances_per_node = 1L,
  feature_subset_strategy = "auto",
  impurity = "variance",
  min_info_gain = 0,
  max_bins = 32L,
  seed = NULL,
  checkpoint_interval = 10L,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("random_forest_regressor_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_random_forest_regressor.tbl_spark <- function(
  x,
  formula = NULL,
  num_trees = 20L,
  subsampling_rate = 1,
  max_depth = 5L,
  min_instances_per_node = 1L,
  feature_subset_strategy = "auto",
  impurity = "variance",
  min_info_gain = 0,
  max_bins = 32L,
  seed = NULL,
  checkpoint_interval = 10L,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("random_forest_regressor_"),
  response = NULL,
  features = NULL, ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(x, predictor, formula, features_col, label_col,
                         "regression", new_ml_model_random_forest_regression)
  }
}

# Validator
ml_validator_random_forest_regressor <- function(args, nms) {
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
      num_trees <- ensure_scalar_integer(num_trees)
      subsampling_rate <- ensure_scalar_double(subsampling_rate)
      feature_subset_strategy <- ensure_scalar_character(feature_subset_strategy)
      impurity <- rlang::arg_match(impurity, "variance")
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_random_forest_regressor <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_random_forest_regressor")
}

new_ml_random_forest_regression_model <- function(jobj) {

  new_ml_prediction_model(
    jobj,
    feature_importances = try_null(read_spark_vector(jobj, "featureImportances")),
    num_features = invoke(jobj, "numFeatures"),
    total_num_nodes = invoke(jobj, "totalNumNodes"),
    tree_weights = invoke(jobj, "treeWeights"),
    trees = invoke(jobj, "trees") %>%
      lapply(new_ml_decision_tree_regression_model),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    subclass = "ml_random_forest_regression_model")
}

new_ml_model_random_forest_regression <- function(
  pipeline, pipeline_model, model, dataset, formula,
  feature_names, call) {
  new_ml_model_regression(
    pipeline, pipeline_model, model, dataset, formula,
    subclass = "ml_model_random_forest_regression",
    .features = feature_names
  )
}
