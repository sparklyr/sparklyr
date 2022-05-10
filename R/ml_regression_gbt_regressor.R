#' @rdname ml_gradient_boosted_trees
#' @template roxlate-ml-predictor-params
#' @export
ml_gbt_regressor <- function(x, formula = NULL, max_iter = 20, max_depth = 5,
                             step_size = 0.1, subsampling_rate = 1,
                             feature_subset_strategy = "auto", min_instances_per_node = 1,
                             max_bins = 32, min_info_gain = 0, loss_type = "squared",
                             seed = NULL, checkpoint_interval = 10, cache_node_ids = FALSE,
                             max_memory_in_mb = 256, features_col = "features",
                             label_col = "label", prediction_col = "prediction",
                             uid = random_string("gbt_regressor_"), ...) {
  check_dots_used()
  UseMethod("ml_gbt_regressor")
}

#' @export
ml_gbt_regressor.default<- function(x, formula = NULL, max_iter = 20, max_depth = 5,
                                              step_size = 0.1, subsampling_rate = 1,
                                              feature_subset_strategy = "auto", min_instances_per_node = 1,
                                              max_bins = 32, min_info_gain = 0, loss_type = "squared",
                                              seed = NULL, checkpoint_interval = 10, cache_node_ids = FALSE,
                                              max_memory_in_mb = 256, features_col = "features",
                                              label_col = "label", prediction_col = "prediction",
                                              uid = random_string("gbt_regressor_"), ...) {
  orig_x <- x
  x <- extract_connection(x)

  args_list <- list(
    max_iter = max_iter,
    max_depth = max_depth,
    step_size = step_size,
    subsampling_rate = subsampling_rate,
    feature_subset_strategy = feature_subset_strategy,
    min_instances_per_node = min_instances_per_node,
    max_bins = max_bins,
    min_info_gain = min_info_gain,
    loss_type = loss_type,
    seed = seed,
    checkpoint_interval = checkpoint_interval,
    cache_node_ids = cache_node_ids,
    max_memory_in_mb = max_memory_in_mb,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col
  )

  .args <- c(args_list, rlang::dots_list(...))

  .args <- validator_ml_gbt_regressor(.args)

  stage <- spark_pipeline_stage(
    sc = x,
    class = "org.apache.spark.ml.regression.GBTRegressor",
    uid = uid,
    features_col = .args[["features_col"]],
    label_col = .args[["label_col"]],
    prediction_col = .args[["prediction_col"]]
  )

  jobj <- batch_invoke(
    stage,
    list(
      list("setCheckpointInterval", .args[["checkpoint_interval"]]),
      list("setMaxBins", .args[["max_bins"]]),
      list("setMaxDepth", .args[["max_depth"]]),
      list("setMinInfoGain", .args[["min_info_gain"]]),
      list("setMinInstancesPerNode", .args[["min_instances_per_node"]]),
      list("setCacheNodeIds", .args[["cache_node_ids"]]),
      list("setMaxMemoryInMB", .args[["max_memory_in_mb"]]),
      list("setLossType", .args[["loss_type"]]),
      list("setMaxIter", .args[["max_iter"]]),
      list("setStepSize", .args[["step_size"]]),
      list("setSubsamplingRate", .args[["subsampling_rate"]]),
      jobj_set_param_helper(stage, "setFeatureSubsetStrategy", .args[["feature_subset_strategy"]], "2.3.0", "auto"),
      jobj_set_param_helper(stage, "setSeed", .args[["seed"]])
      ))

  post_ml_obj(
    x = orig_x,
    nm = new_ml_gbt_regressor(jobj),
    ml_function = new_ml_model_gbt_regression,
    formula = formula,
    response = NULL,
    features = NULL,
    features_col = features_col,
    label_col = label_col
  )
}

# Validator
validator_ml_gbt_regressor <- function(.args) {
  .args <- ml_validate_decision_tree_args(.args)

  .args[["max_iter"]] <- cast_scalar_integer(.args[["max_iter"]])
  .args[["step_size"]] <- cast_scalar_double(.args[["step_size"]])
  .args[["subsampling_rate"]] <- cast_scalar_double(.args[["subsampling_rate"]])
  .args[["loss_type"]] <- cast_choice(.args[["loss_type"]], c("squared", "absolute"))
  .args[["feature_subset_strategy"]] <- cast_string(.args[["feature_subset_strategy"]])
  .args
}

# Constructors

new_ml_gbt_regressor <- function(jobj) {
  new_ml_predictor(jobj, class = "ml_gbt_regressor")
}

new_ml_gbt_regression_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    # `lazy val featureImportances`
    feature_importances = possibly_null(~ read_spark_vector(jobj, "featureImportances")),
    # `lazy val totalNumNodes`
    total_num_nodes = invoke(jobj, "totalNumNodes"),
    # `def treeWeights`
    tree_weights = function() invoke(jobj, "treeWeights"),
    # `def trees`
    trees = function() {
      invoke(jobj, "trees") %>%
        purrr::map(new_ml_decision_tree_regression_model)
    },
    class = "ml_gbt_regression_model"
  )
}
