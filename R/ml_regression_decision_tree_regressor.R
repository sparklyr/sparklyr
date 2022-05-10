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
ml_decision_tree_regressor.default <- function(x, formula = NULL, max_depth = 5, max_bins = 32,
                                               min_instances_per_node = 1, min_info_gain = 0,
                                               impurity = "variance", seed = NULL, cache_node_ids = FALSE,
                                               checkpoint_interval = 10, max_memory_in_mb = 256,
                                               variance_col = NULL, features_col = "features", label_col = "label",
                                               prediction_col = "prediction", uid = random_string("decision_tree_regressor_"),
                                               ...) {
  orig_x <- x
  x <- extract_connection(x)

  args_list <- list(
    max_depth = max_depth,
    max_bins = max_bins,
    min_instances_per_node = min_instances_per_node,
    min_info_gain = min_info_gain,
    impurity = cast_choice(impurity, c("variance")),
    seed = seed,
    cache_node_ids = cache_node_ids,
    checkpoint_interval = checkpoint_interval,
    max_memory_in_mb = max_memory_in_mb,
    variance_col = cast_nullable_string(variance_col),
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col
  )

  args_valid <- ml_validate_decision_tree_args(args_list)

  .args <- c(args_valid, rlang::dots_list(...))

  stage <- spark_pipeline_stage(
    sc = x,
    class = "org.apache.spark.ml.regression.DecisionTreeRegressor",
    uid = uid,
    features_col = .args[["features_col"]],
    label_col = .args[["label_col"]],
    prediction_col = .args[["prediction_col"]]
  )

  jobj <- batch_invoke(
    stage,
    list(
      list("setCheckpointInterval", .args[["checkpoint_interval"]]),
      list("setImpurity", .args[["impurity"]]),
      list("setMaxBins", .args[["max_bins"]]),
      list("setMaxDepth", .args[["max_depth"]]),
      list("setMinInfoGain", .args[["min_info_gain"]]),
      list("setMinInstancesPerNode", .args[["min_instances_per_node"]]),
      list("setCacheNodeIds", .args[["cache_node_ids"]]),
      list("setMaxMemoryInMB", .args[["max_memory_in_mb"]]),
      jobj_set_param_helper(stage, "setVarianceCol", .args[["variance_col"]], "2.0.0"),
      jobj_set_param_helper(stage, "setSeed", .args[["seed"]])
    )
  )

  post_ml_obj(
    x = orig_x,
    nm = new_ml_predictor(jobj, class = "ml_decision_tree_regressor"),
    ml_function = new_ml_model_decision_tree_regression,
    formula = formula,
    response = NULL,
    features = NULL,
    features_col = features_col,
    label_col = label_col
  )
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
