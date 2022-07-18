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

ml_decision_tree_regressor_impl <- function(x, formula = NULL, max_depth = 5, max_bins = 32,
                                               min_instances_per_node = 1, min_info_gain = 0,
                                               impurity = "variance", seed = NULL, cache_node_ids = FALSE,
                                               checkpoint_interval = 10, max_memory_in_mb = 256,
                                               variance_col = NULL, features_col = "features", label_col = "label",
                                               prediction_col = "prediction", uid = random_string("decision_tree_regressor_"),
                                               response = NULL, features = NULL,
                                               ...) {

  variance_col <- param_min_version(x, variance_col, "2.0.0")

  ml_process_model(
    x = x,
    r_class = "ml_decision_tree_regressor",
    ml_function = new_ml_model_decision_tree_regression,
    features = features,
    response = response,
    uid = uid,
    formula = formula,
    invoke_steps = list(
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      impurity = impurity,
      checkpoint_interval = checkpoint_interval,
      max_bins = max_bins,
      max_depth = max_depth,
      min_info_gain = min_info_gain,
      min_instances_per_node = min_instances_per_node,
      cache_node_ids = cache_node_ids,
      max_memory_in_mb = max_memory_in_mb,
      variance_col = variance_col,
      seed = seed
    )
  )
}

# ------------------------------- Methods --------------------------------------
#' @export
ml_decision_tree_regressor.spark_connection <- ml_decision_tree_regressor_impl

#' @export
ml_decision_tree_regressor.ml_pipeline <- ml_decision_tree_regressor_impl

#' @export
ml_decision_tree_regressor.tbl_spark <- ml_decision_tree_regressor_impl

# ---------------------------- Constructors ------------------------------------
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
