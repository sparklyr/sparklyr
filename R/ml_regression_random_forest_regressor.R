#' @rdname ml_random_forest
#' @export
ml_random_forest_regressor <- function(x, formula = NULL, num_trees = 20, subsampling_rate = 1,
                                       max_depth = 5, min_instances_per_node = 1, feature_subset_strategy = "auto",
                                       impurity = "variance", min_info_gain = 0, max_bins = 32,
                                       seed = NULL, checkpoint_interval = 10, cache_node_ids = FALSE,
                                       max_memory_in_mb = 256, features_col = "features", label_col = "label",
                                       prediction_col = "prediction", uid = random_string("random_forest_regressor_"), ...) {
  check_dots_used()
  UseMethod("ml_random_forest_regressor")
}

ml_random_forest_regressor_impl <- function(x, formula = NULL, num_trees = 20, subsampling_rate = 1,
                                               max_depth = 5, min_instances_per_node = 1, feature_subset_strategy = "auto",
                                               impurity = "variance", min_info_gain = 0, max_bins = 32,
                                               seed = NULL, checkpoint_interval = 10, cache_node_ids = FALSE,
                                               max_memory_in_mb = 256, features_col = "features", label_col = "label",
                                               prediction_col = "prediction", uid = random_string("random_forest_regressor_"),
                                               response = NULL, features = NULL, ...) {
  ml_process_model(
    x = x,
    r_class = "ml_random_forest_regressor",
    ml_function = new_ml_model_random_forest_regression,
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
      seed = seed,
      num_trees = num_trees,
      subsampling_rate = subsampling_rate,
      feature_subset_strategy = feature_subset_strategy
    )
  )
}

# ------------------------------- Methods --------------------------------------
#' @export
ml_random_forest_regressor.spark_connection <- ml_random_forest_regressor_impl

#' @export
ml_random_forest_regressor.ml_pipeline <- ml_random_forest_regressor_impl

#' @export
ml_random_forest_regressor.tbl_spark <- ml_random_forest_regressor_impl

# ---------------------------- Constructors ------------------------------------
new_ml_random_forest_regression_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    # `lazy val featureImportances`
    feature_importances = possibly_null(~ read_spark_vector(jobj, "featureImportances")),
    # `lazy val totalNumNodes`
    total_num_nodes = function() invoke(jobj, "totalNumNodes"),
    # `def treeWeights`, `def trees`
    tree_weights = function() invoke(jobj, "treeWeights"),
    trees = function() {
      invoke(jobj, "trees") %>%
        purrr::map(new_ml_decision_tree_regression_model)
    },
    class = "ml_random_forest_regression_model"
  )
}
