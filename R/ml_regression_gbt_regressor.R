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

ml_gbt_regressor_impl <- function(x, formula = NULL, max_iter = 20, max_depth = 5,
                                     step_size = 0.1, subsampling_rate = 1,
                                     feature_subset_strategy = "auto", min_instances_per_node = 1,
                                     max_bins = 32, min_info_gain = 0, loss_type = "squared",
                                     seed = NULL, checkpoint_interval = 10, cache_node_ids = FALSE,
                                     max_memory_in_mb = 256, features_col = "features",
                                     label_col = "label", prediction_col = "prediction",
                                     uid = random_string("gbt_regressor_"),
                                     response = NULL, features = NULL, ...) {

  feature_subset_strategy <- param_min_version(x, feature_subset_strategy, "2.3.0", "auto")

  ml_process_model(
    x = x,
    r_class = "ml_gbt_regressor",
    ml_function = new_ml_model_gbt_regression,
    features = features,
    response = response,
    uid = uid,
    formula = formula,
    invoke_steps = list(
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      max_iter = max_iter,
      step_size = step_size,
      subsampling_rate = subsampling_rate,
      feature_subset_strategy = feature_subset_strategy,
      loss_type = loss_type,
      checkpoint_interval = checkpoint_interval,
      max_bins = max_bins,
      max_depth = max_depth,
      min_info_gain = min_info_gain,
      min_instances_per_node = min_instances_per_node,
      cache_node_ids = cache_node_ids,
      max_memory_in_mb = max_memory_in_mb,
      seed = seed
    )
  )
}

# ------------------------------- Methods --------------------------------------
#' @export
ml_gbt_regressor.spark_connection <- ml_gbt_regressor_impl

#' @export
ml_gbt_regressor.ml_pipeline <- ml_gbt_regressor_impl

#' @export
ml_gbt_regressor.tbl_spark <- ml_gbt_regressor_impl

# ---------------------------- Constructors ------------------------------------
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
