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
    spark_class = "org.apache.spark.ml.regression.RandomForestRegressor",
    r_class = "ml_random_forest_regressor",
    ml_function = new_ml_model_random_forest_regression,
    features = features,
    response = response,
    uid = uid,
    formula = formula,
    invoke_steps = list(
      setFeaturesCol = features_col,
      setLabelCol = label_col,
      setPredictionCol = prediction_col,
      setImpurity = cast_choice(impurity, c("variance")),
      setCheckpointInterval = cast_scalar_integer(checkpoint_interval),
      setMaxBins = cast_scalar_integer(max_bins),
      setMaxDepth = cast_scalar_integer(max_depth),
      setMinInfoGain = cast_scalar_double(min_info_gain),
      setMinInstancesPerNode = cast_scalar_integer(min_instances_per_node),
      setCacheNodeIds = cast_scalar_logical(cache_node_ids),
      setMaxMemoryInMB = cast_scalar_integer(max_memory_in_mb),
      setSeed = cast_nullable_scalar_integer(seed),
      setNumTrees = cast_scalar_integer(num_trees),
      setSubsamplingRate = cast_scalar_double(subsampling_rate),
      setFeatureSubsetStrategy = cast_string(feature_subset_strategy)
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
