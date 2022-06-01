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

  feature_subset_strategy <- param_min_version(x, feature_subset_strategy, "2.3.0")

  ml_process_model(
    x = x,
    spark_class = "org.apache.spark.ml.regression.GBTRegressor",
    r_class = "ml_gbt_regressor",
    ml_function = new_ml_model_gbt_regression,
    features = features,
    response = response,
    uid = uid,
    formula = formula,
    invoke_steps = list(
      setFeaturesCol = features_col,
      setLabelCol = label_col,
      setPredictionCol = prediction_col,
      setMaxIter = cast_scalar_integer(max_iter),
      setStepSize = cast_scalar_double(step_size),
      setSubsamplingRate = cast_scalar_double(subsampling_rate),
      setFeatureSubsetStrategy = cast_nullable_string(feature_subset_strategy),
      setLossType = cast_choice(loss_type, c("squared", "absolute")),
      setCheckpointInterval = cast_scalar_integer(checkpoint_interval),
      setMaxBins = cast_scalar_integer(max_bins),
      setMaxDepth = cast_scalar_integer(max_depth),
      setMinInfoGain = cast_scalar_double(min_info_gain),
      setMinInstancesPerNode = cast_scalar_integer(min_instances_per_node),
      setCacheNodeIds = cast_scalar_logical(cache_node_ids),
      setMaxMemoryInMB = cast_scalar_integer(max_memory_in_mb),
      setSeed = cast_nullable_scalar_integer(seed)
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
