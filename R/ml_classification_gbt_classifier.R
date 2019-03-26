#' @rdname ml_gradient_boosted_trees
#' @template roxlate-ml-probabilistic-classifier-params
#' @export
ml_gbt_classifier <- function(x, formula = NULL, max_iter = 20, max_depth = 5,
                              step_size = 0.1, subsampling_rate = 1,
                              feature_subset_strategy = "auto", min_instances_per_node = 1L,
                              max_bins = 32, min_info_gain = 0, loss_type = "logistic",
                              seed = NULL, thresholds = NULL, checkpoint_interval = 10,
                              cache_node_ids = FALSE, max_memory_in_mb = 256,
                              features_col = "features", label_col = "label",
                              prediction_col = "prediction", probability_col = "probability",
                              raw_prediction_col = "rawPrediction",
                              uid = random_string("gbt_classifier_"), ...) {
  check_dots_used()
  UseMethod("ml_gbt_classifier")
}

#' @export
ml_gbt_classifier.spark_connection <- function(x, formula = NULL, max_iter = 20, max_depth = 5,
                                               step_size = 0.1, subsampling_rate = 1,
                                               feature_subset_strategy = "auto", min_instances_per_node = 1L,
                                               max_bins = 32, min_info_gain = 0, loss_type = "logistic",
                                               seed = NULL, thresholds = NULL, checkpoint_interval = 10,
                                               cache_node_ids = FALSE, max_memory_in_mb = 256,
                                               features_col = "features", label_col = "label",
                                               prediction_col = "prediction", probability_col = "probability",
                                               raw_prediction_col = "rawPrediction",
                                               uid = random_string("gbt_classifier_"), ...) {

  .args <- list(
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
    thresholds = thresholds,
    checkpoint_interval = checkpoint_interval,
    cache_node_ids = cache_node_ids,
    max_memory_in_mb = max_memory_in_mb,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    probability_col = probability_col,
    raw_prediction_col = raw_prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_gbt_classifier()

  stage_class <- "org.apache.spark.ml.classification.GBTClassifier"

  jobj <- (
    if (spark_version(x) < "2.2.0")
      spark_pipeline_stage(
        x, stage_class, uid, features_col = .args[["features_col"]],
        label_col = .args[["label_col"]], prediction_col = .args[["prediction_col"]]
      )
    else
      spark_pipeline_stage(
        x, stage_class, uid, features_col = .args[["features_col"]],
        label_col = .args[["label_col"]],
        prediction_col = .args[["prediction_col"]],
        probability_col = .args[["probability_col"]],
        raw_prediction_col = .args[["raw_prediction_col"]]
      )
  ) %>%
    invoke("setCheckpointInterval", .args[["checkpoint_interval"]]) %>%
    invoke("setMaxBins", .args[["max_bins"]]) %>%
    invoke("setMaxDepth", .args[["max_depth"]]) %>%
    invoke("setMinInfoGain", .args[["min_info_gain"]]) %>%
    invoke("setMinInstancesPerNode", .args[["min_instances_per_node"]]) %>%
    invoke("setCacheNodeIds", .args[["cache_node_ids"]]) %>%
    invoke("setMaxMemoryInMB", .args[["max_memory_in_mb"]]) %>%
    invoke("setLossType", .args[["loss_type"]]) %>%
    invoke("setMaxIter", .args[["max_iter"]]) %>%
    invoke("setStepSize", .args[["step_size"]]) %>%
    invoke("setSubsamplingRate", .args[["subsampling_rate"]]) %>%
    jobj_set_param("setFeatureSubsetStrategy", .args[["feature_subset_strategy"]], "2.3.0", "auto") %>%
    jobj_set_param("setThresholds", .args[["thresholds"]]) %>%
    jobj_set_param("setSeed", .args[["seed"]])

  new_ml_gbt_classifier(jobj)
}

#' @export
ml_gbt_classifier.ml_pipeline <- function(x, formula = NULL, max_iter = 20, max_depth = 5,
                                          step_size = 0.1, subsampling_rate = 1,
                                          feature_subset_strategy = "auto", min_instances_per_node = 1L,
                                          max_bins = 32, min_info_gain = 0, loss_type = "logistic",
                                          seed = NULL, thresholds = NULL, checkpoint_interval = 10,
                                          cache_node_ids = FALSE, max_memory_in_mb = 256,
                                          features_col = "features", label_col = "label",
                                          prediction_col = "prediction", probability_col = "probability",
                                          raw_prediction_col = "rawPrediction",
                                          uid = random_string("gbt_classifier_"), ...) {
  stage <- ml_gbt_classifier.spark_connection(
    x = spark_connection(x),
    formula = formula,
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
    thresholds = thresholds,
    checkpoint_interval = checkpoint_interval,
    cache_node_ids = cache_node_ids,
    max_memory_in_mb = max_memory_in_mb,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    probability_col = probability_col,
    raw_prediction_col = raw_prediction_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_gbt_classifier.tbl_spark <- function(x, formula = NULL, max_iter = 20, max_depth = 5,
                                        step_size = 0.1, subsampling_rate = 1,
                                        feature_subset_strategy = "auto", min_instances_per_node = 1L,
                                        max_bins = 32, min_info_gain = 0, loss_type = "logistic",
                                        seed = NULL, thresholds = NULL, checkpoint_interval = 10,
                                        cache_node_ids = FALSE, max_memory_in_mb = 256,
                                        features_col = "features", label_col = "label",
                                        prediction_col = "prediction", probability_col = "probability",
                                        raw_prediction_col = "rawPrediction",
                                        uid = random_string("gbt_classifier_"),
                                        response = NULL, features = NULL,
                                        predicted_label_col = "predicted_label", ...) {
  formula <- ml_standardize_formula(formula, response, features)

  stage <- ml_gbt_classifier.spark_connection(
    x = spark_connection(x),
    formula = NULL,
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
    thresholds = thresholds,
    checkpoint_interval = checkpoint_interval,
    cache_node_ids = cache_node_ids,
    max_memory_in_mb = max_memory_in_mb,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    probability_col = probability_col,
    raw_prediction_col = raw_prediction_col,
    uid = uid,
    ...
  )

  if (is.null(formula)) {
    stage %>%
      ml_fit(x)
  } else {
    ml_construct_model_supervised(
      new_ml_model_gbt_classification,
      predictor = stage,
      formula = formula,
      dataset = x,
      features_col = features_col,
      label_col = label_col,
      predicted_label_col = predicted_label_col
    )
  }
}

# Validator
validator_ml_gbt_classifier <- function(.args) {
  .args <- ml_validate_decision_tree_args(.args)

  .args[["thresholds"]] <- cast_nullable_double_list(.args[["thresholds"]])
  .args[["max_iter"]] <- cast_scalar_integer(.args[["max_iter"]])
  .args[["step_size"]] <- cast_scalar_double(.args[["step_size"]])
  .args[["subsampling_rate"]] <- cast_scalar_double(.args[["subsampling_rate"]])
  .args[["loss_type"]] <- cast_choice(.args[["loss_type"]], "logistic")
  .args[["feature_subset_strategy"]] <- cast_string(.args[["feature_subset_strategy"]])
  .args
}

new_ml_gbt_classifier <- function(jobj) {
  v <- jobj %>%
    spark_connection() %>%
    spark_version()

  if (v < "2.2.0") {
    new_ml_predictor(jobj, class = "ml_gbt_classifier")
  } else {
    new_ml_probabilistic_classifier(jobj, class = "ml_gbt_classifier")
  }
}

new_ml_gbt_classification_model <- function(jobj) {

  v <- jobj %>%
    spark_connection() %>%
    spark_version()

  if (v < "2.2.0") {
    new_ml_prediction_model(
      jobj,
      # `lazy val featureImportances`
      feature_importances = possibly_null(~ read_spark_vector(jobj, "featureImportances")),
      num_classes = possibly_null(~ invoke(jobj, "numClasses"))(),
      # `lazy val totalNumNodes`
      total_num_nodes = function() invoke(jobj, "totalNumNodes"),
      tree_weights = invoke(jobj, "treeWeights"),
      # `def trees`
      trees = function() invoke(jobj, "trees") %>%
        purrr::map(new_ml_decision_tree_regression_model),
      class = "ml_multilayer_perceptron_classification_model"
    )
  } else {
    new_ml_probabilistic_classification_model(
      jobj,
      # `lazy val featureImportances`
      feature_importances = possibly_null(~ read_spark_vector(jobj, "featureImportances")),
      num_classes = possibly_null(~ invoke(jobj, "numClasses"))(),
      # `lazy val totalNumNodes`
      total_num_nodes = function() invoke(jobj, "totalNumNodes"),
      tree_weights = invoke(jobj, "treeWeights"),
      # `def trees`
      trees = function() invoke(jobj, "trees") %>%
        purrr::map(new_ml_decision_tree_regression_model),
      class = "ml_gbt_classification_model"
    )
  }
}
