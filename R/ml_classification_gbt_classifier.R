#' @include ml_clustering.R
NULL

#' @rdname ml_gradient_boosted_trees
#' @template roxlate-ml-probabilistic-classifier-params
#' @export
ml_gbt_classifier <- function(
  x,
  formula = NULL,
  max_iter = 20,
  max_depth = 5,
  step_size = 0.1,
  subsampling_rate = 1,
  feature_subset_strategy = "auto",
  min_instances_per_node = 1L,
  max_bins = 32,
  min_info_gain = 0,
  loss_type = "logistic",
  seed = NULL,
  thresholds = NULL,
  checkpoint_interval = 10,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("gbt_classifier_"),
  ...
) {
  check_dots_used()
  UseMethod("ml_gbt_classifier")
}

ml_gbt_classifier_impl <- function(
  x,
  formula = NULL,
  max_iter = 20,
  max_depth = 5,
  step_size = 0.1,
  subsampling_rate = 1,
  feature_subset_strategy = "auto",
  min_instances_per_node = 1L,
  max_bins = 32,
  min_info_gain = 0,
  loss_type = "logistic",
  seed = NULL,
  thresholds = NULL,
  checkpoint_interval = 10,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("gbt_classifier_"),
  response = NULL,
  features = NULL,
  predicted_label_col = "predicted_label",
  ...
) {
  feature_subset_strategy <- param_min_version(
    x,
    feature_subset_strategy,
    "2.3.0",
    "auto"
  )

  probability_col <- param_min_version(
    x,
    probability_col,
    "2.2.0",
    "probability"
  )

  raw_prediction_col <- param_min_version(
    x,
    raw_prediction_col,
    "2.2.0",
    "rawPrediction"
  )

  ml_process_model(
    x = x,
    r_class = "ml_gbt_classifier",
    ml_function = new_ml_model_gbt_classification,
    features = features,
    response = response,
    uid = uid,
    formula = formula,
    constructor_args = list(predicted_label_col = predicted_label_col),
    invoke_steps = list(
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      probability_col = probability_col,
      raw_prediction_col = raw_prediction_col,
      loss_type = loss_type,
      max_iter = max_iter,
      step_size = step_size,
      subsampling_rate = subsampling_rate,
      feature_subset_strategy = feature_subset_strategy,
      checkpoint_interval = checkpoint_interval,
      max_bins = max_bins,
      max_depth = max_depth,
      min_info_gain = min_info_gain,
      min_instances_per_node = min_instances_per_node,
      cache_node_ids = cache_node_ids,
      max_memory_in_mb = max_memory_in_mb,
      thresholds = thresholds,
      seed = seed
    )
  )
}

# ------------------------------- Methods --------------------------------------
#' @export
ml_gbt_classifier.spark_connection <- ml_gbt_classifier_impl

#' @export
ml_gbt_classifier.ml_pipeline <- ml_gbt_classifier_impl

#' @export
ml_gbt_classifier.tbl_spark <- ml_gbt_classifier_impl

# ---------------------------- Constructors ------------------------------------
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
      feature_importances = possibly_null(
        ~ read_spark_vector(jobj, "featureImportances")
      ),
      num_classes = possibly_null(~ invoke(jobj, "numClasses"))(),
      # `lazy val totalNumNodes`
      total_num_nodes = function() invoke(jobj, "totalNumNodes"),
      tree_weights = invoke(jobj, "treeWeights"),
      # `def trees`
      trees = function() {
        invoke(jobj, "trees") %>%
          purrr::map(new_ml_decision_tree_regression_model)
      },
      class = "ml_multilayer_perceptron_classification_model"
    )
  } else {
    new_ml_probabilistic_classification_model(
      jobj,
      # `lazy val featureImportances`
      feature_importances = possibly_null(
        ~ read_spark_vector(jobj, "featureImportances")
      ),
      num_classes = possibly_null(~ invoke(jobj, "numClasses"))(),
      # `lazy val totalNumNodes`
      total_num_nodes = function() invoke(jobj, "totalNumNodes"),
      tree_weights = invoke(jobj, "treeWeights"),
      # `def trees`
      trees = function() {
        invoke(jobj, "trees") %>%
          purrr::map(new_ml_decision_tree_regression_model)
      },
      class = "ml_gbt_classification_model"
    )
  }
}
