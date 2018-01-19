#' @rdname ml_gradient_boosted_trees
#' @template roxlate-ml-probabilistic-classifier-params
#' @export
ml_gbt_classifier <- function(
  x,
  formula = NULL,
  max_iter = 20L,
  max_depth = 5L,
  step_size = 0.1,
  subsampling_rate = 1,
  min_instances_per_node = 1L,
  max_bins = 32L,
  min_info_gain = 0,
  loss_type = "logistic",
  seed = NULL,
  thresholds = NULL,
  checkpoint_interval = 10L,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("gbt_classifier_"), ...
) {
  UseMethod("ml_gbt_classifier")
}

#' @export
ml_gbt_classifier.spark_connection <- function(
  x,
  formula = NULL,
  max_iter = 20L,
  max_depth = 5L,
  step_size = 0.1,
  subsampling_rate = 1,
  min_instances_per_node = 1L,
  max_bins = 32L,
  min_info_gain = 0,
  loss_type = "logistic",
  seed = NULL,
  thresholds = NULL,
  checkpoint_interval = 10L,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("gbt_classifier_"), ...) {

  ml_ratify_args()

  class <- "org.apache.spark.ml.classification.GBTClassifier"

  jobj <- (if (spark_version(x) < "2.2.0")
    ml_new_predictor(x, class, uid, features_col,
                     label_col, prediction_col)
    else
      ml_new_classifier(
        x, class, uid, features_col, label_col,
        prediction_col, probability_col, raw_prediction_col
      )) %>%
    invoke("setCheckpointInterval", checkpoint_interval) %>%
    invoke("setMaxBins", max_bins) %>%
    invoke("setMaxDepth", max_depth) %>%
    invoke("setMinInfoGain", min_info_gain) %>%
    invoke("setMinInstancesPerNode", min_instances_per_node) %>%
    invoke("setCacheNodeIds", cache_node_ids) %>%
    invoke("setMaxMemoryInMB", max_memory_in_mb) %>%
    invoke("setLossType", loss_type) %>%
    invoke("setMaxIter", max_iter) %>%
    invoke("setStepSize", step_size) %>%
    invoke("setSubsamplingRate", subsampling_rate)

  if(!rlang::is_null(thresholds) && spark_version(x) >= "2.2.0")
    jobj <- invoke(jobj, "setThresholds", thresholds)

  if (!rlang::is_null(seed))
    jobj <- invoke(jobj, "setSeed", seed)

  new_ml_gbt_classifier(jobj)
}

#' @export
ml_gbt_classifier.ml_pipeline <- function(
  x,
  formula = NULL,
  max_iter = 20L,
  max_depth = 5L,
  step_size = 0.1,
  subsampling_rate = 1,
  min_instances_per_node = 1L,
  max_bins = 32L,
  min_info_gain = 0,
  loss_type = "logistic",
  seed = NULL,
  thresholds = NULL,
  checkpoint_interval = 10L,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("gbt_classifier_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_gbt_classifier.tbl_spark <- function(
  x,
  formula = NULL,
  max_iter = 20L,
  max_depth = 5L,
  step_size = 0.1,
  subsampling_rate = 1,
  min_instances_per_node = 1L,
  max_bins = 32L,
  min_info_gain = 0,
  loss_type = "logistic",
  seed = NULL,
  thresholds = NULL,
  checkpoint_interval = 10L,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("gbt_classifier_"),
  response = NULL,
  features = NULL,
  predicted_label_col = "predicted_label", ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    call <- if (identical(sys.call(sys.parent())[[1]], quote(ml_gradient_boosted_trees)))
      sys.call(sys.parent())
    ml_generate_ml_model(x, predictor, formula, features_col, label_col,
                         "classification", new_ml_model_gbt_classification,
                         predicted_label_col, call = call)
  }
}

# Validator
ml_validator_gbt_classifier <- function(args, nms) {
  old_new_mapping <- c(
    ml_tree_param_mapping(),
    list(
      num.trees = "max_iter",
      loss.type = "loss_type",
      sample.rate = "subsampling_rate"
    )
  )

  args %>%
    ml_validate_decision_tree_args() %>%
    ml_validate_args({
      if (rlang::env_has(nms = "thresholds") && !rlang::is_null(thresholds))
        thresholds <- lapply(thresholds, ensure_scalar_double)
      max_iter <- ensure_scalar_integer(max_iter)
      step_size <- ensure_scalar_double(step_size)
      subsampling_rate <- ensure_scalar_double(subsampling_rate)
      loss_type <- ensure_scalar_character(loss_type)
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_gbt_classifier <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_gbt_classifier")
}

new_ml_gbt_classification_model <- function(jobj) {

  new_ml_prediction_model(
    jobj,
    feature_importances = try_null(read_spark_vector(jobj, "featureImportances")),
    num_trees = invoke(jobj, "numTrees"),
    num_classes = try_null(invoke(jobj, "numClasses")),
    num_features = invoke(jobj, "numFeatures"),
    total_num_nodes = invoke(jobj, "totalNumNodes"),
    tree_weights = invoke(jobj, "treeWeights"),
    trees = invoke(jobj, "trees") %>%
      lapply(new_ml_decision_tree_regression_model),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    probability_col = try_null(invoke(jobj, "getProbabilityCol")),
    raw_prediction_col = try_null(invoke(jobj, "getRawPredictionCol")),
    thresholds = try_null(invoke(jobj, "getThresholds")),
    subclass = "ml_gbt_classification_model")
}

new_ml_model_gbt_classification <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names,
  index_labels, call) {

  jobj <- spark_jobj(model)
  sc <- spark_connection(model)

  new_ml_model_classification(
    pipeline, pipeline_model, model, dataset, formula,
    subclass = "ml_model_gbt_classification",
    .features = feature_names,
    .index_labels = index_labels,
    .call = call
  )
}
