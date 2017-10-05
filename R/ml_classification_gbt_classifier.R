#' @export
ml_gbt_classifier <- function(
  x,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  checkpoint_interval = 10L,
  loss_type = "logistic",
  max_bins = 32L,
  max_depth = 5L,
  max_iter = 20L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  step_size = 0.1,
  subsampling_rate = 1,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("gbt_classifier_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...
) {
  UseMethod("ml_gbt_classifier")
}

#' @export
ml_gbt_classifier.spark_connection <- function(
  x,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  checkpoint_interval = 10L,
  max_bins = 32L,
  max_depth = 5L,
  max_iter = 20L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  step_size = 0.1,
  subsampling_rate = 1,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("gbt_classifier_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...) {

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
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  checkpoint_interval = 10L,
  loss_type = "logistic",
  max_bins = 32L,
  max_depth = 5L,
  max_iter = 20L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  step_size = 0.1,
  subsampling_rate = 1,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("gbt_classifier_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_gbt_classifier.tbl_spark <- function(
  x,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  checkpoint_interval = 10L,
  loss_type = "logistic",
  max_bins = 32L,
  max_depth = 5L,
  max_iter = 20L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  step_size = 0.1,
  subsampling_rate = 1,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("gbt_classifier_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {

    sc <- spark_connection(x)
    r_formula <- ft_r_formula(sc, formula, features_col,
                              label_col,
                              dataset = x)
    pipeline <- ml_pipeline(r_formula, predictor)

    pipeline_model <- pipeline %>%
      ml_fit(x)

    new_ml_model_gbt_classification(
      pipeline,
      pipeline_model,
      model = pipeline_model %>% ml_stage(2),
      dataset = x,
      formula = formula)
  }
}

# Validator
ml_validator_gbt_classifier <- function(args, nms) {
  old_new_mapping <- list(
    max.bins = "max_bins",
    max.depth = "max_depth",
    num.trees = "max_iter",
    loss.type = "loss_type",
    min.info.gain = "min_info_gain",
    sample.rate = "subsampling_rate",
    min.rows = "min_instances_per_node",
    checkpoint.interval = "checkpoint_interval",
    cache.node.ids = "cache_node_ids",
    max.memory = "max_memory_in_mb"
  )

  args %>%
    ml_validate_args({
      max_bins <- ensure_scalar_integer(max_bins)
      max_depth <- ensure_scalar_integer(max_depth)
      min_info_gain <- ensure_scalar_double(min_info_gain)
      min_instances_per_node <- ensure_scalar_integer(min_instances_per_node)
      seed <- ensure_scalar_integer(seed, allow.null = TRUE)
      checkpoint_interval <- ensure_scalar_integer(checkpoint_interval)
      cache_node_ids <- ensure_scalar_boolean(cache_node_ids)
      max_memory_in_mb <- ensure_scalar_integer(max_memory_in_mb)
      if (!rlang::is_null(thresholds))
        thresholds <- lapply(thresholds, ensure_scalar_double)
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
    depth = invoke(jobj, "depth"),
    feature_importances = try_null(read_spark_vector(jobj, "featureImportances")),
    num_trees = invoke(jobj, "numTrees"),
    num_classes = invoke(jobj, "numClasses"),
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
  pipeline, pipeline_model, model, dataset, formula) {

  jobj <- spark_jobj(model)
  sc <- spark_connection(model)
  features_col <- ml_param(model, "features_col")
  label_col <- ml_param(model, "label_col")
  transformed_tbl <- pipeline_model %>%
    ml_transform(dataset)

  feature_names <- ml_column_metadata(transformed_tbl, features_col) %>%
    `[[`("attrs") %>%
    `[[`("numeric") %>%
    dplyr::pull("name")

  call <- rlang::ctxt_frame(rlang::ctxt_frame()$caller_pos)$expr

  new_ml_model_classification(
    pipeline, pipeline_model, model, dataset, formula,
    coefficients = coefficients,
    subclass = "ml_model_gbt_classification",
    .response = gsub("~.+$", "", formula) %>% trimws(),
    .features = feature_names,
    .call = call
  )
}

# Generic implementations

#' @export
ml_fit.ml_gbt_classifier <- function(x, data, ...) {
  jobj <- spark_jobj(x) %>%
    invoke("fit", spark_dataframe(data))
  new_ml_gbt_classification_model(jobj)
}
