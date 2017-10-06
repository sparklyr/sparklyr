#' @rdname ml_decision_tree
#' @template roxlate-ml-probabilistic-classifier-params
#' @export
ml_decision_tree_classifier <- function(
  x,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  checkpoint_interval = 10L,
  impurity = "gini",
  max_bins = 32L,
  max_depth = 5L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("decision_tree_classifier_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...
) {
  UseMethod("ml_decision_tree_classifier")
}

#' @export
ml_decision_tree_classifier.spark_connection <- function(
  x,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  checkpoint_interval = 10L,
  impurity = "gini",
  max_bins = 32L,
  max_depth = 5L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("decision_tree_classifier_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...) {

  ml_ratify_args()

  jobj <- ml_new_classifier(
    x, "org.apache.spark.ml.classification.DecisionTreeClassifier", uid,
    features_col, label_col, prediction_col, probability_col, raw_prediction_col
  ) %>%
    invoke("setCheckpointInterval", checkpoint_interval) %>%
    invoke("setImpurity", impurity) %>%
    invoke("setMaxBins", max_bins) %>%
    invoke("setMaxDepth", max_depth) %>%
    invoke("setMinInfoGain", min_info_gain) %>%
    invoke("setMinInstancesPerNode", min_instances_per_node) %>%
    invoke("setCacheNodeIds", cache_node_ids) %>%
    invoke("setMaxMemoryInMB", max_memory_in_mb)

  if(!rlang::is_null(thresholds))
    jobj <- invoke(jobj, "setThresholds", thresholds)

  if (!rlang::is_null(seed))
    jobj <- invoke(jobj, "setSeed", seed)

  new_ml_decision_tree_classifier(jobj)
}

#' @export
ml_decision_tree_classifier.ml_pipeline <- function(
  x,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  checkpoint_interval = 10L,
  impurity = "gini",
  max_bins = 32L,
  max_depth = 5L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("decision_tree_classifier_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_decision_tree_classifier.tbl_spark <- function(
  x,
  formula = NULL,
  response = NULL,
  features = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  checkpoint_interval = 10L,
  impurity = "gini",
  max_bins = 32L,
  max_depth = 5L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("decision_tree_classifier_"), ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {

    sc <- spark_connection(x)
    r_formula <- ft_r_formula(sc, formula, features_col,
                              label_col, force_index_label = TRUE,
                              dataset = x)
    pipeline <- ml_pipeline(r_formula, predictor)

    pipeline_model <- pipeline %>%
      ml_fit(x)

    new_ml_model_decision_tree_classification(
      pipeline,
      pipeline_model,
      model = pipeline_model %>% ml_stage(2),
      dataset = x,
      formula = formula)
  }
}

# Validator
ml_validator_decision_tree_classifier <- function(args, nms) {
  args %>%
    ml_validate_decision_tree_args() %>%
    ml_validate_args({
      if (!rlang::is_null(thresholds))
        thresholds <- lapply(thresholds, ensure_scalar_double)
      impurity <- rlang::arg_match(impurity, c("gini", "entropy"))
    }, ml_tree_param_mapping()) %>%
    ml_extract_args(nms, ml_tree_param_mapping())
}

# Constructors

new_ml_decision_tree_classifier <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_decision_tree_classifier")
}

new_ml_decision_tree_classification_model <- function(jobj) {

  new_ml_prediction_model(
    jobj,
    depth = invoke(jobj, "depth"),
    feature_importances = try_null(read_spark_vector(jobj, "featureImportances")),
    num_features = invoke(jobj, "numFeatures"),
    num_nodes = invoke(jobj, "numNodes"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    probability_col = invoke(jobj, "getProbabilityCol"),
    raw_prediction_col = invoke(jobj, "getRawPredictionCol"),
    thresholds = try_null(invoke(jobj, "getThresholds")),
    subclass = "ml_decision_tree_classification_model")
}

new_ml_model_decision_tree_classification <- function(
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
    subclass = "ml_model_decision_tree_classification",
    .response = gsub("~.+$", "", formula) %>% trimws(),
    .features = feature_names,
    .call = call
  )
}

# Generic implementations

#' @export
ml_fit.ml_decision_tree_classifier <- function(x, data, ...) {
  jobj <- spark_jobj(x) %>%
    invoke("fit", spark_dataframe(data))
  new_ml_decision_tree_classification_model(jobj)
}
