#' @rdname ml_random_forest
#' @template roxlate-ml-probabilistic-classifier-params
#' @export
ml_random_forest_classifier <- function(
  x,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  feature_subset_strategy = "auto",
  impurity = "gini",
  checkpoint_interval = 10L,
  max_bins = 32L,
  max_depth = 5L,
  num_trees = 20L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  subsampling_rate = 1,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("random_forest_classifier_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...
) {
  UseMethod("ml_random_forest_classifier")
}

#' @export
ml_random_forest_classifier.spark_connection <- function(
  x,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  feature_subset_strategy = "auto",
  impurity = "gini",
  checkpoint_interval = 10L,
  max_bins = 32L,
  max_depth = 5L,
  num_trees = 20L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  subsampling_rate = 1,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("random_forest_classifier_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...) {

  ml_ratify_args()

  class <- "org.apache.spark.ml.classification.RandomForestClassifier"

  jobj <- ml_new_classifier(
    x, class, uid, features_col, label_col,
    prediction_col, probability_col, raw_prediction_col
  ) %>%
    invoke("setCheckpointInterval", checkpoint_interval) %>%
    invoke("setMaxBins", max_bins) %>%
    invoke("setMaxDepth", max_depth) %>%
    invoke("setMinInfoGain", min_info_gain) %>%
    invoke("setMinInstancesPerNode", min_instances_per_node) %>%
    invoke("setCacheNodeIds", cache_node_ids) %>%
    invoke("setMaxMemoryInMB", max_memory_in_mb) %>%
    invoke("setNumTrees", num_trees) %>%
    invoke("setSubsamplingRate", subsampling_rate) %>%
    invoke("setFeatureSubsetStrategy", feature_subset_strategy) %>%
    invoke("setImpurity", impurity)

  if(!rlang::is_null(thresholds))
    jobj <- invoke(jobj, "setThresholds", thresholds)

  if (!rlang::is_null(seed))
    jobj <- invoke(jobj, "setSeed", seed)

  new_ml_random_forest_classifier(jobj)
}

#' @export
ml_random_forest_classifier.ml_pipeline <- function(
  x,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  feature_subset_strategy = "auto",
  impurity = "gini",
  checkpoint_interval = 10L,
  max_bins = 32L,
  max_depth = 5L,
  num_trees = 20L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  subsampling_rate = 1,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("random_forest_classifier_"),
  formula = NULL,
  response = NULL,
  features = NULL, ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_random_forest_classifier.tbl_spark <- function(
  x,
  formula = NULL,
  response = NULL,
  features = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  feature_subset_strategy = "auto",
  impurity = "gini",
  checkpoint_interval = 10L,
  max_bins = 32L,
  max_depth = 5L,
  num_trees = 20L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  subsampling_rate = 1,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("random_forest_classifier_"), ...) {

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

    new_ml_model_random_forest_classification(
      pipeline,
      pipeline_model,
      model = pipeline_model %>% ml_stage(2),
      dataset = x,
      formula = formula)
  }
}

# Validator
ml_validator_random_forest_classifier <- function(args, nms) {
  old_new_mapping <- c(
    ml_tree_param_mapping(),
    list(
      sample.rate = "subsampling_rate",
      num.trees = "num_trees",
      col.sample.rate = "feature_subset_strategy"
    ))

  args %>%
    ml_validate_decision_tree_args() %>%
    ml_validate_args({
      if (!rlang::is_null(thresholds))
        thresholds <- lapply(thresholds, ensure_scalar_double)
      num_trees <- ensure_scalar_integer(num_trees)
      subsampling_rate <- ensure_scalar_double(subsampling_rate)
      feature_subset_strategy <- ensure_scalar_character(feature_subset_strategy)
      impurity <- rlang::arg_match(impurity, c("gini", "entropy"))
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_random_forest_classifier <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_random_forest_classifier")
}

new_ml_random_forest_classification_model <- function(jobj) {

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
    subclass = "ml_random_forest_classification_model")
}

new_ml_model_random_forest_classification <- function(
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
    subclass = "ml_model_random_forest_classification",
    .response = gsub("~.+$", "", formula) %>% trimws(),
    .features = feature_names,
    .call = call
  )
}

# Generic implementations

#' @export
ml_fit.ml_random_forest_classifier <- function(x, data, ...) {
  jobj <- spark_jobj(x) %>%
    invoke("fit", spark_dataframe(data))
  new_ml_random_forest_classification_model(jobj)
}
