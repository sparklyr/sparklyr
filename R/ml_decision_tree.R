NULL

#' @rdname ml_decision_tree
#' @template roxlate-ml-probabilistic-classifier-params
#' @export
ml_decision_tree_classifier <- function(
  x,
  formula = NULL,
  max_depth = 5,
  max_bins = 32,
  min_instances_per_node = 1,
  min_info_gain = 0,
  impurity = "gini",
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10,
  max_memory_in_mb = 256,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("decision_tree_classifier_"),
  ...
) {
  check_dots_used()
  UseMethod("ml_decision_tree_classifier")
}

#' @export
ml_decision_tree_classifier.spark_connection <- function(
  x,
  formula = NULL,
  max_depth = 5,
  max_bins = 32,
  min_instances_per_node = 1,
  min_info_gain = 0,
  impurity = "gini",
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10,
  max_memory_in_mb = 256,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("decision_tree_classifier_"),
  ...
) {
  .args <- list(
    max_depth = max_depth,
    max_bins = max_bins,
    min_instances_per_node = min_instances_per_node,
    min_info_gain = min_info_gain,
    impurity = impurity,
    seed = seed,
    thresholds = thresholds,
    cache_node_ids = cache_node_ids,
    checkpoint_interval = checkpoint_interval,
    max_memory_in_mb = max_memory_in_mb,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    probability_col = probability_col,
    raw_prediction_col = raw_prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_decision_tree_classifier()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.classification.DecisionTreeClassifier",
    uid,
    features_col = .args[["features_col"]],
    label_col = .args[["label_col"]],
    prediction_col = .args[["prediction_col"]],
    probability_col = .args[["probability_col"]],
    raw_prediction_col = .args[["raw_prediction_col"]]
  ) %>%
    invoke("setCheckpointInterval", .args[["checkpoint_interval"]]) %>%
    invoke("setImpurity", .args[["impurity"]]) %>%
    invoke("setMaxBins", .args[["max_bins"]]) %>%
    invoke("setMaxDepth", .args[["max_depth"]]) %>%
    invoke("setMinInfoGain", .args[["min_info_gain"]]) %>%
    invoke("setMinInstancesPerNode", .args[["min_instances_per_node"]]) %>%
    invoke("setCacheNodeIds", .args[["cache_node_ids"]]) %>%
    invoke("setMaxMemoryInMB", .args[["max_memory_in_mb"]]) %>%
    jobj_set_param("setThresholds", .args[["thresholds"]]) %>%
    jobj_set_param("setSeed", .args[["seed"]])

  new_ml_decision_tree_classifier(jobj)
}

#' @export
ml_decision_tree_classifier.ml_pipeline <- function(
  x,
  formula = NULL,
  max_depth = 5,
  max_bins = 32,
  min_instances_per_node = 1,
  min_info_gain = 0,
  impurity = "gini",
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10,
  max_memory_in_mb = 256,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("decision_tree_classifier_"),
  ...
) {
  stage <- ml_decision_tree_classifier.spark_connection(
    x = spark_connection(x),
    formula = formula,
    max_depth = max_depth,
    max_bins = max_bins,
    min_instances_per_node = min_instances_per_node,
    min_info_gain = min_info_gain,
    impurity = impurity,
    seed = seed,
    thresholds = thresholds,
    cache_node_ids = cache_node_ids,
    checkpoint_interval = checkpoint_interval,
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
ml_decision_tree_classifier.tbl_spark <- function(
  x,
  formula = NULL,
  max_depth = 5,
  max_bins = 32,
  min_instances_per_node = 1,
  min_info_gain = 0,
  impurity = "gini",
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10,
  max_memory_in_mb = 256,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("decision_tree_classifier_"),
  response = NULL,
  features = NULL,
  predicted_label_col = "predicted_label",
  ...
) {
  formula <- ml_standardize_formula(formula, response, features)

  stage <- ml_decision_tree_classifier.spark_connection(
    x = spark_connection(x),
    formula = formula,
    max_depth = max_depth,
    max_bins = max_bins,
    min_instances_per_node = min_instances_per_node,
    min_info_gain = min_info_gain,
    impurity = impurity,
    seed = seed,
    thresholds = thresholds,
    cache_node_ids = cache_node_ids,
    checkpoint_interval = checkpoint_interval,
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
      new_ml_model_decision_tree_classification,
      predictor = stage,
      formula = formula,
      dataset = x,
      features_col = features_col,
      label_col = label_col,
      predicted_label_col = predicted_label_col
    )
  }
}

validator_ml_decision_tree_classifier <- function(.args) {
  .args <- ml_validate_decision_tree_args(.args)
  .args[["thresholds"]] <- cast_double_list(
    .args[["thresholds"]],
    allow_null = TRUE
  )
  .args[["impurity"]] <- cast_choice(.args[["impurity"]], c("gini", "entropy"))
  .args
}

new_ml_decision_tree_classifier <- function(jobj) {
  new_ml_probabilistic_classifier(jobj, class = "ml_decision_tree_classifier")
}

new_ml_decision_tree_classification_model <- function(jobj) {
  new_ml_probabilistic_classification_model(
    jobj,
    # `depth` and `featureImportances` are lazy vals in Spark.
    depth = function() invoke(jobj, "depth"),
    feature_importances = possibly_null(
      ~ read_spark_vector(jobj, "featureImportances")
    ),
    # `numNodes` is a def in Spark.
    num_nodes = function() invoke(jobj, "numNodes"),
    class = "ml_decision_tree_classification_model"
  )
}

#' @rdname ml_decision_tree
#' @param variance_col (Optional) Column name for the biased sample variance of prediction.
#' @export
ml_decision_tree_regressor <- function(
  x,
  formula = NULL,
  max_depth = 5,
  max_bins = 32,
  min_instances_per_node = 1,
  min_info_gain = 0,
  impurity = "variance",
  seed = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10,
  max_memory_in_mb = 256,
  variance_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("decision_tree_regressor_"),
  ...
) {
  check_dots_used()
  UseMethod("ml_decision_tree_regressor")
}

ml_decision_tree_regressor_impl <- function(
  x,
  formula = NULL,
  max_depth = 5,
  max_bins = 32,
  min_instances_per_node = 1,
  min_info_gain = 0,
  impurity = "variance",
  seed = NULL,
  cache_node_ids = FALSE,
  checkpoint_interval = 10,
  max_memory_in_mb = 256,
  variance_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("decision_tree_regressor_"),
  response = NULL,
  features = NULL,
  ...
) {
  variance_col <- param_min_version(x, variance_col, "2.0.0")

  ml_process_model(
    x = x,
    r_class = "ml_decision_tree_regressor",
    ml_function = new_ml_model_decision_tree_regression,
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
      variance_col = variance_col,
      seed = seed
    )
  )
}

# ------------------------------- Methods --------------------------------------
#' @export
ml_decision_tree_regressor.spark_connection <- ml_decision_tree_regressor_impl

#' @export
ml_decision_tree_regressor.ml_pipeline <- ml_decision_tree_regressor_impl

#' @export
ml_decision_tree_regressor.tbl_spark <- ml_decision_tree_regressor_impl

# ---------------------------- Constructors ------------------------------------
new_ml_decision_tree_regression_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    # `depth` and `featureImportances` are lazy vals in Spark.
    depth = function() invoke(jobj, "depth"),
    feature_importances = possibly_null(
      ~ read_spark_vector(jobj, "featureImportances")
    ),
    # `numNodes` is a def in Spark.
    num_nodes = function() invoke(jobj, "numNodes"),
    variance_col = possibly_null(invoke)(jobj, "getVarianceCol"),
    class = "ml_decision_tree_regression_model"
  )
}

#' Spark ML -- Decision Trees
#'
#' Perform classification and regression using decision trees.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-decision-trees-base-params
#' @template roxlate-ml-old-feature-response
#' @template roxlate-ml-formula-params
#' @param impurity Criterion used for information gain calculation. Supported: "entropy"
#'   and "gini" (default) for classification and "variance" (default) for regression. For
#'   \code{ml_decision_tree}, setting \code{"auto"} will default to the appropriate
#'   criterion based on model type.
#' @name ml_decision_tree
NULL

#' @rdname ml_decision_tree
#' @template roxlate-ml-decision-trees-type
#' @details \code{ml_decision_tree} is a wrapper around \code{ml_decision_tree_regressor.tbl_spark} and \code{ml_decision_tree_classifier.tbl_spark} and calls the appropriate method based on model type.
#' @template roxlate-ml-predictor-params
#'
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' partitions <- iris_tbl %>%
#'   sdf_random_split(training = 0.7, test = 0.3, seed = 1111)
#'
#' iris_training <- partitions$training
#' iris_test <- partitions$test
#'
#' dt_model <- iris_training %>%
#'   ml_decision_tree(Species ~ .)
#'
#' pred <- ml_predict(dt_model, iris_test)
#'
#' ml_multiclass_classification_evaluator(pred)
#' }
#'
#' @export
ml_decision_tree <- function(
  x,
  formula = NULL,
  type = c("auto", "regression", "classification"),
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  variance_col = NULL,
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  checkpoint_interval = 10L,
  impurity = "auto",
  max_bins = 32L,
  max_depth = 5L,
  min_info_gain = 0,
  min_instances_per_node = 1L,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256L,
  uid = random_string("decision_tree_"),
  response = NULL,
  features = NULL,
  ...
) {
  formula <- ml_standardize_formula(formula, response, features)
  response_col <- gsub("~.+$", "", formula) %>% trimws()

  sdf <- spark_dataframe(x)
  # choose classification vs. regression model based on column type
  schema <- sdf_schema(sdf)
  response_type <- schema[[response_col]]$type

  type <- rlang::arg_match(type)
  model_type <- if (!identical(type, "auto")) {
    type
  } else {
    if (response_type %in% c("DoubleType", "IntegerType")) {
      "regression"
    } else {
      "classification"
    }
  }

  impurity <- if (identical(impurity, "auto")) {
    if (identical(model_type, "regression")) "variance" else "gini"
  } else if (identical(model_type, "classification")) {
    if (!impurity %in% c("gini", "entropy")) {
      stop(
        "`impurity` must be \"gini\" or \"entropy\" for classification.",
        call. = FALSE
      )
    }
    impurity
  } else {
    if (!identical(impurity, "variance")) {
      stop("`impurity` must be \"variance\" for regression.", call. = FALSE)
    }
    impurity
  }

  switch(
    model_type,
    regression = ml_decision_tree_regressor(
      x = x,
      formula = formula,
      max_depth = max_depth,
      max_bins = max_bins,
      min_instances_per_node = min_instances_per_node,
      min_info_gain = min_info_gain,
      impurity = impurity,
      seed = seed,
      cache_node_ids = cache_node_ids,
      checkpoint_interval = checkpoint_interval,
      max_memory_in_mb = max_memory_in_mb,
      variance_col = variance_col,
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      uid = uid,
      ...
    ),
    classification = ml_decision_tree_classifier(
      x = x,
      formula = formula,
      max_depth = max_depth,
      max_bins = max_bins,
      min_instances_per_node = min_instances_per_node,
      min_info_gain = min_info_gain,
      impurity = impurity,
      seed = seed,
      thresholds = thresholds,
      cache_node_ids = cache_node_ids,
      checkpoint_interval = checkpoint_interval,
      max_memory_in_mb = max_memory_in_mb,
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      probability_col = probability_col,
      raw_prediction_col = raw_prediction_col,
      uid = uid,
      ...
    )
  )
}

new_ml_model_decision_tree_classification <- function(
  pipeline_model,
  formula,
  dataset,
  label_col,
  features_col,
  predicted_label_col
) {
  new_ml_model_classification(
    pipeline_model,
    formula,
    dataset = dataset,
    label_col = label_col,
    features_col = features_col,
    predicted_label_col = predicted_label_col,
    class = "ml_model_decision_tree_classification"
  )
}

new_ml_model_decision_tree_regression <- function(
  pipeline_model,
  formula,
  dataset,
  label_col,
  features_col
) {
  new_ml_model_regression(
    pipeline_model,
    formula,
    dataset = dataset,
    label_col = label_col,
    features_col = features_col,
    class = "ml_model_decision_tree_regression"
  )
}
