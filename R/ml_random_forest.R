#' @rdname ml_random_forest
#' @template roxlate-ml-probabilistic-classifier-params
#' @export
ml_random_forest_classifier <- function(
  x,
  formula = NULL,
  num_trees = 20,
  subsampling_rate = 1,
  max_depth = 5,
  min_instances_per_node = 1,
  feature_subset_strategy = "auto",
  impurity = "gini",
  min_info_gain = 0,
  max_bins = 32,
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
  uid = random_string("random_forest_classifier_"),
  ...
) {
  check_dots_used()
  UseMethod("ml_random_forest_classifier")
}

#' @export
ml_random_forest_classifier.spark_connection <- function(
  x,
  formula = NULL,
  num_trees = 20,
  subsampling_rate = 1,
  max_depth = 5,
  min_instances_per_node = 1,
  feature_subset_strategy = "auto",
  impurity = "gini",
  min_info_gain = 0,
  max_bins = 32,
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
  uid = random_string("random_forest_classifier_"),
  ...
) {
  .args <- list(
    num_trees = num_trees,
    subsampling_rate = subsampling_rate,
    max_depth = max_depth,
    min_instances_per_node = min_instances_per_node,
    feature_subset_strategy = feature_subset_strategy,
    impurity = impurity,
    min_info_gain = min_info_gain,
    max_bins = max_bins,
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
    validator_ml_random_forest_classifier()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.classification.RandomForestClassifier",
    uid,
    features_col = .args[["features_col"]],
    label_col = .args[["label_col"]],
    prediction_col = .args[["prediction_col"]],
    probability_col = .args[["probability_col"]],
    raw_prediction_col = .args[["raw_prediction_col"]]
  ) %>%
    (function(obj) {
      do.call(
        invoke,
        c(
          obj,
          "%>%",
          Filter(
            function(x) !is.null(x),
            list(
              list("setCheckpointInterval", .args[["checkpoint_interval"]]),
              list("setMaxBins", .args[["max_bins"]]),
              list("setMaxDepth", .args[["max_depth"]]),
              list("setMinInfoGain", .args[["min_info_gain"]]),
              list("setMinInstancesPerNode", .args[["min_instances_per_node"]]),
              list("setCacheNodeIds", .args[["cache_node_ids"]]),
              list("setMaxMemoryInMB", .args[["max_memory_in_mb"]]),
              list("setNumTrees", .args[["num_trees"]]),
              list("setSubsamplingRate", .args[["subsampling_rate"]]),
              list(
                "setFeatureSubsetStrategy",
                .args[["feature_subset_strategy"]]
              ),
              list("setImpurity", .args[["impurity"]]),
              jobj_set_param_helper(
                obj,
                "setThresholds",
                .args[["thresholds"]]
              ),
              jobj_set_param_helper(obj, "setSeed", .args[["seed"]])
            )
          )
        )
      )
    })

  new_ml_random_forest_classifier(jobj)
}

#' @export
ml_random_forest_classifier.ml_pipeline <- function(
  x,
  formula = NULL,
  num_trees = 20,
  subsampling_rate = 1,
  max_depth = 5,
  min_instances_per_node = 1,
  feature_subset_strategy = "auto",
  impurity = "gini",
  min_info_gain = 0,
  max_bins = 32,
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
  uid = random_string("random_forest_classifier_"),
  ...
) {
  stage <- ml_random_forest_classifier.spark_connection(
    x = spark_connection(x),
    formula = formula,
    num_trees = num_trees,
    subsampling_rate = subsampling_rate,
    max_depth = max_depth,
    min_instances_per_node = min_instances_per_node,
    feature_subset_strategy = feature_subset_strategy,
    impurity = impurity,
    min_info_gain = min_info_gain,
    max_bins = max_bins,
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
ml_random_forest_classifier.tbl_spark <- function(
  x,
  formula = NULL,
  num_trees = 20,
  subsampling_rate = 1,
  max_depth = 5,
  min_instances_per_node = 1,
  feature_subset_strategy = "auto",
  impurity = "gini",
  min_info_gain = 0,
  max_bins = 32,
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
  uid = random_string("random_forest_classifier_"),
  response = NULL,
  features = NULL,
  predicted_label_col = "predicted_label",
  ...
) {
  formula <- ml_standardize_formula(formula, response, features)

  stage <- ml_random_forest_classifier.spark_connection(
    x = spark_connection(x),
    formula = NULL,
    num_trees = num_trees,
    subsampling_rate = subsampling_rate,
    max_depth = max_depth,
    min_instances_per_node = min_instances_per_node,
    feature_subset_strategy = feature_subset_strategy,
    impurity = impurity,
    min_info_gain = min_info_gain,
    max_bins = max_bins,
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
      new_ml_model_random_forest_classification,
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
validator_ml_random_forest_classifier <- function(.args) {
  .args <- .args %>% ml_validate_decision_tree_args()
  .args[["thresholds"]] <- cast_double_list(
    .args[["thresholds"]],
    allow_null = TRUE
  )
  .args[["num_trees"]] <- cast_scalar_integer(.args[["num_trees"]])
  .args[["subsampling_rate"]] <- cast_scalar_double(.args[["subsampling_rate"]])
  .args[["impurity"]] <- cast_choice(.args[["impurity"]], c("gini", "entropy"))
  .args
}

# Constructors

new_ml_random_forest_classifier <- function(jobj) {
  new_ml_probabilistic_classifier(jobj, class = "ml_random_forest_classifier")
}

new_ml_random_forest_classification_model <- function(jobj) {
  new_ml_probabilistic_classification_model(
    jobj,
    # `lazy val featureImportances`
    feature_importances = possibly_null(
      ~ read_spark_vector(jobj, "featureImportances")
    ),
    # `lazy val totalNumNodes`
    total_num_nodes = function() invoke(jobj, "totalNumNodes"),
    # `def treeWeights`, `def trees`
    tree_weights = function() invoke(jobj, "treeWeights"),
    trees = function() {
      invoke(jobj, "trees") %>%
        purrr::map(new_ml_decision_tree_regression_model)
    },
    class = "ml_random_forest_classification_model"
  )
}

#' @rdname ml_random_forest
#' @export
ml_random_forest_regressor <- function(
  x,
  formula = NULL,
  num_trees = 20,
  subsampling_rate = 1,
  max_depth = 5,
  min_instances_per_node = 1,
  feature_subset_strategy = "auto",
  impurity = "variance",
  min_info_gain = 0,
  max_bins = 32,
  seed = NULL,
  checkpoint_interval = 10,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("random_forest_regressor_"),
  ...
) {
  check_dots_used()
  UseMethod("ml_random_forest_regressor")
}

ml_random_forest_regressor_impl <- function(
  x,
  formula = NULL,
  num_trees = 20,
  subsampling_rate = 1,
  max_depth = 5,
  min_instances_per_node = 1,
  feature_subset_strategy = "auto",
  impurity = "variance",
  min_info_gain = 0,
  max_bins = 32,
  seed = NULL,
  checkpoint_interval = 10,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("random_forest_regressor_"),
  response = NULL,
  features = NULL,
  ...
) {
  ml_process_model(
    x = x,
    r_class = "ml_random_forest_regressor",
    ml_function = new_ml_model_random_forest_regression,
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
      seed = seed,
      num_trees = num_trees,
      subsampling_rate = subsampling_rate,
      feature_subset_strategy = feature_subset_strategy
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
    feature_importances = possibly_null(
      ~ read_spark_vector(jobj, "featureImportances")
    ),
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

#' Spark ML -- Random Forest
#'
#' Perform classification and regression using random forests.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-decision-trees-base-params
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-predictor-params
#' @template roxlate-ml-old-feature-response
#' @param impurity Criterion used for information gain calculation. Supported: "entropy"
#'   and "gini" (default) for classification and "variance" (default) for regression. For
#'   \code{ml_decision_tree}, setting \code{"auto"} will default to the appropriate
#'   criterion based on model type.
#' @template roxlate-ml-feature-subset-strategy
#' @param num_trees Number of trees to train (>= 1). If 1, then no bootstrapping is used. If > 1, then bootstrapping is done.
#' @param subsampling_rate Fraction of the training data used for learning each decision tree, in range (0, 1]. (default = 1.0)
#' @name ml_random_forest
NULL

#' @rdname ml_random_forest
#' @template roxlate-ml-decision-trees-type
#' @details \code{ml_random_forest} is a wrapper around \code{ml_random_forest_regressor.tbl_spark} and \code{ml_random_forest_classifier.tbl_spark} and calls the appropriate method based on model type.
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
#' rf_model <- iris_training %>%
#'   ml_random_forest(Species ~ ., type = "classification")
#'
#' pred <- ml_predict(rf_model, iris_test)
#'
#' ml_multiclass_classification_evaluator(pred)
#' }
#' @export
ml_random_forest <- function(
  x,
  formula = NULL,
  type = c("auto", "regression", "classification"),
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  feature_subset_strategy = "auto",
  impurity = "auto",
  checkpoint_interval = 10,
  max_bins = 32,
  max_depth = 5,
  num_trees = 20,
  min_info_gain = 0,
  min_instances_per_node = 1,
  subsampling_rate = 1,
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256,
  uid = random_string("random_forest_"),
  response = NULL,
  features = NULL,
  ...
) {
  formula <- ml_standardize_formula(formula, response, features)
  response_col <- gsub("~.+$", "", formula) %>% trimws()

  sdf <- spark_dataframe(x)
  # choose classification vs. regression model based on column type
  schema <- sdf_schema(sdf)
  if (!response_col %in% names(schema)) {
    stop("`", response_col, "` is not a column in the input dataset.")
  }

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
      stop("`impurity` must be \"gini\" or \"entropy\" for classification.")
    }
    impurity
  } else {
    if (!identical(impurity, "variance")) {
      stop("`impurity` must be \"variance\" for regression.")
    }
    impurity
  }

  switch(
    model_type,
    regression = ml_random_forest_regressor(
      x = x,
      formula = formula,
      num_trees = num_trees,
      subsampling_rate = subsampling_rate,
      max_depth = max_depth,
      min_instances_per_node = min_instances_per_node,
      feature_subset_strategy = feature_subset_strategy,
      impurity = impurity,
      min_info_gain = min_info_gain,
      max_bins = max_bins,
      seed = seed,
      checkpoint_interval = checkpoint_interval,
      cache_node_ids = cache_node_ids,
      max_memory_in_mb = max_memory_in_mb,
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      uid = uid,
      ...
    ),
    classification = ml_random_forest_classifier(
      x = x,
      formula = formula,
      num_trees = num_trees,
      subsampling_rate = subsampling_rate,
      max_depth = max_depth,
      min_instances_per_node = min_instances_per_node,
      feature_subset_strategy = feature_subset_strategy,
      impurity = impurity,
      min_info_gain = min_info_gain,
      max_bins = max_bins,
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
  )
}

new_ml_model_random_forest_classification <- function(
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
    class = "ml_model_random_forest_classification"
  )
}

new_ml_model_random_forest_regression <- function(
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
    class = "ml_model_random_forest_regression"
  )
}
