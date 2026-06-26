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

#' @export
ml_gbt_classifier.spark_connection <- function(
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

  jobj <- (if (spark_version(x) < "2.2.0") {
    spark_pipeline_stage(
      x,
      stage_class,
      uid,
      features_col = .args[["features_col"]],
      label_col = .args[["label_col"]],
      prediction_col = .args[["prediction_col"]]
    )
  } else {
    spark_pipeline_stage(
      x,
      stage_class,
      uid,
      features_col = .args[["features_col"]],
      label_col = .args[["label_col"]],
      prediction_col = .args[["prediction_col"]],
      probability_col = .args[["probability_col"]],
      raw_prediction_col = .args[["raw_prediction_col"]]
    )
  }) %>%
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
              list("setLossType", .args[["loss_type"]]),
              list("setMaxIter", .args[["max_iter"]]),
              list("setStepSize", .args[["step_size"]]),
              list("setSubsamplingRate", .args[["subsampling_rate"]]),
              jobj_set_param_helper(
                obj,
                "setFeatureSubsetStrategy",
                .args[["feature_subset_strategy"]],
                "2.3.0",
                "auto"
              ),
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

  new_ml_gbt_classifier(jobj)
}

#' @export
ml_gbt_classifier.ml_pipeline <- function(
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
ml_gbt_classifier.tbl_spark <- function(
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

  .args[["thresholds"]] <- cast_double_list(
    .args[["thresholds"]],
    allow_null = TRUE
  )
  .args[["max_iter"]] <- cast_scalar_integer(.args[["max_iter"]])
  .args[["step_size"]] <- cast_scalar_double(.args[["step_size"]])
  .args[["subsampling_rate"]] <- cast_scalar_double(.args[["subsampling_rate"]])
  .args[["loss_type"]] <- cast_choice(.args[["loss_type"]], "logistic")
  .args[["feature_subset_strategy"]] <- cast_string(.args[[
    "feature_subset_strategy"
  ]])
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

#' @rdname ml_gradient_boosted_trees
#' @template roxlate-ml-predictor-params
#' @export
ml_gbt_regressor <- function(
  x,
  formula = NULL,
  max_iter = 20,
  max_depth = 5,
  step_size = 0.1,
  subsampling_rate = 1,
  feature_subset_strategy = "auto",
  min_instances_per_node = 1,
  max_bins = 32,
  min_info_gain = 0,
  loss_type = "squared",
  seed = NULL,
  checkpoint_interval = 10,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("gbt_regressor_"),
  ...
) {
  check_dots_used()
  UseMethod("ml_gbt_regressor")
}

ml_gbt_regressor_impl <- function(
  x,
  formula = NULL,
  max_iter = 20,
  max_depth = 5,
  step_size = 0.1,
  subsampling_rate = 1,
  feature_subset_strategy = "auto",
  min_instances_per_node = 1,
  max_bins = 32,
  min_info_gain = 0,
  loss_type = "squared",
  seed = NULL,
  checkpoint_interval = 10,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("gbt_regressor_"),
  response = NULL,
  features = NULL,
  ...
) {
  feature_subset_strategy <- param_min_version(
    x,
    feature_subset_strategy,
    "2.3.0",
    "auto"
  )

  ml_process_model(
    x = x,
    r_class = "ml_gbt_regressor",
    ml_function = new_ml_model_gbt_regression,
    features = features,
    response = response,
    uid = uid,
    formula = formula,
    invoke_steps = list(
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      max_iter = max_iter,
      step_size = step_size,
      subsampling_rate = subsampling_rate,
      feature_subset_strategy = feature_subset_strategy,
      loss_type = loss_type,
      checkpoint_interval = checkpoint_interval,
      max_bins = max_bins,
      max_depth = max_depth,
      min_info_gain = min_info_gain,
      min_instances_per_node = min_instances_per_node,
      cache_node_ids = cache_node_ids,
      max_memory_in_mb = max_memory_in_mb,
      seed = seed
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
    feature_importances = possibly_null(
      ~ read_spark_vector(jobj, "featureImportances")
    ),
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

#' Spark ML -- Gradient Boosted Trees
#'
#' Perform binary classification and regression using gradient boosted trees. Multiclass classification is not supported yet.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-decision-trees-base-params
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-feature-subset-strategy
#' @param max_iter Maxmimum number of iterations.
#' @param step_size Step size (a.k.a. learning rate) in interval (0, 1] for shrinking the contribution of each estimator. (default = 0.1)
#' @param subsampling_rate Fraction of the training data used for learning each decision tree, in range (0, 1]. (default = 1.0)
#' @param loss_type Loss function which GBT tries to minimize. Supported: \code{"squared"} (L2) and \code{"absolute"} (L1) (default = squared) for regression and \code{"logistic"} (default) for classification. For \code{ml_gradient_boosted_trees}, setting \code{"auto"}
#'   will default to the appropriate loss type based on model type.
#' @name ml_gradient_boosted_trees
NULL

#' @rdname ml_gradient_boosted_trees
#' @template roxlate-ml-decision-trees-type
#' @details \code{ml_gradient_boosted_trees} is a wrapper around \code{ml_gbt_regressor.tbl_spark} and \code{ml_gbt_classifier.tbl_spark} and calls the appropriate method based on model type.
#' @template roxlate-ml-old-feature-response
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
#' gbt_model <- iris_training %>%
#'   ml_gradient_boosted_trees(Sepal_Length ~ Petal_Length + Petal_Width)
#'
#' pred <- ml_predict(gbt_model, iris_test)
#'
#' ml_regression_evaluator(pred, label_col = "Sepal_Length")
#' }
#'
#' @export
ml_gradient_boosted_trees <- function(
  x,
  formula = NULL,
  type = c("auto", "regression", "classification"),
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  checkpoint_interval = 10,
  loss_type = c("auto", "logistic", "squared", "absolute"),
  max_bins = 32,
  max_depth = 5,
  max_iter = 20L,
  min_info_gain = 0,
  min_instances_per_node = 1,
  step_size = 0.1,
  subsampling_rate = 1,
  feature_subset_strategy = "auto",
  seed = NULL,
  thresholds = NULL,
  cache_node_ids = FALSE,
  max_memory_in_mb = 256,
  uid = random_string("gradient_boosted_trees_"),
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

  loss_type <- rlang::arg_match(loss_type)
  loss_type <- if (identical(loss_type, "auto")) {
    if (identical(model_type, "classification")) "logistic" else "squared"
  } else if (identical(model_type, "regression")) {
    if (!loss_type %in% c("squared", "absolute")) {
      stop("`loss_type` must be \"squared\" or \"absolute\" for regression.")
    }
    loss_type
  } else {
    if (!identical(loss_type, "logistic")) {
      stop("`loss_type` must be \"logistic\" for classification.")
    }
    loss_type
  }

  if (spark_version(spark_connection(x)) < "2.2.0" && !is.null(thresholds)) {
    stop("`thresholds` is only supported for GBT in Spark 2.2.0+.")
  }

  switch(
    model_type,
    regression = ml_gbt_regressor(
      x = x,
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
      checkpoint_interval = checkpoint_interval,
      cache_node_ids = cache_node_ids,
      max_memory_in_mb = max_memory_in_mb,
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      uid = uid,
      ...
    ),
    classification = ml_gbt_classifier(
      x = x,
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
  )
}

new_ml_model_gbt_classification <- function(
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
    class = "ml_model_gbt_classification"
  )
}

new_ml_model_gbt_regression <- function(
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
    class = "ml_model_gbt_regression"
  )
}
