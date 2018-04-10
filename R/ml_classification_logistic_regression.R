#' Spark ML -- Logistic Regression
#'
#' Perform classification using logistic regression.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-linear-regression-params
#' @template roxlate-ml-predictor-params
#' @template roxlate-ml-probabilistic-classifier-params
#' @param family (Spark 2.1.0+) Param for the name of family which is a description of the label distribution to be used in the model. Supported options: "auto", "binomial", and "multinomial."
#' @template roxlate-ml-elastic-net-param
#' @param threshold in binary classification prediction, in range [0, 1].
#' @template roxlate-ml-aggregation-depth
#' @param lower_bounds_on_coefficients (Spark 2.2.0+) Lower bounds on coefficients if fitting under bound constrained optimization.
#'   The bound matrix must be compatible with the shape (1, number of features) for binomial regression, or (number of classes, number of features) for multinomial regression.
#' @param lower_bounds_on_intercepts (Spark 2.2.0+) Lower bounds on intercepts if fitting under bound constrained optimization.
#'   The bounds vector size must be equal with 1 for binomial regression, or the number of classes for multinomial regression.
#' @param upper_bounds_on_coefficients (Spark 2.2.0+) Upper bounds on coefficients if fitting under bound constrained optimization.
#'   The bound matrix must be compatible with the shape (1, number of features) for binomial regression, or (number of classes, number of features) for multinomial regression.
#' @param upper_bounds_on_intercepts (Spark 2.2.0+) Upper bounds on intercepts if fitting under bound constrained optimization.
#'   The bounds vector size must be equal with 1 for binomial regression, or the number of classes for multinomial regression.
#' @export
ml_logistic_regression <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  tol = 1e-06,
  weight_col = NULL,
  aggregation_depth = 2L,
  lower_bounds_on_coefficients = NULL,
  lower_bounds_on_intercepts = NULL,
  upper_bounds_on_coefficients = NULL,
  upper_bounds_on_intercepts = NULL,
  features_col = "features",
  label_col = "label",
  family = "auto",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"),
  ...
) {
  UseMethod("ml_logistic_regression")
}

#' @export
ml_logistic_regression.spark_connection <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  tol = 1e-06,
  weight_col = NULL,
  aggregation_depth = 2L,
  lower_bounds_on_coefficients = NULL,
  lower_bounds_on_intercepts = NULL,
  upper_bounds_on_coefficients = NULL,
  upper_bounds_on_intercepts = NULL,
  features_col = "features",
  label_col = "label",
  family = "auto",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"),
  ...) {

  ml_ratify_args()

  jobj <- ml_new_classifier(
    x, "org.apache.spark.ml.classification.LogisticRegression", uid,
    features_col = features_col, label_col = label_col,
    prediction_col = prediction_col, probability_col = probability_col,
    raw_prediction_col = raw_prediction_col
  ) %>%
    invoke("setFitIntercept", fit_intercept) %>%
    invoke("setElasticNetParam", elastic_net_param) %>%
    invoke("setRegParam", reg_param) %>%
    invoke("setMaxIter", max_iter) %>%
    invoke("setThreshold", threshold) %>%
    invoke("setTol", tol) %>%
    jobj_set_param("setFamily", family, "auto", "2.1.0") %>%
    jobj_set_param("setAggregationDepth", aggregation_depth, 2L, "2.1.0")

  if (!rlang::is_null(thresholds))
    jobj <- invoke(jobj, "setThresholds", thresholds)
  if (!rlang::is_null(weight_col))
    jobj <- invoke(jobj, "setWeightCol", weight_col)

  if (!rlang::is_null(lower_bounds_on_coefficients)) {
    lower_bounds_on_coefficients <- spark_dense_matrix(x, lower_bounds_on_coefficients)
    jobj <- jobj_set_param(jobj, "setLowerBoundsOnCoefficients",
                           lower_bounds_on_coefficients,
                           NULL, "2.2.0")
  }

  if (!rlang::is_null(upper_bounds_on_coefficients)) {
    upper_bounds_on_coefficients <- spark_dense_matrix(x, upper_bounds_on_coefficients)
    jobj <- jobj_set_param(jobj, "setUpperBoundsOnCoefficients",
                           upper_bounds_on_coefficients,
                           NULL, "2.2.0")
  }

  if (!rlang::is_null(lower_bounds_on_intercepts)) {
    lower_bounds_on_intercepts <- spark_dense_vector(x, lower_bounds_on_intercepts)
    jobj <- jobj_set_param(jobj, "setLowerBoundsOnIntercepts",
                           lower_bounds_on_intercepts,
                           NULL, "2.2.0")
  }

  if (!rlang::is_null(upper_bounds_on_intercepts)) {
    upper_bounds_on_intercepts <- spark_dense_vector(x, upper_bounds_on_intercepts)
    jobj <- jobj_set_param(jobj, "setUpperBoundsOnIntercepts",
                           upper_bounds_on_intercepts,
                           NULL, "2.2.0")
  }


  new_ml_logistic_regression(jobj)
}

#' @export
ml_logistic_regression.ml_pipeline <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  tol = 1e-06,
  weight_col = NULL,
  aggregation_depth = 2L,
  lower_bounds_on_coefficients = NULL,
  lower_bounds_on_intercepts = NULL,
  upper_bounds_on_coefficients = NULL,
  upper_bounds_on_intercepts = NULL,
  features_col = "features",
  label_col = "label",
  family = "auto",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"),
  ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_logistic_regression.tbl_spark <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  threshold = 0.5,
  thresholds = NULL,
  tol = 1e-06,
  weight_col = NULL,
  aggregation_depth = 2L,
  lower_bounds_on_coefficients = NULL,
  lower_bounds_on_intercepts = NULL,
  upper_bounds_on_coefficients = NULL,
  upper_bounds_on_intercepts = NULL,
  features_col = "features",
  label_col = "label",
  family = "auto",
  prediction_col = "prediction",
  probability_col = "probability",
  raw_prediction_col = "rawPrediction",
  uid = random_string("logistic_regression_"),
  response = NULL,
  features = NULL,
  predicted_label_col = "predicted_label", ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(
      x, predictor = predictor, formula = formula,
      features_col = features_col, label_col = label_col,
      type = "classification",
      constructor = new_ml_model_logistic_regression,
      predicted_label_col)
  }
}

# Validator

ensure_matrix_double <- function(mat) {
  mat %>%
    sapply(ensure_scalar_double) %>%
    matrix(nrow = nrow(mat))
}

ml_validator_logistic_regression <- function(args, nms) {
  old_new_mapping <- list(
    intercept = "fit_intercept",
    alpha = "elastic_net_param",
    lambda = "reg_param",
    weights.column = "weight_col",
    iter.max = "max_iter",
    max.iter = "max_iter"
  )

  args %>%
    ml_validate_args({
      elastic_net_param <- ensure_scalar_double(elastic_net_param)
      reg_param <- ensure_scalar_double(reg_param)
      max_iter <- ensure_scalar_integer(max_iter)
      family <- rlang::arg_match(family, c("auto", "binomial", "multinomial"))
      fit_intercept <- ensure_scalar_boolean(fit_intercept)
      threshold <- ensure_scalar_double(threshold)
      if (!is.null(weight_col))
        weight_col <- ensure_scalar_character(weight_col)
      aggregation_depth <- ensure_scalar_integer(aggregation_depth)
      if (!is.null(lower_bounds_on_coefficients))
        lower_bounds_on_coefficients <- ensure_matrix_double(
          lower_bounds_on_coefficients)
      if (!is.null(upper_bounds_on_coefficients))
        upper_bounds_on_coefficients <- ensure_matrix_double(
          upper_bounds_on_coefficients)
      if (!is.null(lower_bounds_on_intercepts))
        lower_bounds_on_intercepts <- sapply(
          lower_bounds_on_intercepts, ensure_scalar_double)
      if (!is.null(upper_bounds_on_intercepts))
        upper_bounds_on_intercepts <- sapply(
          upper_bounds_on_intercepts, ensure_scalar_double)
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_logistic_regression <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_logistic_regression")
}

new_ml_logistic_regression_model <- function(jobj) {
  is_multinomial <- invoke(jobj, "numClasses") > 2

  summary <- if (invoke(jobj, "hasSummary")) {
    summary_jobj <- if (spark_version(spark_connection(jobj)) >= "2.3.0" &&
                        !is_multinomial)
      invoke(jobj, "binarySummary")
    else
      jobj
    new_ml_summary_logistic_regression_model(invoke(jobj, "summary"))
  }

  new_ml_prediction_model(
    jobj,
    coefficients = if (is_multinomial) NULL else read_spark_vector(jobj, "coefficients"),
    coefficient_matrix = try_null(read_spark_matrix(jobj, "coefficientMatrix")),
    intercept = if (is_multinomial) NULL else invoke(jobj, "intercept"),
    intercept_vector = try_null(read_spark_vector(jobj, "interceptVector")),
    num_classes = invoke(jobj, "numClasses"),
    num_features = invoke(jobj, "numFeatures"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    probability_col = invoke(jobj, "getProbabilityCol"),
    raw_prediction_col = invoke(jobj, "getRawPredictionCol"),
    threshold = if (ml_is_set(jobj, "threshold")) invoke(jobj, "getThreshold") else NULL,
    thresholds = if (ml_is_set(jobj, "thresholds")) invoke(jobj, "getThresholds") else NULL,
    summary = summary,
    subclass = "ml_logistic_regression_model")
}

new_ml_summary_logistic_regression_model <- function(jobj) {
  new_ml_summary(
    jobj,
    area_under_roc = try_null(invoke(jobj, "areaUnderROC")),
    f_measure_by_threshold = try_null(invoke(jobj, "fMeasureByThreshold") %>%
      invoke("withColumnRenamed", "F-Measure", "F_Measure") %>%
      collect()),
    false_positive_rate_by_label = try_null(invoke(jobj, "falsePositiveRateByLabel")),
    precision_by_label = try_null(invoke(jobj, "precisionByLabel")),
    recall_by_label = try_null(invoke(jobj, "recallByLabel")),
    true_positive_rate_by_label = try_null(invoke(jobj, "truePositiveRateByLabel")),
    weighted_f_measure = try_null(invoke(jobj, "weightedFMeasure")),
    weighted_false_positive_rate = try_null(invoke(jobj, "weightedFalsePositiveRate")),
    weighted_precision = try_null(invoke(jobj, "weightedPrecision")),
    weighted_recall = try_null(invoke(jobj, "weightedRecall")),
    weighted_true_positive_rate = try_null(invoke(jobj, "weightedTruePositiveRate")),
    features_col = invoke(jobj, "featuresCol"),
    label_col = invoke(jobj, "labelCol"),
    objective_history = invoke(jobj, "objectiveHistory"),
    pr = try_null(invoke(jobj, "pr") %>% collect()),
    precision_by_threshold = try_null(invoke(jobj, "precisionByThreshold") %>% collect()),
    predictions = invoke(jobj, "predictions") %>% sdf_register(),
    probability_col = invoke(jobj, "probabilityCol"),
    recall_by_threshold = try_null(invoke(jobj, "recallByThreshold") %>% collect()),
    roc = try_null(invoke(jobj, "roc") %>% collect()),
    total_iterations = invoke(jobj, "totalIterations"),
    subclass = "ml_summary_logistic_regression")
}

new_ml_model_logistic_regression <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names, index_labels,
  call) {

  jobj <- spark_jobj(model)
  sc <- spark_connection(model)

  # multinomial vs. binomial models have separate APIs for
  # retrieving results
  is_multinomial <- invoke(jobj, "numClasses") > 2

  # extract coefficients (can be either a vector or matrix, depending
  # on binomial vs. multinomial)
  coefficients <- if (is_multinomial) {
    if (spark_version(sc) < "2.1.0") stop("Multinomial regression requires Spark 2.1.0 or higher.")

    # multinomial
    coefficients <- model$coefficient_matrix
    colnames(coefficients) <- feature_names
    rownames(coefficients) <- index_labels

    if (ml_param(model, "fit_intercept")) {
      intercept <- model$intercept_vector
      coefficients <- cbind(intercept, coefficients)
      colnames(coefficients) <- c("(Intercept)", feature_names)
    }
    coefficients
  } else {
    # binomial

    coefficients <- if (ml_param(model, "fit_intercept"))
      rlang::set_names(
        c(invoke(jobj, "intercept"), model$coefficients),
        c("(Intercept)", feature_names)
      )
    else
      rlang::set_names(coefficients, feature_names)
    coefficients
  }

  summary <- model$summary

  new_ml_model_classification(
    pipeline, pipeline_model,
    model, dataset, formula,
    coefficients = coefficients,
    summary = summary,
    subclass = "ml_model_logistic_regression",
    .features = feature_names,
    .index_labels = index_labels
  )
}


# Generic implementations

#' @export
print.ml_model_logistic_regression <- function(x, ...) {
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}

#' @export
summary.ml_model_logistic_regression <- function(object, ...) {
  ml_model_print_coefficients(object)
  print_newline()
}
