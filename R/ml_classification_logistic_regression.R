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
#'
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' mtcars_tbl <- sdf_copy_to(sc, mtcars, name = "mtcars_tbl", overwrite = TRUE)
#'
#' partitions <- mtcars_tbl %>%
#'   sdf_partition(training = 0.7, test = 0.3, seed = 1111)
#'
#' mtcars_training <- partitions$training
#' mtcars_test <- partitions$test
#'
#' lr_model <- mtcars_training %>%
#'   ml_logistic_regression(am ~ gear + carb)
#'
#' pred <- sdf_predict(mtcars_test, lr_model)
#'
#' ml_binary_classification_evaluator(pred)
#' }
#'
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

  .args <- c(as.list(environment()), list(...)) %>%
    ml_validator_logistic_regression()

  jobj <- ml_new_classifier(
    x, "org.apache.spark.ml.classification.LogisticRegression", .args$uid,
    features_col = .args$features_col, label_col = .args$label_col,
    prediction_col = .args$prediction_col, probability_col = .args$probability_col,
    raw_prediction_col = .args$raw_prediction_col
  ) %>%
    invoke("setFitIntercept", .args$fit_intercept) %>%
    invoke("setElasticNetParam", .args$elastic_net_param) %>%
    invoke("setRegParam", .args$reg_param) %>%
    invoke("setMaxIter", .args$max_iter) %>%
    invoke("setThreshold", .args$threshold) %>%
    invoke("setTol", .args$tol) %>%
    jobj_set_param("setFamily", .args$family, "auto", "2.1.0") %>%
    jobj_set_param("setAggregationDepth", .args$aggregation_depth, 2L, "2.1.0")

  if (!is.null(.args$thresholds))
    jobj <- invoke(jobj, "setThresholds", .args$thresholds)
  if (!is.null(.args$weight_col))
    jobj <- invoke(jobj, "setWeightCol", .args$weight_col)

  if (!is.null(.args$lower_bounds_on_coefficients)) {
    lower_bounds_on_coefficients <- spark_dense_matrix(x, .args$lower_bounds_on_coefficients)
    jobj <- jobj_set_param(jobj, "setLowerBoundsOnCoefficients",
                           lower_bounds_on_coefficients,
                           NULL, "2.2.0")
  }

  if (!is.null(.args$upper_bounds_on_coefficients)) {
    upper_bounds_on_coefficients <- spark_dense_matrix(x, .args$upper_bounds_on_coefficients)
    jobj <- jobj_set_param(jobj, "setUpperBoundsOnCoefficients",
                           upper_bounds_on_coefficients,
                           NULL, "2.2.0")
  }

  if (!is.null(.args$lower_bounds_on_intercepts)) {
    lower_bounds_on_intercepts <- spark_dense_vector(x, .args$lower_bounds_on_intercepts)
    jobj <- jobj_set_param(jobj, "setLowerBoundsOnIntercepts",
                           lower_bounds_on_intercepts,
                           NULL, "2.2.0")
  }

  if (!is.null(.args$upper_bounds_on_intercepts)) {
    upper_bounds_on_intercepts <- spark_dense_vector(x, .args$upper_bounds_on_intercepts)
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

  transformer <- ml_logistic_regression.spark_connection(
    x = spark_connection(x),
    formula = formula,
    fit_intercept = fit_intercept,
    elastic_net_param = elastic_net_param,
    reg_param = reg_param,
    max_iter = max_iter,
    threshold = threshold,
    thresholds = thresholds,
    tol = tol,
    weight_col = weight_col,
    aggregation_depth = aggregation_depth,
    lower_bounds_on_coefficients = lower_bounds_on_coefficients,
    lower_bounds_on_intercepts = lower_bounds_on_intercepts,
    upper_bounds_on_coefficients = upper_bounds_on_coefficients,
    upper_bounds_on_intercepts = upper_bounds_on_intercepts,
    features_col = features_col,
    label_col = label_col,
    family = family,
    prediction_col = prediction_col,
    probability_col = probability_col,
    raw_prediction_col = raw_prediction_col,
    uid = uid,
    ...
  )

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

  predictor <- ml_logistic_regression.spark_connection(
    x = spark_connection(x),
    formula = formula,
    fit_intercept = fit_intercept,
    elastic_net_param = elastic_net_param,
    reg_param = reg_param,
    max_iter = max_iter,
    threshold = threshold,
    thresholds = thresholds,
    tol = tol,
    weight_col = weight_col,
    aggregation_depth = aggregation_depth,
    lower_bounds_on_coefficients = lower_bounds_on_coefficients,
    lower_bounds_on_intercepts = lower_bounds_on_intercepts,
    upper_bounds_on_coefficients = upper_bounds_on_coefficients,
    upper_bounds_on_intercepts = upper_bounds_on_intercepts,
    features_col = features_col,
    label_col = label_col,
    family = family,
    prediction_col = prediction_col,
    probability_col = probability_col,
    raw_prediction_col = raw_prediction_col,
    uid = uid,
    ...
  )

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
    purrr::map_dbl(camp::mold_scalar_double) %>%
    matrix(nrow = nrow(mat))
}

ml_validator_logistic_regression <- function(.args) {
  .args <- ml_backwards_compatibility(
    .args, list(
    intercept = "fit_intercept",
    alpha = "elastic_net_param",
    lambda = "reg_param",
    weights.column = "weight_col",
    iter.max = "max_iter",
    max.iter = "max_iter"
  )) %>%
    validate_args_classifier()

  .args[["elastic_net_param"]] <- camp::mold_scalar_double(.args[["elastic_net_param"]])
  .args[["reg_param"]] <- camp::mold_scalar_double(.args[["reg_param"]])
  .args[["max_iter"]] <- camp::mold_scalar_integer(.args[["max_iter"]])
  .args[["family"]] <- ensure_valid_option(.args[["family"]], c("auto", "binomial", "multinomial"))
  .args[["fit_intercept"]] <- ensure_scalar_boolean(.args[["fit_intercept"]])
  .args[["threshold"]] <- camp::mold_scalar_double(.args[["threshold"]])
  if (!is.null(.args[["weight_col"]]))
    .args[["weight_col"]] <- ensure_scalar_character(.args[["weight_col"]])
  .args[["aggregation_depth"]] <- camp::mold_scalar_integer(.args[["aggregation_depth"]])
  if (!is.null(.args[["lower_bounds_on_coefficients"]]))
    .args[["lower_bounds_on_coefficients"]] <- ensure_matrix_double(
      .args[["lower_bounds_on_coefficients"]])
  if (!is.null(.args[["upper_bounds_on_coefficients"]]))
    .args[["upper_bounds_on_coefficients"]] <- ensure_matrix_double(
      .args[["upper_bounds_on_coefficients"]])
  if (!is.null(.args[["lower_bounds_on_intercepts"]]))
    .args[["lower_bounds_on_intercepts"]] <- purrr::map_dbl(
      .args[["lower_bounds_on_intercepts"]], camp::mold_scalar_double)
  if (!is.null(.args[["upper_bounds_on_intercepts"]]))
    .args[["upper_bounds_on_intercepts"]] <- purrr::map_dbl(
      .args[["upper_bounds_on_intercepts"]], camp::mold_scalar_double)
  .args
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
