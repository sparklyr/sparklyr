#' Spark ML -- LinearSVC
#'
#' Perform classification using linear support vector machines (SVM). This binary classifier optimizes the Hinge Loss using the OWLQN optimizer. Only supports L2 regularization currently.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-linear-regression-params
#' @template roxlate-ml-predictor-params
#' @template roxlate-ml-aggregation-depth
#' @template roxlate-ml-standardization
#' @param threshold in binary classification prediction, in range [0, 1].
#' @param raw_prediction_col Raw prediction (a.k.a. confidence) column name.
#' @export
ml_linear_svc <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  reg_param = 0,
  max_iter = 100L,
  standardization = TRUE,
  weight_col = NULL,
  tol = 1e-6,
  threshold = 0,
  aggregation_depth = 2L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  raw_prediction_col = "rawPrediction",
  uid = random_string("linear_svc_"), ...
) {
  UseMethod("ml_linear_svc")
}

#' @export
ml_linear_svc.spark_connection <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  reg_param = 0,
  max_iter = 100L,
  standardization = TRUE,
  weight_col = NULL,
  tol = 1e-6,
  threshold = 0,
  aggregation_depth = 2L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  raw_prediction_col = "rawPrediction",
  uid = random_string("linear_svc_"), ...) {

  ml_ratify_args()

  jobj <- ml_new_predictor(
    x, "org.apache.spark.ml.classification.LinearSVC", uid,
    features_col, label_col, prediction_col
  ) %>%
    invoke("setRawPredictionCol", raw_prediction_col) %>%
    invoke("setFitIntercept", fit_intercept) %>%
    invoke("setRegParam", reg_param) %>%
    invoke("setMaxIter", max_iter) %>%
    invoke("setStandardization", standardization) %>%
    invoke("setTol", tol) %>%
    invoke("setAggregationDepth", aggregation_depth) %>%
    invoke("setThreshold", threshold)

  if (!rlang::is_null(weight_col))
    jobj <- invoke(jobj, "setWeightCol", weight_col)

  new_ml_linear_svc(jobj)
}

#' @export
ml_linear_svc.ml_pipeline <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  reg_param = 0,
  max_iter = 100L,
  standardization = TRUE,
  weight_col = NULL,
  tol = 1e-6,
  threshold = 0,
  aggregation_depth = 2L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  raw_prediction_col = "rawPrediction",
  uid = random_string("linear_svc_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_linear_svc.tbl_spark <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  reg_param = 0,
  max_iter = 100L,
  standardization = TRUE,
  weight_col = NULL,
  tol = 1e-6,
  threshold = 0,
  aggregation_depth = 2L,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  raw_prediction_col = "rawPrediction",
  uid = random_string("linear_svc_"),
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
      x, predictor, formula, features_col, label_col,
      "classification", new_ml_model_linear_svc, predicted_label_col
    )
  }
}

# Validator
ml_validator_linear_svc <- function(args, nms) {
  args %>%
    ml_validate_args({
      reg_param <- ensure_scalar_double(reg_param)
      max_iter <- ensure_scalar_integer(max_iter)
      fit_intercept <- ensure_scalar_boolean(fit_intercept)
      standardization <- ensure_scalar_boolean(standardization)
      tol <- ensure_scalar_double(tol)
      aggregation_depth <- ensure_scalar_integer(aggregation_depth)
      raw_prediction_col <- ensure_scalar_character(raw_prediction_col)
      threshold <- ensure_scalar_double(threshold)
      if (!is.null(weight_col))
        weight_col <- ensure_scalar_character(weight_col)
    }) %>%
    ml_extract_args(nms)
}

# Constructors

new_ml_linear_svc <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_linear_svc")
}

new_ml_linear_svc_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    coefficients = read_spark_vector(jobj, "coefficients"),
    intercept = invoke(jobj, "intercept"),
    num_classes = invoke(jobj, "numClasses"),
    num_features = invoke(jobj, "numFeatures"),
    threshold = invoke(jobj, "threshold"),
    weight_col = try_null(invoke(jobj, "weightCol")),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    raw_prediction_col = invoke(jobj, "getRawPredictionCol"),
    subclass = "ml_linear_svc_model")
}

new_ml_model_linear_svc <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names,
  index_labels, call) {

  jobj <- spark_jobj(model)
  sc <- spark_connection(model)

  coefficients <- model$coefficients
  names(coefficients) <- feature_names

  coefficients <- if (ml_param(model, "fit_intercept"))
    rlang::set_names(
      c(invoke(jobj, "intercept"), model$coefficients),
      c("(Intercept)", feature_names))

  new_ml_model_classification(
    pipeline, pipeline_model, model, dataset, formula,
    coefficients = coefficients,
    subclass = "ml_model_linear_svc",
    .features = feature_names,
    .index_labels = index_labels
  )
}

# Generic implementations

#' @export
print.ml_model_linear_svc <- function(x, ...) {
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}
