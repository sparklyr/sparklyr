#' Spark ML -- Survival Regression
#'
#' Fit a parametric survival regression model named accelerated failure time (AFT) model (see \href{https://en.wikipedia.org/wiki/Accelerated_failure_time_model}{Accelerated failure time model (Wikipedia)}) based on the Weibull distribution of the survival time.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-max-iter
#' @template roxlate-ml-tol
#' @template roxlate-ml-intercept
#' @template roxlate-ml-predictor-params
#' @template roxlate-ml-aggregation-depth
#' @param censor_col Censor column name. The value of this column could be 0 or 1. If the value is 1, it means the event has occurred i.e. uncensored; otherwise censored.
#' @param quantile_probabilities Quantile probabilities array. Values of the quantile probabilities array should be in the range (0, 1) and the array should be non-empty.
#' @param quantiles_col Quantiles column name. This column will output quantiles of corresponding quantileProbabilities if it is set.
#' @export
ml_aft_survival_regression <- function(
  x,
  formula = NULL,
  censor_col = "censor",
  quantile_probabilities = list(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99),
  fit_intercept = TRUE,
  max_iter = 100L,
  tol = 1e-06,
  aggregation_depth = 2L,
  quantiles_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("aft_survival_regression_"), ...
) {
  UseMethod("ml_aft_survival_regression")
}

#' @export
ml_aft_survival_regression.spark_connection <- function(
  x,
  formula = NULL,
  censor_col = "censor",
  quantile_probabilities = list(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99),
  fit_intercept = TRUE,
  max_iter = 100L,
  tol = 1e-06,
  aggregation_depth = 2L,
  quantiles_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("aft_survival_regression_"), ...) {

  ml_ratify_args()

  jobj <- ml_new_regressor(
    x, "org.apache.spark.ml.regression.AFTSurvivalRegression", uid,
    features_col, label_col, prediction_col
  ) %>%
    invoke("setFitIntercept", fit_intercept) %>%
    invoke("setMaxIter", max_iter) %>%
    invoke("setTol", tol) %>%
    invoke("setCensorCol", censor_col) %>%
    invoke("setFitIntercept", fit_intercept) %>%
    invoke("setQuantileProbabilities", quantile_probabilities) %>%
    jobj_set_param("setAggregationDepth", aggregation_depth, 2L, "2.1.0")

  if (!rlang::is_null(quantiles_col))
    jobj <- invoke(jobj, "setQuantilesCol", quantiles_col)

  new_ml_aft_survival_regression(jobj)
}

#' @export
ml_aft_survival_regression.ml_pipeline <- function(
  x,
  formula = NULL,
  censor_col = "censor",
  quantile_probabilities = list(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99),
  fit_intercept = TRUE,
  max_iter = 100L,
  tol = 1e-06,
  aggregation_depth = 2L,
  quantiles_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("aft_survival_regression_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_aft_survival_regression.tbl_spark <- function(
  x,
  formula = NULL,
  censor_col = "censor",
  quantile_probabilities = list(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99),
  fit_intercept = TRUE,
  max_iter = 100L,
  tol = 1e-06,
  aggregation_depth = 2L,
  quantiles_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("aft_survival_regression_"),
  response = NULL,
  features = NULL,...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(
      x, predictor, formula, features_col, label_col,
      "regression", new_ml_model_aft_survival_regression
    )
  }
}

# Validator
ml_validator_aft_survival_regression <- function(args, nms) {
  old_new_mapping <- list(
    intercept = "fit_intercept",
    iter.max = "max_iter",
    max.iter = "max_iter"
  )

  args %>%
    ml_validate_args({
      max_iter <- ensure_scalar_integer(max_iter)
      fit_intercept <- ensure_scalar_boolean(fit_intercept)
      tol <- ensure_scalar_double(tol)
      censor_col <- ensure_scalar_character(censor_col)
      quantile_probabilities <- quantile_probabilities %>%
        lapply(ensure_scalar_double)
      max_iter <- ensure_scalar_integer(max_iter)
      aggregation_depth <- ensure_scalar_integer(aggregation_depth)
      if (!rlang::is_null(quantiles_col))
        quantiles_col <- ensure_scalar_character(quantiles_col)
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_aft_survival_regression <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_aft_survival_regression")
}

new_ml_aft_survival_regression_model <- function(jobj) {
  new_ml_prediction_model(
    jobj,
    coefficients = read_spark_vector(jobj, "coefficients"),
    intercept = try_null(invoke(jobj, "intercept")),
    scale = invoke(jobj, "scale"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    quantile_probabilities = invoke(jobj, "getQuantileProbabilities"),
    quantiles_col = try_null(invoke(jobj, "getQuantilesCol")),
    subclass = "ml_aft_survival_regression_model")
}

new_ml_model_aft_survival_regression <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names, call) {

  jobj <- spark_jobj(model)
  sc <- spark_connection(model)


  coefficients <- model$coefficients
  names(coefficients) <- feature_names

  coefficients <- if (ml_param(model, "fit_intercept"))
    rlang::set_names(
      c(invoke(jobj, "intercept"), model$coefficients),
      c("(Intercept)", feature_names))

  new_ml_model_regression(
    pipeline, pipeline_model, model, dataset, formula,
    coefficients = coefficients,
    subclass = "ml_model_aft_survival_regression",
    .features = feature_names,
    .call = call
  )
}


#' @rdname ml_aft_survival_regression
#' @template roxlate-ml-old-feature-response
#' @details \code{ml_survival_regression()} is an alias for \code{ml_aft_survival_regression()} for backwards compatibility.
#' @export
ml_survival_regression <- function(
  x,
  formula = NULL,
  censor_col = "censor",
  quantile_probabilities = list(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99),
  fit_intercept = TRUE,
  max_iter = 100L,
  tol = 1e-06,
  aggregation_depth = 2L,
  quantiles_col = NULL,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("aft_survival_regression_"),
  response = NULL,
  features = NULL,...) {
  UseMethod("ml_aft_survival_regression")
}

# Generic implementations

#' @export
print.ml_model_aft_survival_regression <- function(x, ...) {
  ml_model_print_call(x)
  print_newline()
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}
