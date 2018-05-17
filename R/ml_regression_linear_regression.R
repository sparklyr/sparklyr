#' Spark ML -- Linear Regression
#'
#' Perform regression using linear regression.
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-linear-regression-params
#' @template roxlate-ml-predictor-params
#' @template roxlate-ml-elastic-net-param
#' @template roxlate-ml-standardization
#' @param loss The loss function to be optimized. Supported options: "squaredError"
#'    and "huber". Default: "squaredError"
#' @param solver Solver algorithm for optimization.
#' @export
ml_linear_regression <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  weight_col = NULL,
  loss = "squaredError",
  solver = "auto",
  standardization = TRUE,
  tol = 1e-6,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("linear_regression_"), ...
) {
  UseMethod("ml_linear_regression")
}

#' @export
ml_linear_regression.spark_connection <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  weight_col = NULL,
  loss = "squaredError",
  solver = "auto",
  standardization = TRUE,
  tol = 1e-6,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("linear_regression_"), ...) {

  ml_ratify_args()

  jobj <- ml_new_regressor(
    x, "org.apache.spark.ml.regression.LinearRegression", uid,
    features_col, label_col, prediction_col
  ) %>%
    invoke("setElasticNetParam", elastic_net_param) %>%
    invoke("setFitIntercept", fit_intercept) %>%
    invoke("setRegParam", reg_param) %>%
    invoke("setMaxIter", max_iter) %>%
    invoke("setSolver", solver) %>%
    invoke("setStandardization", standardization) %>%
    invoke("setTol", tol) %>%
    jobj_set_param("setLoss", loss,
                   "squaredError", "2.3.0")

  if (!is.null(weight_col))
    jobj <- invoke(jobj, "setWeightCol", weight_col)

  new_ml_linear_regression(jobj)
}

#' @export
ml_linear_regression.ml_pipeline <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  weight_col = NULL,
  loss = "squaredError",
  solver = "auto",
  standardization = TRUE,
  tol = 1e-6,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("linear_regression_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_linear_regression.tbl_spark <- function(
  x,
  formula = NULL,
  fit_intercept = TRUE,
  elastic_net_param = 0,
  reg_param = 0,
  max_iter = 100L,
  weight_col = NULL,
  loss = "squaredError",
  solver = "auto",
  standardization = TRUE,
  tol = 1e-6,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("linear_regression_"),
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
      "regression", new_ml_model_linear_regression
    )
  }
}

# Validator
ml_validator_linear_regression <- function(args, nms) {
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
      fit_intercept <- ensure_scalar_boolean(fit_intercept)
      standardization <- ensure_scalar_boolean(standardization)
      tol <- ensure_scalar_double(tol)
      solver <- rlang::arg_match(solver, c("auto", "l-bfgs", "normal"))
      if (!is.null(weight_col))
        weight_col <- ensure_scalar_character(weight_col)
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_linear_regression <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_linear_regression")
}

new_ml_linear_regression_model <- function(jobj) {
  summary <- if (invoke(jobj, "hasSummary")) {
    fit_intercept <- ml_get_param_map(jobj)$fit_intercept
    new_ml_summary_linear_regression_model(invoke(jobj, "summary"), fit_intercept)
  } else NULL

  new_ml_prediction_model(
    jobj,
    coefficients = read_spark_vector(jobj, "coefficients"),
    intercept = invoke(jobj, "intercept"),
    num_features = invoke(jobj, "numFeatures"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    scale = if (spark_version(spark_connection(jobj)) >= "2.3.0") invoke(jobj, "scale"),
    summary = summary,
    subclass = "ml_linear_regression_model")
}

new_ml_summary_linear_regression_model <- function(
  jobj, fit_intercept) {
  arrange_stats <- make_stats_arranger(fit_intercept)

  new_ml_summary(
    jobj,
    coefficient_standard_errors = try_null(invoke(jobj, "coefficientStandardErrors")) %>%
      arrange_stats(),
    degrees_of_freedom = if (spark_version(spark_connection(jobj)) >= "2.2.0")
      invoke(jobj, "degreesOfFreedom") else NULL,
    deviance_residuals = invoke(jobj, "devianceResiduals"),
    explained_variance = invoke(jobj, "explainedVariance"),
    features_col = invoke(jobj, "featuresCol"),
    label_col = invoke(jobj, "labelCol"),
    mean_absolute_error = invoke(jobj, "meanAbsoluteError"),
    mean_squared_error = invoke(jobj, "meanSquaredError"),
    num_instances = invoke(jobj, "numInstances"),
    p_values = try_null(invoke(jobj, "pValues")) %>%
      arrange_stats(),
    prediction_col = invoke(jobj, "predictionCol"),
    predictions = invoke(jobj, "predictions") %>% sdf_register(),
    r2 = invoke(jobj, "r2"),
    residuals = invoke(jobj, "residuals") %>% sdf_register(),
    root_mean_squared_error = invoke(jobj, "rootMeanSquaredError"),
    t_values = try_null(invoke(jobj, "tValues")) %>%
      arrange_stats(),
    subclass = "ml_summary_linear_regression")
}

new_ml_model_linear_regression <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names, call) {

  jobj <- spark_jobj(model)
  sc <- spark_connection(model)


  coefficients <- model$coefficients
  names(coefficients) <- feature_names

  coefficients <- if (ml_param(model, "fit_intercept"))
    rlang::set_names(
      c(invoke(jobj, "intercept"), model$coefficients),
      c("(Intercept)", feature_names))

  summary <- model$summary

  new_ml_model_regression(
    pipeline, pipeline_model, model, dataset, formula,
    coefficients = coefficients,
    summary = summary,
    subclass = "ml_model_linear_regression",
    .features = feature_names
  )
}

# Generic implementations

#' @export
print.ml_model_linear_regression <- function(x, ...) {
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}

#' @export
summary.ml_model_linear_regression <- function(object, ...) {
  ml_model_print_residuals(object, residuals.header = "Deviance Residuals")
  print_newline()
  ml_model_print_coefficients_detailed(object)
  print_newline()

  cat(paste("R-Squared:", signif(object$summary$r2, 4)), sep = "\n")
  cat(paste("Root Mean Squared Error:",
            signif(object$summary$root_mean_squared_error, 4)), sep = "\n")
}

#' @export
residuals.ml_model_linear_regression <- function(object, ...) {

  residuals <- object$summary$residuals

  sdf_read_column(residuals, "residuals")

}

#' @export
#' @rdname sdf_residuals
sdf_residuals.ml_model_linear_regression <- function(
  object, ...) {

  residuals <- object$summary$residuals

  ml_model_data(object) %>%
    sdf_fast_bind_cols(residuals)
}

