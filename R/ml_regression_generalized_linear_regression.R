#' Spark ML -- Generalized Linear Regression
#'
#' Perform regression using Generalized Linear Model (GLM).
#'
#' @template roxlate-ml-algo
#' @template roxlate-ml-formula-params
#' @template roxlate-ml-linear-regression-params
#' @template roxlate-ml-predictor-params
#' @param offset_col Offset column name. If this is not set, we treat all instance offsets as 0.0. The feature specified as offset has a constant coefficient of 1.0.
#' @param family Name of family which is a description of the error distribution to be used in the model. Supported options: "gaussian", "binomial", "poisson", "gamma" and "tweedie". Default is "gaussian".
#' @param link Name of link function which provides the relationship between the linear predictor and the mean of the distribution function. See for supported link functions.
#' @param link_power Index in the power link function. Only applicable to the Tweedie family. Note that link power 0, 1, -1 or 0.5 corresponds to the Log, Identity, Inverse or Sqrt link, respectively. When not set, this value defaults to 1 - variancePower, which matches the R "statmod" package.
#' @param link_prediction_col Link prediction (linear predictor) column name. Default is not set, which means we do not output link prediction.
#' @param solver Solver algorithm for optimization.
#' @param variance_power Power in the variance function of the Tweedie distribution which provides the relationship between the variance and mean of the distribution. Only applicable to the Tweedie family. (see \href{https://en.wikipedia.org/wiki/Tweedie_distribution}{Tweedie Distribution (Wikipedia)}) Supported values: 0 and [1, Inf). Note that variance power 0, 1, or 2 corresponds to the Gaussian, Poisson or Gamma family, respectively.
#'
#' @details Valid link functions for each family is listed below. The first link function of each family is the default one.
#'   \itemize{
#'     \item gaussian: "identity", "log", "inverse"
#'     \item binomial: "logit", "probit", "loglog"
#'     \item poisson: "log", "identity", "sqrt"
#'     \item gamma: "inverse", "identity", "log"
#'     \item tweedie: power link function specified through \code{link_power}. The default link power in the tweedie family is \code{1 - variance_power}.
#'     }
#' @export
ml_generalized_linear_regression <- function(
  x,
  formula = NULL,
  family = "gaussian",
  link = NULL,
  fit_intercept = TRUE,
  offset_col = NULL,
  link_power = NULL,
  link_prediction_col = NULL,
  reg_param = 0,
  max_iter = 25L,
  weight_col = NULL,
  solver = "irls",
  tol = 1e-6,
  variance_power = 0,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("generalized_linear_regression_"), ...
) {
  spark_require_version(spark_connection(x), "2.0.0")
  UseMethod("ml_generalized_linear_regression")
}

#' @export
ml_generalized_linear_regression.spark_connection <- function(
  x,
  formula = NULL,
  family = "gaussian",
  link = NULL,
  fit_intercept = TRUE,
  offset_col = NULL,
  link_power = NULL,
  link_prediction_col = NULL,
  reg_param = 0,
  max_iter = 25L,
  weight_col = NULL,
  solver = "irls",
  tol = 1e-6,
  variance_power = 0,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("generalized_linear_regression_"), ...) {

  ml_ratify_args()

  jobj <- ml_new_regressor(
    x, "org.apache.spark.ml.regression.GeneralizedLinearRegression", uid,
    features_col, label_col, prediction_col
  ) %>%
    invoke("setFamily", family) %>%
    invoke("setFitIntercept", fit_intercept) %>%
    invoke("setRegParam", reg_param) %>%
    invoke("setMaxIter", max_iter) %>%
    invoke("setSolver", solver) %>%
    invoke("setTol", tol)

  if (identical(family, "tweedie")) {
    jobj <- jobj %>%
      invoke("setLinkPower", link_power) %>%
      invoke("setVariancePower", variance_power)
  }

  if (!rlang::is_null(link))
    jobj <- invoke(jobj, "setLink", link)

  if (!rlang::is_null(link_prediction_col))
    jobj <- invoke(jobj, "setLinkPredictionCol", link_prediction_col)

  if (!rlang::is_null(weight_col))
    jobj <- invoke(jobj, "setWeightCol", weight_col)

  if (!rlang::is_null(offset_col))
    jobj <- jobj_set_param(jobj, "setOffsetCol", offset_col, NULL, "2.3.0")

  new_ml_generalized_linear_regression(jobj)
}

#' @export
ml_generalized_linear_regression.ml_pipeline <- function(
  x,
  formula = NULL,
  family = "gaussian",
  link = NULL,
  fit_intercept = TRUE,
  offset_col = NULL,
  link_power = NULL,
  link_prediction_col = NULL,
  reg_param = 0,
  max_iter = 25L,
  weight_col = NULL,
  solver = "irls",
  tol = 1e-6,
  variance_power = 0,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("generalized_linear_regression_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_generalized_linear_regression.tbl_spark <- function(
  x,
  formula = NULL,
  family = "gaussian",
  link = NULL,
  fit_intercept = TRUE,
  offset_col = NULL,
  link_power = NULL,
  link_prediction_col = NULL,
  reg_param = 0,
  max_iter = 25L,
  weight_col = NULL,
  solver = "irls",
  tol = 1e-6,
  variance_power = 0,
  features_col = "features",
  label_col = "label",
  prediction_col = "prediction",
  uid = random_string("generalized_linear_regression_"),
  response = NULL,
  features = NULL, ...) {

  predictor <- ml_new_stage_modified_args()

  ml_formula_transformation()

  if (is.null(formula)) {
    predictor %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(
      x, predictor, formula, features_col, label_col,
      "regression", new_ml_model_generalized_linear_regression
    )
  }
}

# Validator
ml_validator_generalized_linear_regression <- function(args, nms) {
  old_new_mapping <- list(
    intercept = "fit_intercept",
    weights.column = "weight_col",
    iter.max = "max_iter",
    max.iter = "max_iter"
  )

  args %>%
    ml_validate_args({
      reg_param <- ensure_scalar_double(reg_param)
      max_iter <- ensure_scalar_integer(max_iter)
      if (is.function(family)) {
        family <- family()
        link <- ensure_scalar_character(family$link)
        family <- ensure_scalar_character(family$family)
      } else if (rlang::is_character(family)) {
        family <- rlang::arg_match(family, c(
          "gaussian", "binomial", "poisson", "gamma", "tweedie"))
        link <- ensure_scalar_character(link, allow.null = TRUE)
      } else {
        link <- ensure_scalar_character(family$link)
        family <- ensure_scalar_character(family$family)
      }

      fit_intercept <- ensure_scalar_boolean(fit_intercept)
      solver <- rlang::arg_match(solver, c("irls"))
      tol <- ensure_scalar_double(tol)
      if (!rlang::is_null(offset_col))
        offset_col <- ensure_scalar_character(offset_col)
      if (!rlang::is_null(weight_col))
        weight_col <- ensure_scalar_character(weight_col)
      if (!rlang::is_null(link_prediction_col))
        link_prediction_col <- ensure_scalar_character(link_prediction_col)
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_generalized_linear_regression <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_generalized_linear_regression")
}

new_ml_generalized_linear_regression_model <- function(jobj) {
  summary <- if (invoke(jobj, "hasSummary"))
  {
    fit_intercept <- ml_get_param_map(jobj)$fit_intercept
    new_ml_summary_generalized_linear_regression_model(
      invoke(jobj, "summary"), fit_intercept
      )
  } else NA

  new_ml_prediction_model(
    jobj,
    coefficients = read_spark_vector(jobj, "coefficients"),
    intercept = invoke(jobj, "intercept"),
    num_features = invoke(jobj, "numFeatures"),
    features_col = invoke(jobj, "getFeaturesCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    link_prediction_col = if (invoke(jobj, "isSet", invoke(jobj, "linkPredictionCol"))) invoke(jobj, "getLinkPredictionCol") else NULL,
    summary = summary,
    subclass = "ml_generalized_linear_regression_model")
}

new_ml_summary_generalized_linear_regression_model <- function(jobj, fit_intercept) {
  version <- jobj %>%
    spark_connection() %>%
    spark_version()
  resid <- function(x) invoke(jobj, "residuals", x) %>%
    sdf_register()

  arrange_stats <- make_stats_arranger(fit_intercept)

  new_ml_summary(
    jobj,
    aic = invoke(jobj, "aic"),
    coefficient_standard_errors = try_null(invoke(jobj, "coefficientStandardErrors")) %>%
      arrange_stats(),
    degrees_of_freedom = invoke(jobj, "degreesOfFreedom"),
    deviance = invoke(jobj, "deviance"),
    dispersion = invoke(jobj, "dispersion"),
    null_deviance = invoke(jobj, "nullDeviance"),
    num_instances = if (version > "2.2.0") invoke(jobj, "numInstances") else NULL,
    num_iterations = try_null(invoke(jobj, "numIterations")),
    p_values = try_null(invoke(jobj, "pValues")) %>%
      arrange_stats(),
    prediction_col = invoke(jobj, "predictionCol"),
    predictions = invoke(jobj, "predictions") %>% sdf_register(),
    rank = invoke(jobj, "rank"),
    residual_degree_of_freedom = invoke(jobj, "residualDegreeOfFreedom"),
    residual_degree_of_freedom_null = invoke(jobj, "residualDegreeOfFreedomNull"),
    residuals = function(type = "deviance") (invoke(jobj, "residuals", type)
                                             %>% sdf_register()),
    solver = try_null(invoke(jobj, "solver")),
    t_values = try_null(invoke(jobj, "tValues")) %>%
      arrange_stats(),
    subclass = "ml_summary_generalized_linear_regression")
}

new_ml_model_generalized_linear_regression <- function(
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
    subclass = "ml_model_generalized_linear_regression",
    .features = feature_names
  )
}

# Generic implementations

#' @export
print.ml_model_generalized_linear_regression <-
  function(x, digits = max(3L, getOption("digits") - 3L), ...)
  {
    ml_model_print_coefficients(x)
    print_newline()

    cat(
      sprintf("Degress of Freedom:  %s Total (i.e. Null);  %s Residual",
              x$summary$residual_degree_of_freedom_null,
              x$summary$residual_degree_of_freedom),
      sep = "\n"
    )
    cat(sprintf("Null Deviance:       %s", signif(x$summary$null_deviance, digits)), sep = "\n")
    cat(sprintf("Residual Deviance:   %s\tAIC: %s",
                signif(x$summary$deviance, digits),
                signif(x$summary$aic, digits)), sep = "\n")
  }

#' @export
summary.ml_model_generalized_linear_regression <-
  function(object, digits = max(3L, getOption("digits") - 3L), ...)
  {
    ml_model_print_residuals(object, residuals.header = "Deviance Residuals")
    print_newline()
    ml_model_print_coefficients_detailed(object)
    print_newline()

    printf("(Dispersion paramter for %s family taken to be %s)\n\n",
           ml_param(ml_stage(object$pipeline_model, 2), "family"),
           signif(object$summary$dispersion, digits + 3))

    printf("   Null  deviance: %s on %s degress of freedom\n",
           signif(object$summary$null_deviance, digits + 2),
           signif(object$summary$residual_degree_of_freedom_null, digits))

    printf("Residual deviance: %s on %s degrees of freedom\n",
           signif(object$summary$deviance, digits + 2),
           signif(object$summary$degrees_of_freedom, digits))
    printf("AIC: %s\n", signif(object$summary$aic, digits + 1))

    invisible(object)
  }

#' @export
residuals.ml_model_generalized_linear_regression <- function(
  object,
  type = c("deviance", "pearson", "working", "response"),
  ...) {

  type <- rlang::arg_match(type)
  ensure_scalar_character(type)

  residuals <- object %>%
    `[[`("summary") %>%
    `[[`("residuals") %>%
    do.call(list(type = type))

  sdf_read_column(residuals, paste0(type, "Residuals"))
}

#' @rdname sdf_residuals
#' @param type type of residuals which should be returned.
#' @export
sdf_residuals.ml_model_generalized_linear_regression <- function(
  object,
  type = c("deviance", "pearson", "working", "response"),
  ...) {

  type <- rlang::arg_match(type)
  ensure_scalar_character(type)

  residuals <- object %>%
    `[[`("summary") %>%
    `[[`("residuals") %>%
    do.call(list(type = type)) %>%
    dplyr::rename(residuals = !!rlang::sym(paste0(type, "Residuals")))

  ml_model_data(object) %>%
    sdf_fast_bind_cols(residuals)
}


