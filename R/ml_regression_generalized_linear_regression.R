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
#'
#'@examples
#'\dontrun{
#' library(sparklyr)
#'
#' sc <- spark_connect(master = "local")
#' mtcars_tbl <- sdf_copy_to(sc, mtcars, name = "mtcars_tbl", overwrite = TRUE)
#'
#' partitions <- mtcars_tbl %>%
#'   sdf_partition(training = 0.7, test = 0.3, seed = 1111)
#'
#' mtcars_training <- partitions$training
#' mtcars_test <- partitions$test
#'
#' # Specify the grid
#' family <- c("gaussian", "gamma", "poisson")
#' link <- c("identity", "log")
#' family_link <- expand.grid(family = family, link = link, stringsAsFactors = FALSE)
#' family_link <- data.frame(family_link, rmse = 0)
#'
#' # Train the models
#' for(i in 1:nrow(family_link)){
#'   glm_model <- mtcars_training %>%
#'     ml_generalized_linear_regression(mpg ~ .,
#'                                     family = family_link[i, 1],
#'                                     link = family_link[i, 2])
#'
#'   pred <- sdf_predict(mtcars_test, glm_model)
#'   family_link[i,3] <- ml_regression_evaluator(pred, label_col = "mpg")
#' }
#'
#' family_link
#'}
#'
#' @export
ml_generalized_linear_regression <- function(x, formula = NULL, family = "gaussian",
                                             link = NULL, fit_intercept = TRUE, offset_col = NULL,
                                             link_power = NULL, link_prediction_col = NULL,
                                             reg_param = 0, max_iter = 25, weight_col = NULL,
                                             solver = "irls", tol = 1e-6, variance_power = 0,
                                             features_col = "features", label_col = "label",
                                             prediction_col = "prediction",
                                             uid = random_string("generalized_linear_regression_"),
                                             ...) {
  UseMethod("ml_generalized_linear_regression")
}

#' @export
ml_generalized_linear_regression.spark_connection <- function(x, formula = NULL, family = "gaussian",
                                                              link = NULL, fit_intercept = TRUE, offset_col = NULL,
                                                              link_power = NULL, link_prediction_col = NULL,
                                                              reg_param = 0, max_iter = 25, weight_col = NULL,
                                                              solver = "irls", tol = 1e-6, variance_power = 0,
                                                              features_col = "features", label_col = "label",
                                                              prediction_col = "prediction",
                                                              uid = random_string("generalized_linear_regression_"),
                                                              ...) {
  spark_require_version(x, "2.0.0", "GeneralizedLinearRegression")

  .args <- list(
    family = family,
    link = link,
    fit_intercept = fit_intercept,
    offset_col = offset_col,
    link_power = link_power,
    link_prediction_col = link_prediction_col,
    reg_param = reg_param,
    max_iter = max_iter,
    weight_col = weight_col,
    solver = solver,
    tol = tol,
    variance_power = variance_power,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_generalized_linear_regression()

  jobj <- ml_new_regressor(
    x, "org.apache.spark.ml.regression.GeneralizedLinearRegression", uid,
    .args[["features_col"]], .args[["label_col"]], .args[["prediction_col"]]
  ) %>%
    invoke("setFamily", .args[["family"]]) %>%
    invoke("setFitIntercept", .args[["fit_intercept"]]) %>%
    invoke("setRegParam", .args[["reg_param"]]) %>%
    invoke("setMaxIter", .args[["max_iter"]]) %>%
    invoke("setSolver", .args[["solver"]]) %>%
    invoke("setTol", .args[["tol"]]) %>%
    maybe_set_param("setLinkPower", .args[["link_power"]]) %>%
    maybe_set_param("setVariancePower", .args[["variance_power"]]) %>%
    maybe_set_param("setLink", .args[["link"]]) %>%
    maybe_set_param("setLinkPredictionCol", .args[["link_prediction_col"]]) %>%
    maybe_set_param("setWeightCol", .args[["weight_col"]]) %>%
    maybe_set_param("setOffsetCol", .args[["offset_col"]], "2.3.0")

  new_ml_generalized_linear_regression(jobj)
}

#' @export
ml_generalized_linear_regression.ml_pipeline <- function(x, formula = NULL, family = "gaussian",
                                                         link = NULL, fit_intercept = TRUE, offset_col = NULL,
                                                         link_power = NULL, link_prediction_col = NULL,
                                                         reg_param = 0, max_iter = 25, weight_col = NULL,
                                                         solver = "irls", tol = 1e-6, variance_power = 0,
                                                         features_col = "features", label_col = "label",
                                                         prediction_col = "prediction",
                                                         uid = random_string("generalized_linear_regression_"),
                                                         ...) {

  stage <- ml_generalized_linear_regression(
    x = spark_connection(x),
    formula = formula,
    family = family,
    link = link,
    fit_intercept = fit_intercept,
    offset_col = offset_col,
    link_power = link_power,
    link_prediction_col = link_prediction_col,
    reg_param = reg_param,
    max_iter = max_iter,
    weight_col = weight_col,
    solver = solver,
    tol = tol,
    variance_power = variance_power,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_generalized_linear_regression.tbl_spark <- function(x, formula = NULL, family = "gaussian",
                                                       link = NULL, fit_intercept = TRUE, offset_col = NULL,
                                                       link_power = NULL, link_prediction_col = NULL,
                                                       reg_param = 0, max_iter = 25, weight_col = NULL,
                                                       solver = "irls", tol = 1e-6, variance_power = 0,
                                                       features_col = "features", label_col = "label",
                                                       prediction_col = "prediction",
                                                       uid = random_string("generalized_linear_regression_"),
                                                       response = NULL, features = NULL, ...) {



  ml_formula_transformation()

  stage <- ml_generalized_linear_regression(
    x = spark_connection(x),
    formula = formula,
    family = family,
    link = link,
    fit_intercept = fit_intercept,
    offset_col = offset_col,
    link_power = link_power,
    link_prediction_col = link_prediction_col,
    reg_param = reg_param,
    max_iter = max_iter,
    weight_col = weight_col,
    solver = solver,
    tol = tol,
    variance_power = variance_power,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )

  if (is.null(formula)) {
    stage %>%
      ml_fit(x)
  } else {
    ml_generate_ml_model(
      x, stage, formula, features_col, label_col,
      "regression", new_ml_model_generalized_linear_regression
    )
  }
}

ml_validator_generalized_linear_regression <- function(.args) {
  .args <- ml_backwards_compatibility(.args, list(
    intercept = "fit_intercept",
    weights.column = "weight_col",
    iter.max = "max_iter",
    max.iter = "max_iter"
  ))
  .args[["reg_param"]] <- cast_scalar_double(.args[["reg_param"]])
  .args[["max_iter"]] <- cast_scalar_integer(.args[["max_iter"]])
  fam <- .args[["family"]]
  if (is.function(fam)) {
    warning("Specifying a function for `family` is deprecated; please specify strings for `family` and `link`.")
    fam <- fam()
    .args[["link"]] <- cast_string(fam$link)
    .args[["family"]] <- cast_string(fam$family)
  } else if (inherits(fam, "family")) {
    .args[["link"]] <- cast_string(fam$link)
    .args[["family"]] <- cast_string(fam$family)
  } else {
    .args[["family"]] <- cast_choice(fam, c("gaussian", "binomial", "poisson", "gamma", "tweedie"))
    .args[["link"]] <- cast_nullable_string(.args[["link"]])
  }
  .args[["fit_intercept"]] <- cast_scalar_logical(.args[["fit_intercept"]])
  .args[["solver"]] <- cast_choice(.args[["solver"]], "irls")
  .args[["tol"]] <- cast_scalar_double(.args[["tol"]])
  .args[["offset_col"]] <- cast_nullable_string(.args[["offset_col"]])
  .args[["link_power"]] <- cast_nullable_scalar_double(.args[["link_power"]])
  .args[["variance_power"]] <- cast_nullable_scalar_double(.args[["variance_power"]])
  .args[["weight_col"]] <- cast_nullable_string(.args[["weight_col"]])
  .args[["link_prediction_col"]] <- cast_nullable_string(.args[["link_prediction_col"]])
  .args
}

new_ml_generalized_linear_regression <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_generalized_linear_regression")
}

new_ml_generalized_linear_regression_model <- function(jobj) {
  summary <- if (invoke(jobj, "hasSummary")) {
    fit_intercept <- ml_get_param_map(jobj)$fit_intercept
    new_ml_summary_generalized_linear_regression_model(
      invoke(jobj, "summary"), fit_intercept
    )
  } else
    NULL

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
  arrange_stats <- make_stats_arranger(fit_intercept)

  new_ml_summary(
    jobj,
    aic = function() invoke(jobj, "aic"), # lazy val
    coefficient_standard_errors = function() try_null(invoke(jobj, "coefficientStandardErrors")) %>%
      arrange_stats(), # lazy val
    degrees_of_freedom = function() invoke(jobj, "degreesOfFreedom"), # lazy val
    deviance = function()invoke(jobj, "deviance"), # lazy val
    dispersion = function() invoke(jobj, "dispersion"), # lazy val
    null_deviance = function() invoke(jobj, "nullDeviance"), # lazy val
    num_instances = if (version > "2.2.0") function() invoke(jobj, "numInstances") else NULL, # lazy val
    num_iterations = try_null(invoke(jobj, "numIterations")),
    p_values = function() try_null(invoke(jobj, "pValues")) %>% # lazy val
      arrange_stats(),
    prediction_col = invoke(jobj, "predictionCol"),
    predictions = invoke(jobj, "predictions") %>% sdf_register(),
    rank = invoke(jobj, "rank"), # lazy val
    residual_degree_of_freedom = function() invoke(jobj, "residualDegreeOfFreedom"), # lazy val
    residual_degree_of_freedom_null = function() invoke(jobj, "residualDegreeOfFreedomNull"), # lazy val
    residuals = function(type = "deviance") (invoke(jobj, "residuals", type) %>% sdf_register()),
    solver = try_null(invoke(jobj, "solver")),
    t_values = function() try_null(invoke(jobj, "tValues")) %>% # lazy val
      arrange_stats(),
    subclass = "ml_summary_generalized_linear_regression")
}
