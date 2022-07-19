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
#' @examples
#' \dontrun{
#' library(sparklyr)
#'
#' sc <- spark_connect(master = "local")
#' mtcars_tbl <- sdf_copy_to(sc, mtcars, name = "mtcars_tbl", overwrite = TRUE)
#'
#' partitions <- mtcars_tbl %>%
#'   sdf_random_split(training = 0.7, test = 0.3, seed = 1111)
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
#' for (i in seq_len(nrow(family_link))) {
#'   glm_model <- mtcars_training %>%
#'     ml_generalized_linear_regression(mpg ~ .,
#'       family = family_link[i, 1],
#'       link = family_link[i, 2]
#'     )
#'
#'   pred <- ml_predict(glm_model, mtcars_test)
#'   family_link[i, 3] <- ml_regression_evaluator(pred, label_col = "mpg")
#' }
#'
#' family_link
#' }
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
  #check_dots_used()
  UseMethod("ml_generalized_linear_regression")
}

ml_generalized_linear_regression_impl <- function(x, formula = NULL, family = "gaussian",
                                             link = NULL, fit_intercept = TRUE, offset_col = NULL,
                                             link_power = NULL, link_prediction_col = NULL,
                                             reg_param = 0, max_iter = 25, weight_col = NULL,
                                             solver = "irls", tol = 1e-6, variance_power = 0,
                                             features_col = "features", label_col = "label",
                                             prediction_col = "prediction",
                                             uid = random_string("generalized_linear_regression_"),
                                             response = NULL, features = NULL,
                                             ...) {
  offset_col <- param_min_version(x, offset_col, "2.3.0")

  fam <- family
  if (is.function(fam)) {
    warning("Specifying a function for `family` is deprecated; please specify strings for `family` and `link`.")
    fam <- fam()
    }

  if (inherits(fam, "family") | is.function(fam)) {
    link <- fam$link
    family <- fam$family
  }

  ml_process_model(
    x = x,
    r_class = "ml_generalized_linear_regression",
    ml_function = new_ml_model_generalized_linear_regression,
    features = features,
    response = response,
    uid = uid,
    formula = formula,
    invoke_steps = list(
      features_col = features_col,
      label_col = label_col,
      prediction_col = prediction_col,
      family = family,
      link = link,
      fit_intercept = fit_intercept,
      reg_param = reg_param,
      max_iter = max_iter,
      solver = solver,
      tol = tol,
      link_power = link_power,
      variance_power = variance_power,
      link_prediction_col = link_prediction_col,
      weight_col = weight_col,
      offset_col = offset_col
    )
  )
}

params_validator.ml_generalized_linear_regression <- function(x) {
  x <- params_base_validator(x)
  x$solver <- function(x) cast_choice(x, "irls")
  x
}


# ------------------------------- Methods --------------------------------------

#' @export
ml_generalized_linear_regression.spark_connection <- ml_generalized_linear_regression_impl

#' @export
ml_generalized_linear_regression.ml_pipeline <- ml_generalized_linear_regression_impl

#' @export
ml_generalized_linear_regression.tbl_spark <- ml_generalized_linear_regression_impl

# ------------------------------ Fitted models ---------------------------------

new_ml_generalized_linear_regression_model <- function(jobj) {
  summary <- if (invoke(jobj, "hasSummary")) {
    fit_intercept <- ml_get_param_map(jobj)$fit_intercept
    new_ml_generalized_linear_regression_training_summary(
      invoke(jobj, "summary"), fit_intercept
    )
  } else {
    NULL
  }

  new_ml_prediction_model(
    jobj,
    coefficients = read_spark_vector(jobj, "coefficients"),
    intercept = invoke(jobj, "intercept"),
    link_prediction_col = if (
      invoke(jobj, "isSet", invoke(jobj, "linkPredictionCol")))
      invoke(jobj, "getLinkPredictionCol")
    else NULL
    ,
    summary = summary,
    class = "ml_generalized_linear_regression_model"
  )
}

new_ml_generalized_linear_regression_summary <- function(jobj, fit_intercept, ..., class = character()) {
  version <- jobj %>%
    spark_connection() %>%
    spark_version()
  arrange_stats <- make_stats_arranger(fit_intercept)

  new_ml_summary(
    jobj,
    aic = function() invoke(jobj, "aic"), # lazy val

    degrees_of_freedom = function() invoke(jobj, "degreesOfFreedom"), # lazy val
    deviance = function() invoke(jobj, "deviance"), # lazy val
    dispersion = function() invoke(jobj, "dispersion"), # lazy val
    null_deviance = function() invoke(jobj, "nullDeviance"), # lazy val
    num_instances = if (version > "2.2.0") function() invoke(jobj, "numInstances") else NULL, # lazy val
    prediction_col = invoke(jobj, "predictionCol"),
    predictions = invoke(jobj, "predictions") %>% sdf_register(),
    rank = invoke(jobj, "rank"), # lazy val
    residual_degree_of_freedom = function() invoke(jobj, "residualDegreeOfFreedom"), # lazy val
    residual_degree_of_freedom_null = function() invoke(jobj, "residualDegreeOfFreedomNull"), # lazy val
    residuals = function(type = "deviance") (invoke(jobj, "residuals", type) %>% sdf_register()),
    ...,
    class = "ml_generalized_linear_regression_summary"
  )
}

# ---------------------------- Constructors ------------------------------------

new_ml_generalized_linear_regression_training_summary <- function(jobj, fit_intercept) {
  arrange_stats <- make_stats_arranger(fit_intercept)

  s <- new_ml_generalized_linear_regression_summary(
    jobj, fit_intercept,
    coefficient_standard_errors = possibly_null(~ invoke(jobj, "coefficientStandardErrors") %>% arrange_stats()),
    num_iterations = invoke(jobj, "numIterations"),
    solver = invoke(jobj, "solver"),
    p_values = possibly_null(~ invoke(jobj, "pValues") %>% arrange_stats()),
    t_values = possibly_null(~ invoke(jobj, "tValues") %>% arrange_stats()),
    class = "ml_generalized_linear_regression_training_summary"
  )

  s
}
