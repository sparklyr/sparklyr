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
#' lm_model <- mtcars_training %>%
#'   ml_linear_regression(mpg ~ .)
#'
#' pred <- sdf_predict(mtcars_test, lm_model)
#'
#' ml_regression_evaluator(pred, label_col = "mpg")
#' }
#' @export
ml_linear_regression <- function(x, formula = NULL, fit_intercept = TRUE,
                                 elastic_net_param = 0, reg_param = 0,
                                 max_iter = 100, weight_col = NULL,
                                 loss = "squaredError", solver = "auto",
                                 standardization = TRUE, tol = 1e-6,
                                 features_col = "features", label_col = "label",
                                 prediction_col = "prediction",
                                 uid = random_string("linear_regression_"), ...) {
  UseMethod("ml_linear_regression")
}

#' @export
ml_linear_regression.spark_connection <- function(x, formula = NULL, fit_intercept = TRUE,
                                                  elastic_net_param = 0, reg_param = 0,
                                                  max_iter = 100, weight_col = NULL,
                                                  loss = "squaredError", solver = "auto",
                                                  standardization = TRUE, tol = 1e-6,
                                                  features_col = "features", label_col = "label",
                                                  prediction_col = "prediction",
                                                  uid = random_string("linear_regression_"), ...) {

  .args <- list(
    fit_intercept = fit_intercept,
    elastic_net_param = elastic_net_param,
    reg_param = reg_param,
    max_iter = max_iter,
    weight_col = weight_col,
    loss = loss,
    solver = solver,
    standardization = standardization,
    tol = tol,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_linear_regression()

  jobj <- ml_new_regressor(
    x, "org.apache.spark.ml.regression.LinearRegression", uid,
    .args[["features_col"]], .args[["label_col"]], .args[["prediction_col"]]
  ) %>%
    invoke("setElasticNetParam", .args[["elastic_net_param"]]) %>%
    invoke("setFitIntercept", .args[["fit_intercept"]]) %>%
    invoke("setRegParam", .args[["reg_param"]]) %>%
    invoke("setMaxIter", .args[["max_iter"]]) %>%
    invoke("setSolver", .args[["solver"]]) %>%
    invoke("setStandardization", .args[["standardization"]]) %>%
    invoke("setTol", .args[["tol"]]) %>%
    maybe_set_param("setLoss", .args[["loss"]], "2.3.0", "squaredError") %>%
    maybe_set_param("setWeightCol", .args[["weight_col"]])

  new_ml_linear_regression(jobj)
}

#' @export
ml_linear_regression.ml_pipeline <- function(x, formula = NULL, fit_intercept = TRUE,
                                             elastic_net_param = 0, reg_param = 0,
                                             max_iter = 100, weight_col = NULL,
                                             loss = "squaredError", solver = "auto",
                                             standardization = TRUE, tol = 1e-6,
                                             features_col = "features", label_col = "label",
                                             prediction_col = "prediction",
                                             uid = random_string("linear_regression_"), ...) {
  stage <- ml_linear_regression.spark_connection(
    x = spark_connection(x),
    formula = formula,
    fit_intercept = fit_intercept,
    elastic_net_param = elastic_net_param,
    reg_param = reg_param,
    max_iter = max_iter,
    weight_col = weight_col,
    loss = loss,
    solver = solver,
    standardization = standardization,
    tol = tol,
    features_col = features_col,
    label_col = label_col,
    prediction_col = prediction_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_linear_regression.tbl_spark <- function(x, formula = NULL, fit_intercept = TRUE,
                                           elastic_net_param = 0, reg_param = 0,
                                           max_iter = 100, weight_col = NULL,
                                           loss = "squaredError", solver = "auto",
                                           standardization = TRUE, tol = 1e-6,
                                           features_col = "features", label_col = "label",
                                           prediction_col = "prediction",
                                           uid = random_string("linear_regression_"),
                                           response = NULL, features = NULL, ...) {
  ml_formula_transformation()

  stage <- ml_linear_regression.spark_connection(
    x = spark_connection(x),
    formula = formula,
    fit_intercept = fit_intercept,
    elastic_net_param = elastic_net_param,
    reg_param = reg_param,
    max_iter = max_iter,
    weight_col = weight_col,
    loss = loss,
    solver = solver,
    standardization = standardization,
    tol = tol,
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
      "regression", new_ml_model_linear_regression
    )
  }
}

# Validator
ml_validator_linear_regression <- function(.args) {
  .args <- ml_backwards_compatibility(.args, list(
    intercept = "fit_intercept",
    alpha = "elastic_net_param",
    lambda = "reg_param",
    weights.column = "weight_col",
    iter.max = "max_iter",
    max.iter = "max_iter"
  ))

  .args[["elastic_net_param"]] <- cast_scalar_double(.args[["elastic_net_param"]])
  .args[["reg_param"]] <- cast_scalar_double(.args[["reg_param"]])
  .args[["max_iter"]] <- cast_scalar_integer(.args[["max_iter"]])
  .args[["fit_intercept"]] <- cast_scalar_logical(.args[["fit_intercept"]])
  .args[["standardization"]] <- cast_scalar_logical(.args[["standardization"]])
  .args[["tol"]] <- cast_scalar_double(.args[["tol"]])
  .args[["solver"]] <- cast_choice(.args[["solver"]], c("auto", "l-bfgs", "normal"))
  .args[["weight_col"]] <- cast_nullable_string(.args[["weight_col"]])
  .args
}

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

new_ml_summary_linear_regression_model <- function(jobj, fit_intercept) {
  arrange_stats <- make_stats_arranger(fit_intercept)

  new_ml_summary(
    jobj,
    # `lazy val coefficientStandardErrors`
    coefficient_standard_errors = function() try_null(invoke(jobj, "coefficientStandardErrors")) %>%
      arrange_stats(),
    degrees_of_freedom = if (spark_version(spark_connection(jobj)) >= "2.2.0")
      invoke(jobj, "degreesOfFreedom") else NULL,
    # `lazy val devianceResiduals`
    deviance_residuals = function() invoke(jobj, "devianceResiduals"),
    explained_variance = invoke(jobj, "explainedVariance"),
    features_col = invoke(jobj, "featuresCol"),
    label_col = invoke(jobj, "labelCol"),
    mean_absolute_error = invoke(jobj, "meanAbsoluteError"),
    mean_squared_error = invoke(jobj, "meanSquaredError"),
    # `lazy val numInstances`
    num_instances = function() invoke(jobj, "numInstances"),
    # `lazy val pValues`
    p_values = try_null(invoke(jobj, "pValues")) %>%
      arrange_stats(),
    prediction_col = invoke(jobj, "predictionCol"),
    predictions = invoke(jobj, "predictions") %>% sdf_register(),
    r2 = invoke(jobj, "r2"),
    # `lazy val residuals`
    residuals = invoke(jobj, "residuals") %>% sdf_register(),
    root_mean_squared_error = invoke(jobj, "rootMeanSquaredError"),
    # `lazy val tValues`
    t_values = try_null(invoke(jobj, "tValues")) %>%
      arrange_stats(),
    subclass = "ml_summary_linear_regression")
}
