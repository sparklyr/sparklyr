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
#'   sdf_random_split(training = 0.7, test = 0.3, seed = 1111)
#'
#' mtcars_training <- partitions$training
#' mtcars_test <- partitions$test
#'
#' lm_model <- mtcars_training %>%
#'   ml_linear_regression(mpg ~ .)
#'
#' pred <- ml_predict(lm_model, mtcars_test)
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
  check_dots_used()
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
    validator_ml_linear_regression()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.regression.LinearRegression", uid,
    features_col = .args[["features_col"]],
    label_col = .args[["label_col"]],
    prediction_col = .args[["prediction_col"]]
  ) %>%
    invoke("setElasticNetParam", .args[["elastic_net_param"]]) %>%
    invoke("setFitIntercept", .args[["fit_intercept"]]) %>%
    invoke("setRegParam", .args[["reg_param"]]) %>%
    invoke("setMaxIter", .args[["max_iter"]]) %>%
    invoke("setSolver", .args[["solver"]]) %>%
    invoke("setStandardization", .args[["standardization"]]) %>%
    invoke("setTol", .args[["tol"]]) %>%
    jobj_set_param("setLoss", .args[["loss"]], "2.3.0", "squaredError") %>%
    jobj_set_param("setWeightCol", .args[["weight_col"]])

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
  formula <- ml_standardize_formula(formula, response, features)

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
    ml_construct_model_supervised(
      new_ml_model_linear_regression,
      predictor = stage,
      formula = formula,
      dataset = x,
      features_col = features_col,
      label_col = label_col
    )
  }
}

# Validator
validator_ml_linear_regression <- function(.args) {
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
  new_ml_predictor(jobj, class = "ml_linear_regression")
}

new_ml_linear_regression_model <- function(jobj) {
  summary <- if (invoke(jobj, "hasSummary")) {
    fit_intercept <- ml_get_param_map(jobj)$fit_intercept
    new_ml_linear_regression_training_summary(invoke(jobj, "summary"), fit_intercept)
  } else {
    NULL
  }

  new_ml_prediction_model(
    jobj,
    coefficients = read_spark_vector(jobj, "coefficients"),
    intercept = invoke(jobj, "intercept"),
    scale = if (spark_version(spark_connection(jobj)) >= "2.3.0") invoke(jobj, "scale"),
    summary = summary,
    class = "ml_linear_regression_model"
  )
}

new_ml_linear_regression_summary <- function(jobj, fit_intercept, ..., class = character()) {
  arrange_stats <- make_stats_arranger(fit_intercept)

  s <- new_ml_summary(
    jobj,
    # `lazy val coefficientStandardErrors`
    coefficient_standard_errors = possibly_null(
      ~ invoke(jobj, "coefficientStandardErrors") %>%
        arrange_stats()
    ),
    # `lazy val devianceResiduals`
    deviance_residuals = function() invoke(jobj, "devianceResiduals"),
    explained_variance = invoke(jobj, "explainedVariance"),
    features_col = if (spark_version(spark_connection(jobj)) >= "2.0.0") invoke(jobj, "featuresCol") else NULL,
    label_col = invoke(jobj, "labelCol"),
    mean_absolute_error = invoke(jobj, "meanAbsoluteError"),
    mean_squared_error = invoke(jobj, "meanSquaredError"),
    # `lazy val numInstances`
    num_instances = function() invoke(jobj, "numInstances"),
    # `lazy val pValues`
    p_values = possibly_null(
      ~ invoke(jobj, "pValues") %>%
        arrange_stats()
    ),
    prediction_col = invoke(jobj, "predictionCol"),
    predictions = invoke(jobj, "predictions") %>% sdf_register(),
    r2 = invoke(jobj, "r2"),
    # `lazy val residuals`
    residuals = function() invoke(jobj, "residuals") %>% sdf_register(),
    root_mean_squared_error = invoke(jobj, "rootMeanSquaredError"),
    # `lazy val tValues`
    t_values = possibly_null(
      ~ invoke(jobj, "tValues") %>%
        arrange_stats()
    ),
    ...,
    class = "ml_linear_regression_summary"
  )

  if (spark_version(spark_connection(jobj)) >= "2.2.0") {
    s$degrees_of_freedom <- invoke(jobj, "degreesOfFreedom")
  }

  if (spark_version(spark_connection(jobj)) >= "2.3.0") {
    s$r2adj <- invoke(jobj, "r2adj")
  }

  s
}

new_ml_linear_regression_training_summary <- function(jobj, fit_intercept) {
  s <- new_ml_linear_regression_summary(
    jobj, fit_intercept,
    objective_history = invoke(jobj, "objectiveHistory"),
    total_iterations = invoke(jobj, "totalIterations"),
    class = "ml_linear_regression_training_summary"
  )

  if (is.null(s$features_col)) {
    s$features_col <- invoke(jobj, "featuresCol")
  }

  s
}
