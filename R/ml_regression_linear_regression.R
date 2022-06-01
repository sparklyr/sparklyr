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

ml_linear_regression_impl <- function(x, formula = NULL, fit_intercept = TRUE,
                                           elastic_net_param = 0, reg_param = 0,
                                           max_iter = 100, weight_col = NULL,
                                           loss = "squaredError", solver = "auto",
                                           standardization = TRUE, tol = 1e-6,
                                           features_col = "features", label_col = "label",
                                           prediction_col = "prediction",
                                           uid = random_string("linear_regression_"),
                                           response = NULL, features = NULL, ...) {

  loss <- param_min_version(x, loss, "2.3.0")

  ml_process_model(
    x = x,
    spark_class = "org.apache.spark.ml.regression.LinearRegression",
    r_class = "ml_linear_regression",
    ml_function = new_ml_model_linear_regression,
    features = features,
    response = response,
    uid = uid,
    formula = formula,
    invoke_steps = list(
      setFeaturesCol = features_col,
      setLabelCol = label_col,
      setPredictionCol = prediction_col,
      setElasticNetParam = cast_scalar_double(elastic_net_param),
      setFitIntercept = cast_scalar_logical(fit_intercept),
      setRegParam = cast_scalar_double(reg_param),
      setMaxIter = cast_scalar_integer(max_iter),
      setSolver = cast_choice(solver, c("auto", "l-bfgs", "normal")),
      setStandardization = cast_scalar_logical(standardization),
      setTol = cast_scalar_double(tol),
      setLoss = loss,
      setWeightCol = cast_nullable_string(weight_col)
    )
  )
}

# ------------------------------- Methods --------------------------------------
#' @export
ml_linear_regression.spark_connection <- ml_linear_regression_impl
#' @export
ml_linear_regression.ml_pipeline <- ml_linear_regression_impl
#' @export
ml_linear_regression.tbl_spark <- ml_linear_regression_impl

# ---------------------------- Constructors ------------------------------------
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

# -------------------------- Summary functions ---------------------------------
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
