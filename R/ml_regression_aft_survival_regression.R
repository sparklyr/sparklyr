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
#' @param censor_col Censor column name. The value of this column could be 0 or
#'   1. If the value is 1, it means the event has occurred i.e. uncensored;
#'   otherwise censored.
#' @param quantile_probabilities Quantile probabilities array. Values of the
#'   quantile probabilities array should be in the range (0, 1) and the array
#'   should be non-empty.
#' @param quantiles_col Quantiles column name. This column will output quantiles
#'   of corresponding quantileProbabilities if it is set.
#'
#' @examples
#' \dontrun{
#'
#' library(survival)
#' library(sparklyr)
#'
#' sc <- spark_connect(master = "local")
#' ovarian_tbl <- sdf_copy_to(sc, ovarian, name = "ovarian_tbl", overwrite = TRUE)
#'
#' partitions <- ovarian_tbl %>%
#'   sdf_random_split(training = 0.7, test = 0.3, seed = 1111)
#'
#' ovarian_training <- partitions$training
#' ovarian_test <- partitions$test
#'
#' sur_reg <- ovarian_training %>%
#'   ml_aft_survival_regression(futime ~ ecog_ps + rx + age + resid_ds, censor_col = "fustat")
#'
#' pred <- ml_predict(sur_reg, ovarian_test)
#' pred
#' }
#'
#' @export
ml_aft_survival_regression <-
  function(x,
           formula = NULL,
           censor_col = "censor",
           quantile_probabilities = c(
             0.01, 0.05, 0.1, 0.25, 0.5,
             0.75, 0.9, 0.95, 0.99
           ),
           fit_intercept = TRUE,
           max_iter = 100L,
           tol = 1e-06,
           aggregation_depth = 2,
           quantiles_col = NULL,
           features_col = "features",
           label_col = "label",
           prediction_col = "prediction",
           uid = random_string("aft_survival_regression_"),
           ...) {
    check_dots_used()
    UseMethod("ml_aft_survival_regression")
  }

ml_aft_survival_regression_impl <-
  function(x,
           formula = NULL,
           censor_col = "censor",
           quantile_probabilities = c(
             0.01, 0.05, 0.1, 0.25, 0.5,
             0.75, 0.9, 0.95, 0.99
           ),
           fit_intercept = TRUE,
           max_iter = 100L,
           tol = 1e-06,
           aggregation_depth = 2,
           quantiles_col = NULL,
           features_col = "features",
           label_col = "label",
           prediction_col = "prediction",
           uid = random_string("aft_survival_regression_"),
           response = NULL,
           features = NULL, ...) {
    aggregation_depth <- param_min_version(x, aggregation_depth, "2.1.0")

    ml_process_model(
      x = x,
      spark_class = "org.apache.spark.ml.regression.AFTSurvivalRegression",
      r_class = "ml_aft_survival_regression",
      ml_function = new_ml_model_aft_survival_regression,
      features = features,
      response = response,
      uid = uid,
      formula = formula,
      invoke_steps = list(
        setFeaturesCol = features_col,
        setLabelCol = label_col,
        setPredictionCol = prediction_col,
        setFitIntercept = cast_scalar_logical(fit_intercept),
        setMaxIter = cast_scalar_integer(max_iter),
        setTol = cast_scalar_double(tol),
        setCensorCol = cast_string(censor_col),
        setQuantileProbabilities = cast_double_list(quantile_probabilities),
        setAggregationDepth = cast_scalar_integer(aggregation_depth),
        setQuantilesCol = cast_nullable_string(quantiles_col)
      )
    )
  }

# ------------------------------- Methods --------------------------------------

# can probably safely get rid of these, since default method will catch them.
#' @export
ml_aft_survival_regression.spark_connection <- ml_aft_survival_regression_impl

#' @export
ml_aft_survival_regression.ml_pipeline <- ml_aft_survival_regression_impl

#' @export
ml_aft_survival_regression.tbl_spark <- ml_aft_survival_regression_impl

# ---------------------------- Constructors ------------------------------------

new_ml_aft_survival_regression_model <- function(jobj) {
  new_ml_transformer(
    jobj,
    coefficients = read_spark_vector(jobj, "coefficients"),
    intercept = possibly_null(invoke)(jobj, "intercept"),
    scale = invoke(jobj, "scale"),
    quantile_probabilities = invoke(jobj, "getQuantileProbabilities"),
    quantiles_col = possibly_null(invoke)(jobj, "getQuantilesCol"),
    class = "ml_aft_survival_regression_model"
  )
}

# ------------------------------ Fitted models ---------------------------------

new_ml_model_aft_survival_regression <- function(pipeline_model, formula, dataset,
                                                 label_col, features_col) {
  m <- new_ml_model_regression(
    pipeline_model,
    formula = formula, dataset = dataset,
    label_col = label_col, features_col = features_col,
    class = "ml_model_aft_survival_regression"
  )

  model <- m$model
  jobj <- spark_jobj(model)

  coefficients <- model$coefficients
  names(coefficients) <- m$feature_names

  m$coefficients <- if (ml_param(model, "fit_intercept")) {
    rlang::set_names(
      c(invoke(jobj, "intercept"), model$coefficients),
      c("(Intercept)", m$feature_names)
    )
  }

  m
}

#' @export
print.ml_model_aft_survival_regression <- function(x, ...) {
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}

# ------------------------------ Deprecated ------------------------------------

#' @rdname ml_aft_survival_regression
#' @template roxlate-ml-old-feature-response
#' @details \code{ml_survival_regression()} is an alias for \code{ml_aft_survival_regression()} for backwards compatibility.
#' @export
ml_survival_regression <- function(x, formula = NULL, censor_col = "censor",
                                   quantile_probabilities = c(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99),
                                   fit_intercept = TRUE, max_iter = 100L, tol = 1e-06,
                                   aggregation_depth = 2, quantiles_col = NULL,
                                   features_col = "features", label_col = "label",
                                   prediction_col = "prediction",
                                   uid = random_string("aft_survival_regression_"),
                                   response = NULL, features = NULL, ...) {
  .Deprecated("ml_aft_survival_regression")
  UseMethod("ml_aft_survival_regression")
}
