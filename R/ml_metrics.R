#' Extracts metrics from a fitted table
#' @description The function works best when passed a `tbl_spark` created by
#' `ml_predict()`. The output `tbl_spark` will contain the correct variable
#' types and format that the given Spark model "evaluator" expects.
#' @details The `ml_metrics` family of functions implement Spark's `evaluate`
#' closer to how the `yardstick` package works. The functions expect a table
#' containing the truth and estimate, and return a `tibble` with the results. The
#' `tibble` has the same format and variable names as the output of the `yardstick`
#' functions.
#' @param x A `tbl_spark` containing the estimate (prediction) and the truth (value
#' of what actually happened)
#' @param truth The name of the column from `x` that contains the value of what
#' actually happened
#' @param estimate The name of the column from `x` that contains the prediction.
#' Defaults to `prediction`, since it is the default that `ml_predict()` uses.
#' @param metrics A character vector with the metrics to calculate. For regression
#' models the possible values are: `rmse` (Root mean squared error), `mse` (Mean
#' squared error),`rsq` (R squared), `mae` (Mean absolute error), and `var`
#' (Explained variance). Defaults to: `rmse`, `rsq`, `mae`
#' @param ... Optional arguments; currently unused.
#' @importFrom rlang as_name
#' @importFrom purrr map_dfr imap
#' @importFrom tibble tibble
#' @examples
#' \dontrun{
#' sc <- spark_connect("local")
#' tbl_iris <- copy_to(sc, iris)
#' iris_split <- sdf_random_split(tbl_iris, training = 0.5, test = 0.5)
#' training <- iris_split$training
#' reg_formula <- "Sepal_Length ~ Sepal_Width + Petal_Length + Petal_Width"
#' model <- ml_generalized_linear_regression(training, reg_formula)
#' tbl_predictions <- ml_predict(model, iris_split$test)
#' tbl_predictions %>%
#'   ml_metrics_regression(Sepal_Length)
#' }
#' @export
ml_metrics_regression <- function(x, truth, estimate = prediction,
                                  metrics = c("rmse", "rsq", "mae"),
                                  ...) {
  ml_metrics_impl(
    x = x,
    truth = as_name(enquo(truth)),
    estimate = as_name(enquo(estimate)),
    metrics = metrics,
    evaluator = "RegressionEvaluator",
    pred_col = "setPredictionCol",
    estimator_name = "standard"
  )
}

#' @param truth The name of the column from `x` with an integer field
#' containing the binary response (0 or 1). The `ml_predict()` function will
#' create a new field named `label` which contains the expected type and values.
#' `truth` defaults to `label`.
#' @param estimate The name of the column from `x` that contains the prediction.
#' Defaults to `rawPrediction`, since its type and expected values will match `truth`.
#' @param metrics A character vector with the metrics to calculate. For binary models
#' the possible values are: `roc_auc` (Area under the Receiver Operator curve),
#' `pr_auc` (Area under the Precesion Recall curve).
#'  Defaults to: `roc_auc`, `pr_auc`
#' @inherit ml_metrics_regression
#' @examples
#' \dontrun{
#' sc <- spark_connect("local")
#' tbl_iris <- copy_to(sc, iris)
#' prep_iris <- tbl_iris %>%
#'   mutate(is_setosa = ifelse(Species == "setosa", 1, 0))
#' iris_split <- sdf_random_split(prep_iris, training = 0.5, test = 0.5)
#' model <- ml_logistic_regression(iris_split$training, "is_setosa ~ Sepal_Length")
#' tbl_predictions <- ml_predict(model, iris_split$test)
#' ml_metrics_binary(tbl_predictions)
#' }
#' @export
ml_metrics_binary <- function(x, truth = label, estimate = rawPrediction,
                              metrics = c("roc_auc", "pr_auc"),
                              ...) {
  ml_metrics_impl(
    x = x,
    truth = as_name(enquo(truth)),
    estimate = as_name(enquo(estimate)),
    metrics = metrics,
    evaluator = "BinaryClassificationEvaluator",
    pred_col = "setRawPredictionCol",
    estimator_name = "binary"
  )
}

#' @param truth The name of the column from `x` with an integer field containing
#' an the indexed value for each outcome . The `ml_predict()` function will
#' create a new field named `label` which contains the expected type and values.
#' `truth` defaults to `label`.
#' @param estimate The name of the column from `x` that contains the prediction.
#' Defaults to `prediction`, since its type and indexed values will match `truth`.
#' @param metrics A character vector with the metrics to calculate. For multiclass
#' models the possible values are: `acurracy`, `f_meas` (F-score), `recall` and
#' `precision`. This function translates the argument into an acceptable Spark
#' parameter. If no translation is found, then the raw value of the argument is
#' passed to Spark. This makes it possible to request a metric that is not listed
#' here but, depending on version, it is available in Spark. Other metrics form
#' multi-class models are: `weightedTruePositiveRate`, `weightedFalsePositiveRate`,
#' `weightedFMeasure`, `truePositiveRateByLabel`, `falsePositiveRateByLabel`,
#' `precisionByLabel`, `recallByLabel`, `fMeasureByLabel`, `logLoss`, `hammingLoss`
#' @param beta Numerical value used for precision and recall. Defaults to NULL, but
#' if the Spark session's verion is 3.0 and above, then NULL is changed to 1,
#' unless something different is supplied in this argument.
#' @examples
#' \dontrun{
#' sc <- spark_connect("local")
#' tbl_iris <- copy_to(sc, iris)
#' iris_split <- sdf_random_split(tbl_iris, training = 0.5, test = 0.5)
#' model <- ml_random_forest(iris_split$training, "Species ~ .")
#' tbl_predictions <- ml_predict(model, iris_split$test)
#'
#' ml_metrics_multiclass(tbl_predictions)
#'
#' # Request different metrics
#' ml_metrics_multiclass(tbl_predictions, metrics = c("recall", "precision"))
#'
#' # Request metrics not translated by the function, but valid in Spark
#' ml_metrics_multiclass(tbl_predictions, metrics = c("logLoss", "hammingLoss"))
#' }
#' @inherit ml_metrics_regression
#' @export
ml_metrics_multiclass <- function(x, truth = label, estimate = prediction,
                                  metrics = c("accuracy"), beta = NULL,
                                  ...) {
  ml_metrics_impl(
    x = x,
    truth = as_name(enquo(truth)),
    estimate = as_name(enquo(estimate)),
    metrics = metrics,
    evaluator = "MulticlassClassificationEvaluator",
    pred_col = "setPredictionCol",
    estimator_name = "multiclass",
    beta = beta
  )
}

ml_metrics_impl <- function(x, truth, estimate, metrics,
                            evaluator, pred_col, estimator_name,
                            beta = NULL) {

  if(spark_version(spark_connection(x)) >= 3) {
    if(is.null(beta) && evaluator == "MulticlassClassificationEvaluator") beta <- 1
  }

  init_steps <- list(truth, estimate)
  names(init_steps) <- c("setLabelCol", pred_col)

  conn <- spark_connection(x)
  new_jobj <- invoke_new(
    conn, list(
      paste0("org.apache.spark.ml.evaluation.", evaluator),
      random_string("metric_")
    )
  )
  init <- ml_metrics_steps(new_jobj, init_steps)

  map_dfr(
    metrics,
    ~ {
      steps <- list("setMetricName" = ml_metrics_conversion(.x))
      if (!is.null(beta)) steps <- c(steps, list("setBeta" = beta))
      steps <- c(steps, list("evaluate" = spark_dataframe(x)))
      val <- ml_metrics_steps(init, steps)
      tibble(.metric = .x, .estimator = estimator_name, .estimate = val)
    }
  )
}

ml_metrics_conversion <- function(x) {

  # Spark's value ------- R arg value
  conv_table <- c(
    "weightedPrecision" = "precision",
    "r2" = "rsq",
    "areaUnderROC" = "roc_auc",
    "areaUnderPR" = "pr_auc",
    "f1" = "f_meas",
    "weightedRecall" = "recall"
  )

  match <- conv_table[conv_table == x]
  if (length(match) == 1) {
    names(match)
  } else {
    x
  }
}

ml_metrics_steps <- function(jobj, invoke_steps = list()) {
  l_steps <- imap(invoke_steps, ~ list(.y, .x))
  for (i in seq_along(l_steps)) {
    jobj <- do.call(invoke, c(jobj, l_steps[[i]]))
  }
  jobj
}

utils::globalVariables(c("label", "rawPrediction", "prediction"))
