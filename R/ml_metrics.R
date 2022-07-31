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
#' @param metrics A character vector with the metrics to calculate. For regression models
#' the possible values are: `rmse` (Root mean squared error), `mse` (Mean squared error),
#' `rsq` (R squared), `mae` (Mean absolute error), and `var` (Explained variance).
#'  Defaults to: `rmse`, `rsq`, `mae`
#' @importFrom rlang as_name
#' @importFrom purrr map_dfr imap
#' @importFrom tibble tibble
#' @export
ml_metrics_regression <- function(x, truth, estimate = prediction,
                                  metrics = c("rmse", "rsq", "mae"),
                                  ...) {
  ml_metrics_impl(
    x = x,
    truth = as_name(enquo(truth)),
    estimate = as_name(enquo(estimate)),
    metrics = metrics,
    evaluator = "org.apache.spark.ml.evaluation.RegressionEvaluator",
    pred_col = "setRawPredictionCol",
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
#' @export
ml_metrics_binary <- function(x, truth = label, estimate = rawPrediction,
                              metrics = c("roc_auc", "pr_auc"),
                              ...) {
  ml_metrics_impl(
    x = x,
    truth = as_name(enquo(truth)),
    estimate = as_name(enquo(estimate)),
    metrics = metrics,
    evaluator = "org.apache.spark.ml.evaluation.BinaryClassificationEvaluator",
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
#' @inherit ml_metrics_regression
#' @param metrics A character vector with the metrics to calculate. For multiclass
#' models the possible values are: `acurracy`, `f_meas` (F-score), `recall` and
#' `precision`. This function translates the argument into an acceptable Spark
#' parameter. If no translation is found, then the raw value of the argument is
#' passed to Spark. This makes it possible to request a metric that is not listed
#' here but, depending on version, it is available in Spark. Other metrics form
#' multi-class models are: `weightedTruePositiveRate`, `weightedFalsePositiveRate`,
#' `weightedFMeasure`, `truePositiveRateByLabel`, `falsePositiveRateByLabel`,
#' `precisionByLabel`, `recallByLabel`, `fMeasureByLabel`, `logLoss`, `hammingLoss`
#' @export
ml_metrics_multiclass <- function(x, truth = label, estimate = prediction,
                                  metrics = c("accuracy"),
                                  ...) {
  ml_metrics_impl(
    x = x,
    truth = as_name(enquo(truth)),
    estimate = as_name(enquo(estimate)),
    metrics = metrics,
    evaluator = "org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator",
    pred_col = "setPredictionCol",
    estimator_name = "multiclass"
  )
}

ml_metrics_impl <- function(x, truth, estimate, metrics,
                            evaluator, pred_col, estimator_name
                            ) {
  init_steps <- list(truth, estimate)
  names(init_steps) <- c("setLabelCol", pred_col)

  conn <- spark_connection(x)
  new_jobj <- invoke_new(conn, list(evaluator, random_string("metric_")))
  init <- ml_metrics_steps(new_jobj, init_steps)

  map_dfr(
    metrics,
    ~ {
      steps <- list(
        "setMetricName" = ml_metrics_conversion(.x),
        "evaluate" = spark_xframe(x)
      )
      val <- ml_metrics_steps(init, steps)
      tibble(.metric = .x, .estimator = estimator_name, .estimate = val)
    }
  )
}

ml_metrics_conversion <- function(x) {

  conv_table <- c(
    "accuracy" = "accuracy",
    "weightedPrecision" = "precision",
    "rsq" = "r2",
    "roc_auc" = "areaUnderROC",
    "pr_auc" = "areaUnderPR",
    "f_meas" = "f1",
    "recall" = "weightedRecall",
    "precision" = "weightedPrecision"
  )

  match <- conv_table[conv_table == x]
  if(length(match) == 1) {
    names(match)
  } else {
    x
  }
}

ml_metrics_steps <- function(jobj, invoke_steps = list()) {
  l_steps <- imap(invoke_steps, ~ list(.y, .x))
  for(i in seq_along(l_steps)) {
    jobj <- do.call(invoke, c(jobj, l_steps[[i]]))
  }
  jobj
}
