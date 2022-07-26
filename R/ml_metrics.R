
# "rmse" (default): root mean squared error -
# "mse": mean squared error -
# "r2": R^2^ metric -
# "mae": mean absolute error -
# "var": explained variance

#' @importFrom rlang as_name
#' @importFrom purrr map_dfr imap
#' @importFrom tibble tibble
#' @export
ml_metrics_regression <- function(data, truth, estimate = prediction,
                                  metrics = c("rmse", "rsq", "mae"),
                                  ...) {

  ml_metrics_impl(
    data = data,
    truth = as_name(enquo(truth)),
    estimate = as_name(enquo(estimate)),
    metrics = metrics,
    evaluator = "org.apache.spark.ml.evaluation.RegressionEvaluator",
    pred_col = "setRawPredictionCol",
    estimator_name = "standard"
  )

}

#' @export
ml_metrics_binary <- function(data, truth = label, estimate = rawPrediction,
                              metrics = c("roc_auc", "pr_auc"),
                              ...) {
  ml_metrics_impl(
    data = data,
    truth = as_name(enquo(truth)),
    estimate = as_name(enquo(estimate)),
    metrics = metrics,
    evaluator = "org.apache.spark.ml.evaluation.BinaryClassificationEvaluator",
    pred_col = "setRawPredictionCol",
    estimator_name = "binary"
  )

}

#' @export
ml_metrics_multiclass <- function(data, truth = label, estimate = prediction,
                                  metrics = c("accuracy"),
                                  ...) {
  ml_metrics_impl(
    data = data,
    truth = as_name(enquo(truth)),
    estimate = as_name(enquo(estimate)),
    metrics = metrics,
    evaluator = "org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator",
    pred_col = "setPredictionCol",
    estimator_name = "multiclass"
  )
}

# "f1" (default), "accuracy", "weightedPrecision", "weightedRecall",
# "weightedTruePositiveRate", "weightedFalsePositiveRate", "weightedFMeasure",
# "truePositiveRateByLabel", "falsePositiveRateByLabel", "precisionByLabel",
# "recallByLabel", "fMeasureByLabel", "logLoss", "hammingLoss"

ml_metrics_impl <- function(data, truth, estimate, metrics,
                            evaluator, pred_col, estimator_name
                            ) {
  init_steps <- list(truth, estimate)
  names(init_steps) <- c("setLabelCol", pred_col)

  conn <- spark_connection(data)
  new_jobj <- invoke_new(conn, list(evaluator, random_string("metric_")))
  init <- ml_metrics_steps(new_jobj, init_steps)

  map_dfr(
    metrics,
    ~ {
      steps <- list(
        "setMetricName" = ml_metrics_conversion(.x),
        "evaluate" = spark_dataframe(data)
      )
      val <- ml_metrics_steps(init, steps)
      tibble(.metric = .x, .estimator = estimator_name, .estimate = val)
    }
  )
}

ml_metrics_conversion <- function(x) {
  conv_table <- c(
    "f1" = "f1",
    "accuracy" = "accuracy",
    "weightedPrecision" = "precision",
    "rsq" = "r2",
    "roc_auc" = "areaUnderROC",
    "pr_auc" = "areaUnderPR"
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
