
# "rmse" (default): root mean squared error -
# "mse": mean squared error -
# "r2": R^2^ metric -
# "mae": mean absolute error -
# "var": explained variance

#' @importFrom rlang as_name
#' @importFrom purrr map_dfr
#' @importFrom tibble tibble
#' @export
ml_metrics_regression <- function(data, truth, estimate = prediction,
                                  metrics = c("rmse", "rsq", "mae"),
                                  ...) {
  estimate <- enquo(estimate)
  truth <- enquo(truth)

  conn <- spark_connection(data)
  df_data <- spark_dataframe(data)

  evaluator <- "org.apache.spark.ml.evaluation.RegressionEvaluator"
  init_steps <- list(
    "setLabelCol" = as_name(truth),
    "setPredictionCol" = as_name(estimate)
  )
  init <- ml_metrics_init(evaluator, init_steps)

  map_dfr(
    metrics,
    ~ {
      steps <- list(
        "setMetricName" = ml_metrics_conversion(.x),
        "evaluate" = df_data
      )
      val <- ml_metrics_steps(init, steps)
      tibble(.metric = .x, .estimator = "standard", .estimate = val)
    }
  )
}

#' @export
ml_metrics_binary <- function(data, truth, estimate = prediction,
                              metrics = c("roc_auc", "pr_auc"),
                              ...) {
  estimate <- enquo(estimate)
  truth <- enquo(truth)

  conn <- spark_connection(data)
  df_data <- spark_dataframe(data)

  evaluator <- "org.apache.spark.ml.evaluation.BinaryClassificationEvaluator"
  init_steps <- list(
    "setLabelCol" = as_name(truth),
    "setRawPredictionCol" = as_name(estimate)
  )
  init <- ml_metrics_init(evaluator, init_steps)

  map_dfr(
    metrics,
    ~ {
      steps <- list(
        "setMetricName" = ml_metrics_conversion(.x),
        "evaluate" = df_data
      )
      val <- ml_metrics_steps(init, steps)
      tibble(.metric = .x, .estimator = "binary", .estimate = val)
    }
  )
}

#' @export
ml_metrics_multiclass <- function(data, truth, estimate = prediction,
                                  metrics = c("accuracy"),
                                  ...) {
  estimate <- enquo(estimate)
  truth <- enquo(truth)

  conn <- spark_connection(data)
  df_data <- spark_dataframe(data)

  evaluator <- "org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator"
  init_steps <- list(
    "setLabelCol" = as_name(truth),
    "setPredictionCol" = as_name(estimate)
  )
  init <- ml_metrics_init(evaluator, init_steps)

  map_dfr(
    metrics,
    ~ {
      steps <- list(
        "setMetricName" = ml_metrics_conversion(.x),
        "evaluate" = df_data
      )
      val <- ml_metrics_steps(init, steps)
      tibble(.metric = .x, .estimator = "multiclass", .estimate = val)
    }
  )
}

# "f1" (default), "accuracy", "weightedPrecision", "weightedRecall",
# "weightedTruePositiveRate", "weightedFalsePositiveRate", "weightedFMeasure",
# "truePositiveRateByLabel", "falsePositiveRateByLabel", "precisionByLabel",
# "recallByLabel", "fMeasureByLabel", "logLoss", "hammingLoss"



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



ml_metrics_init <- function(evaluator, invoke_steps = list()) {
  new_jobj <- invoke_new(sc, list(evaluator, random_string("metric_")))
  ml_metrics_steps(new_jobj, invoke_steps)
}

#' @importFrom purrr imap
ml_metrics_steps <- function(jobj, invoke_steps = list()) {
  l_steps <- imap(invoke_steps, ~ list(.y, .x))
  for(i in seq_along(l_steps)) {
    jobj <- do.call(invoke, c(jobj, l_steps[[i]]))
  }
  jobj
}







