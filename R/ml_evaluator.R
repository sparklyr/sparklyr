#' Spark ML - Evaluators
#'
#' A set of functions to calculate performance metrics for prediction models. Also see the Spark ML Documentation \href{https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.evaluation.package}{https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.evaluation.package}
#'
#' @param dataset a \code{tbl_spark} containing a label column and a raw prediction column. This should be the output of \code{\link{sdf_predict}} or \code{\link{ml_predict}}.
#' @param label_col Name of column string specifying which column contains the true labels or values.
#' @param metric_name The performance metric. See details.
#' @param prediction_col Name of the column that contains the predicted
#'   label or value NOT the scored probability. Column should be of type
#'   \code{Double}.
#' @details The following metrics are supported
#'   \itemize{
#'    \item Binary Classification: \code{areaUnderROC} (default) or \code{areaUnderPR} (not available in Spark 2.X.)
#'    \item Multiclass Classification: \code{f1} (default), \code{precision}, \code{recall}, \code{weightedPrecision}, \code{weightedRecall} or \code{accuracy}; for Spark 2.X: \code{f1} (default), \code{weightedPrecision}, \code{weightedRecall} or \code{accuracy}.
#'    \item Regression: \code{rmse} (root mean squared error, default),
#'    \code{mse} (mean squared error), \code{r2}, or \code{mae} (mean absolute error.)
#'   }
#'
#' @return The calculated performance metric
#' @name ml_evaluator
NULL

#' @rdname ml_evaluator
#' @param raw_prediction_col Name of column contains the scored probability of a success
#' @export
ml_binary_classification_evaluator <- function(dataset, label_col, raw_prediction_col, metric_name = "areaUnderROC"){
  ml_ratify_args()
  df <- spark_dataframe(dataset)
  sc <- spark_connection(df)

  auc <- invoke_new(sc, "org.apache.spark.ml.evaluation.BinaryClassificationEvaluator") %>%
    invoke("setLabelCol", label_col) %>%
    invoke("setRawPredictionCol", raw_prediction_col) %>%
    invoke("setMetricName", metric_name) %>%
    invoke("evaluate", df)

  auc
}

# Validator
ml_validator_binary_classification_evaluator <- function(args, nms) {
  old_new_mapping <- list(
    predicted_tbl_spark = "dataset",
    label = "label_col",
    score = "raw_prediction_col",
    metric = "metric_name"
  )

  args %>%
    ml_validate_args({
      label_col <- ensure_scalar_character(label_col)
      raw_prediction_col <- ensure_scalar_character(raw_prediction_col)
      metric_name <- rlang::arg_match(metric_name, c("areaUnderROC", "areaUnderPR"))
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

#' @export
ml_binary_classification_eval <- ml_binary_classification_evaluator

ml_validator_binary_classification_eval <- ml_validator_binary_classification_evaluator

#' @rdname ml_evaluator
#' @export
ml_multiclass_classification_evaluator <- function(dataset, label_col, prediction_col, metric_name = "f1"){

  ml_ratify_args()

  df <- spark_dataframe(dataset)
  sc <- spark_connection(df)

  spark_metric = list(
    "1.6" = c("f1", "precision", "recall", "weightedPrecision", "weightedRecall"),
    "2.0" = c("f1", "weightedPrecision", "weightedRecall", "accuracy")
  )

  if (spark_version(sc) >= "2.0.0" && !metric_name %in% spark_metric[["2.0"]] ||
      spark_version(sc) <  "2.0.0" && !metric_name %in% spark_metric[["1.6"]]) {
    stop("Metric ", metric_name, " is unsupported in Spark ", spark_version(sc))
  }

  res <- invoke_new(sc, "org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator") %>%
    invoke("setLabelCol", label_col) %>%
    invoke("setPredictionCol", prediction_col) %>%
    invoke("setMetricName", metric_name) %>%
    invoke("evaluate", df)

  res
}

# Validator
ml_validator_multiclass_classification_evaluator <- function(args, nms) {
  old_new_mapping <- list(
    predicted_tbl_spark = "dataset",
    label = "label_col",
    predicted_lbl = "prediction_col",
    metric = "metric_name"
  )

  args %>%
    ml_validate_args({
      label_col <- ensure_scalar_character(label_col)
      prediction_col <- ensure_scalar_character(prediction_col)
      metric_name <- rlang::arg_match(
        metric_name, c("f1", "precision", "recall", "weightedPrecision", "weightedRecall", "accuracy"))
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

#' @export
ml_classification_eval <- ml_multiclass_classification_evaluator

ml_validator_classification_eval <- ml_validator_multiclass_classification_evaluator

#' @rdname ml_evaluator
#' @export
ml_regression_evaluator <- function(
  dataset, label_col, prediction_col, metric_name = "rmse") {

  label_col <- ensure_scalar_character(label_col)
  prediction_col <- ensure_scalar_character(prediction_col)
  metric_name <- rlang::arg_match(metric_name, c("rmse", "mse", "r2", "mae"))

  df <- spark_dataframe(dataset)
  sc <- spark_connection(df)

  res <- invoke_new(sc, "org.apache.spark.ml.evaluation.RegressionEvaluator") %>%
    invoke("setLabelCol", label_col) %>%
    invoke("setPredictionCol", prediction_col) %>%
    invoke("setMetricName", metric_name) %>%
    invoke("evaluate", df)

  res
}
