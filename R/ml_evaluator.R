#' Spark ML - Binary Classification Evaluator
#'
#' See the Spark ML Documentation \href{https://spark.apache.org/docs/1.6.1/api/scala/index.html#org.apache.spark.ml.evaluation.BinaryClassificationEvaluator}{BinaryClassificationEvaluator}
#'
#' @param dataset a \code{tbl_spark} containing a label column and a raw prediction column. This should be the output of \code{\link{sdf_predict}} or \code{\link{ml_predict}}.
#' @param label_col Name of column string specifying which column contains the true, indexed labels (ie 0 / 1)
#' @param raw_prediction_col Name of column contains the scored probability of a success (ie 1)
#' @param metric_name The classification metric - one of: areaUnderRoc (default) or areaUnderPR (not available in Spark 2.X)
#'
#' @return  area under the specified curve
#' @export
ml_binary_classification_evaluator <- function(dataset, label_col, raw_prediction_col, metric_name = "areaUnderROC"){
  ml_ratify_args()
  df <- spark_dataframe(dataset)
  sc <- spark_connection(df)

  # spark_metrics <- list(
  #   "2.0" = c("areaUnderROC")
  # )
  #
  # if (spark_version(sc) >= "2.0.0" && !metric %in% spark_metrics[["2.0"]]) {
  #   stop("Metric ", metric, " is unsupported in Spark 2.X")
  # }

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

#' Spark ML - Classification Evaluator
#'
#' See the Spark ML Documentation \href{https://spark.apache.org/docs/1.6.1/api/scala/index.html#org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator}{MulticlassClassificationEvaluator}
#'
#' @param dataset A tbl_spark object that contains a columns with predicted labels
#' @param label_col Name of the column that contains the true, indexed label. Support for binary and multi-class labels, column should be of double type (use as.double)
#' @param prediction_col Name of the column that contains the predicted label NOT the scored probability. Support for binary and multi-class labels, column should be of double type (use as.double)
#' @param metric_name A classification metric, for Spark 1.6: f1 (default), precision, recall, weightedPrecision, weightedRecall or accuracy;
#'   for Spark 2.X: f1 (default), weightedPrecision, weightedRecall or accuracy
#'
#' @return see \code{metric}
#'
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
    stop("Metric ", metric, " is unsupported in Spark ", spark_version(sc))
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
