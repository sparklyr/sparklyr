

#' Spark ML - Binary Classification Evaluator
#'
#' See the Spark ML Documentation \href{https://spark.apache.org/docs/1.6.1/api/scala/index.html#org.apache.spark.ml.evaluation.BinaryClassificationEvaluator}{BinaryClassificationEvaluator}
#'
#' @param predicted_tbl_spark The result of running sdf_predict
#' @param label Name of column string specifying which column contains the true, indexed labels (ie 0 / 1)
#' @param score Name of column contains the scored probability of a success (ie 1)
#' @param metric The classification metric - one of: areaUnderRoc (default) or areaUnderPR (not available in Spark 2.X)
#'
#' @return  area under the specified curve
#' @export


ml_binary_classification_eval <- function(predicted_tbl_spark, label, score, metric = "areaUnderROC"){
  df <- spark_dataframe(predicted_tbl_spark)
  sc <- spark_connection(df)

  spark_metrics <- list(
    "2.0" = c("areaUnderROC")
  )

  if (spark_version(sc) >= "2.0.0" && !metric %in% spark_metrics[["2.0"]]) {
    stop("Metric ", metric, " is unsupported in Spark 2.X")
  }

  envir <- new.env(parent = emptyenv())

  tdf <- df %>%
    ml_prepare_dataframe(response = label, features = c(score, score), envir = envir)

  auc <- invoke_new(sc, "org.apache.spark.ml.evaluation.BinaryClassificationEvaluator") %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setRawPredictionCol", envir$features) %>%
    invoke("setMetricName", metric) %>%
    invoke("evaluate", tdf)

  return(auc)
}

#' Spark ML - Classification Evaluator
#'
#' See the Spark ML Documentation \href{https://spark.apache.org/docs/1.6.1/api/scala/index.html#org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator}{MulticlassClassificationEvaluator}
#'
#' @param predicted_tbl_spark A tbl_spark object that contains a columns with predicted labels
#' @param label Name of the column that contains the true, indexed label. Support for binary and multi-class labels, column should be of double type (use as.double)
#' @param predicted_lbl Name of the column that contains the predicted label NOT the scored probability. Support for binary and multi-class labels, column should be of double type (use as.double)
#' @param metric A classification metric, for Spark 1.6: f1 (default), precision, recall, weightedPrecision, weightedRecall or accuracy;
#'   for Spark 2.X: f1 (default), weightedPrecision, weightedRecall or accuracy
#'
#' @return see \code{metric}
#'
#' @export


ml_classification_eval <- function(predicted_tbl_spark, label, predicted_lbl, metric = "f1"){
  df <- spark_dataframe(predicted_tbl_spark)
  sc <- spark_connection(df)

  spark_metric = list(
    "1.6" = c("f1", "precision", "recall", "weightedPrecision", "weightedRecall"),
    "2.0" = c("f1", "weightedPrecision", "weightedRecall", "accuracy")
  )

  if (spark_version(sc) >= "2.0.0" && !metric %in% spark_metric[["2.0"]] ||
      spark_version(sc) <  "2.0.0" && !metric %in% spark_metric[["1.6"]]) {
    stop("Metric ", metric, " is unsupported in Spark ", spark_version(sc))
  }

  res <- invoke_new(sc, "org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator") %>%
    invoke("setLabelCol", label) %>%
    invoke("setPredictionCol", predicted_lbl) %>%
    invoke("setMetricName", metric) %>%
    invoke("evaluate", df)

  return(res)
}


