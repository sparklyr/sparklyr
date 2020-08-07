#' Spark ML - Evaluators
#'
#' A set of functions to calculate performance metrics for prediction models. Also see the Spark ML Documentation \href{https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.evaluation.package}{https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.evaluation.package}
#'
#' @param x A \code{spark_connection} object or a \code{tbl_spark} containing label and prediction columns. The latter should be the output of \code{\link{sdf_predict}}.
#' @param label_col Name of column string specifying which column contains the true labels or values.
#' @param metric_name The performance metric. See details.
#' @param prediction_col Name of the column that contains the predicted
#'   label or value NOT the scored probability. Column should be of type
#'   \code{Double}.
#' @template roxlate-ml-uid
#' @template roxlate-ml-dots
#' @details The following metrics are supported
#'   \itemize{
#'    \item Binary Classification: \code{areaUnderROC} (default) or \code{areaUnderPR} (not available in Spark 2.X.)
#'    \item Multiclass Classification: \code{f1} (default), \code{precision}, \code{recall}, \code{weightedPrecision}, \code{weightedRecall} or \code{accuracy}; for Spark 2.X: \code{f1} (default), \code{weightedPrecision}, \code{weightedRecall} or \code{accuracy}.
#'    \item Regression: \code{rmse} (root mean squared error, default),
#'    \code{mse} (mean squared error), \code{r2}, or \code{mae} (mean absolute error.)
#'   }
#'
#' @return The calculated performance metric
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
#' # for multiclass classification
#' rf_model <- mtcars_training %>%
#'   ml_random_forest(cyl ~ ., type = "classification")
#'
#' pred <- ml_predict(rf_model, mtcars_test)
#'
#' ml_multiclass_classification_evaluator(pred)
#'
#' # for regression
#' rf_model <- mtcars_training %>%
#'   ml_random_forest(cyl ~ ., type = "regression")
#'
#' pred <- ml_predict(rf_model, mtcars_test)
#'
#' ml_regression_evaluator(pred, label_col = "cyl")
#'
#' # for binary classification
#' rf_model <- mtcars_training %>%
#'   ml_random_forest(am ~ gear + carb, type = "classification")
#'
#' pred <- ml_predict(rf_model, mtcars_test)
#'
#' ml_binary_classification_evaluator(pred)
#' }
#'
#' @name ml_evaluator
NULL

#' @rdname ml_evaluator
#' @export
#' @param raw_prediction_col Raw prediction (a.k.a. confidence) column name.
ml_binary_classification_evaluator <- function(x, label_col = "label", raw_prediction_col = "rawPrediction",
                                               metric_name = "areaUnderROC",
                                               uid = random_string("binary_classification_evaluator_"), ...) {
  UseMethod("ml_binary_classification_evaluator")
}

#' @export
ml_binary_classification_evaluator.spark_connection <- function(x, label_col = "label", raw_prediction_col = "rawPrediction",
                                                                metric_name = "areaUnderROC",
                                                                uid = random_string("binary_classification_evaluator_"), ...) {
  .args <- list(
    label_col = label_col,
    raw_prediction_col = raw_prediction_col,
    metric_name = metric_name
  ) %>%
    validator_ml_binary_classification_evaluator()

  spark_pipeline_stage(x, "org.apache.spark.ml.evaluation.BinaryClassificationEvaluator", uid) %>%
    invoke("setLabelCol", .args[["label_col"]]) %>%
    invoke("setRawPredictionCol", .args[["raw_prediction_col"]]) %>%
    invoke("setMetricName", .args[["metric_name"]]) %>%
    new_ml_evaluator()
}

#' @export
ml_binary_classification_evaluator.tbl_spark <- function(x, label_col = "label", raw_prediction_col = "rawPrediction",
                                                         metric_name = "areaUnderROC",
                                                         uid = random_string("binary_classification_evaluator_"), ...) {
  evaluator <- ml_binary_classification_evaluator.spark_connection(
    x = spark_connection(x),
    label_col = label_col,
    raw_prediction_col = raw_prediction_col,
    metric_name = metric_name,
    uid = uid,
    ...
  )

  evaluator %>%
    ml_evaluate(x)
}

# Validator
validator_ml_binary_classification_evaluator <- function(.args) {
  .args[["label_col"]] <- cast_string(.args[["label_col"]])
  .args[["raw_prediction_col"]] <- cast_string(.args[["raw_prediction_col"]])
  .args[["metric_name"]] <- cast_choice(.args[["metric_name"]], c("areaUnderROC", "areaUnderPR"))
  .args
}

#' @rdname ml_evaluator
#' @details \code{ml_binary_classification_eval()} is an alias for \code{ml_binary_classification_evaluator()} for backwards compatibility.
#' @export
ml_binary_classification_eval <- function(x, label_col = "label", prediction_col = "prediction", metric_name = "areaUnderROC") {
  .Deprecated("ml_binary_classification_evaluator")
  UseMethod("ml_binary_classification_evaluator")
}


#' @rdname ml_evaluator
#' @export
ml_multiclass_classification_evaluator <- function(x, label_col = "label", prediction_col = "prediction",
                                                   metric_name = "f1",
                                                   uid = random_string("multiclass_classification_evaluator_"),
                                                   ...) {
  UseMethod("ml_multiclass_classification_evaluator")
}

#' @export
ml_multiclass_classification_evaluator.spark_connection <- function(x, label_col = "label", prediction_col = "prediction",
                                                                    metric_name = "f1",
                                                                    uid = random_string("multiclass_classification_evaluator_"),
                                                                    ...) {
  .args <- list(
    label_col = label_col,
    prediction_col = prediction_col,
    metric_name = metric_name
  ) %>%
    validator_ml_multiclass_classification_evaluator()

  spark_metric <- list(
    "1.6" = c("f1", "precision", "recall", "weightedPrecision", "weightedRecall"),
    "2.0" = c("f1", "weightedPrecision", "weightedRecall", "accuracy")
  )

  if (spark_version(x) >= "2.0.0" && !metric_name %in% spark_metric[["2.0"]] ||
    spark_version(x) < "2.0.0" && !metric_name %in% spark_metric[["1.6"]]) {
    stop("Metric `", metric_name, "` is unsupported in Spark ", spark_version(x), ".")
  }

  spark_pipeline_stage(x, "org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator", uid) %>%
    invoke("setLabelCol", .args[["label_col"]]) %>%
    invoke("setPredictionCol", .args[["prediction_col"]]) %>%
    invoke("setMetricName", .args[["metric_name"]]) %>%
    new_ml_evaluator()
}

#' @export
ml_multiclass_classification_evaluator.tbl_spark <- function(x, label_col = "label", prediction_col = "prediction",
                                                             metric_name = "f1",
                                                             uid = random_string("multiclass_classification_evaluator_"),
                                                             ...) {
  evaluator <- ml_multiclass_classification_evaluator.spark_connection(
    x = spark_connection(x),
    label_col = label_col,
    prediction_col = prediction_col,
    metric_name = metric_name,
    uid = uid,
    ...
  )

  evaluator %>%
    ml_evaluate(x)
}

validator_ml_multiclass_classification_evaluator <- function(.args) {
  .args[["label_col"]] <- cast_string(.args[["label_col"]])
  .args[["prediction_col"]] <- cast_string(.args[["prediction_col"]])
  .args[["metric_name"]] <- cast_choice(
    .args[["metric_name"]],
    c("f1", "precision", "recall", "weightedPrecision", "weightedRecall", "accuracy")
  )
  .args
}

#' @rdname ml_evaluator
#' @details \code{ml_classification_eval()} is an alias for \code{ml_multiclass_classification_evaluator()} for backwards compatibility.
#' @export
ml_classification_eval <- function(x, label_col = "label", prediction_col = "prediction", metric_name = "f1") {
  .Deprecated("ml_multiclass_classification_evaluator")
  UseMethod("ml_multiclass_classification_evaluator")
}

#' @rdname ml_evaluator
#' @export
ml_regression_evaluator <- function(x, label_col = "label", prediction_col = "prediction", metric_name = "rmse",
                                    uid = random_string("regression_evaluator_"), ...) {
  UseMethod("ml_regression_evaluator")
}

#' @export
ml_regression_evaluator.spark_connection <- function(x, label_col = "label", prediction_col = "prediction", metric_name = "rmse",
                                                     uid = random_string("regression_evaluator_"), ...) {
  .args <- list(
    label_col = label_col,
    prediction_col = prediction_col,
    metric_name = metric_name
  ) %>%
    validator_ml_regression_evaluator()

  evaluator <- spark_pipeline_stage(x, "org.apache.spark.ml.evaluation.RegressionEvaluator", uid) %>%
    invoke("setLabelCol", .args[["label_col"]]) %>%
    invoke("setPredictionCol", .args[["prediction_col"]]) %>%
    invoke("setMetricName", .args[["metric_name"]]) %>%
    new_ml_evaluator()

  evaluator
}

#' @export
ml_regression_evaluator.tbl_spark <- function(x, label_col = "label", prediction_col = "prediction", metric_name = "rmse",
                                              uid = random_string("regression_evaluator_"), ...) {
  evaluator <- ml_regression_evaluator.spark_connection(
    x = spark_connection(x),
    label_col = label_col,
    prediction_col = prediction_col,
    metric_name = metric_name
  )

  evaluator %>%
    ml_evaluate(x)
}

validator_ml_regression_evaluator <- function(.args) {
  .args[["label_col"]] <- cast_string(.args[["label_col"]])
  .args[["prediction_col"]] <- cast_string(.args[["prediction_col"]])
  .args[["metric_name"]] <- cast_choice(.args[["metric_name"]], c("rmse", "mse", "r2", "mae"))
  .args
}

new_ml_binary_classification_evaluator <- function(jobj) {
  new_ml_evaluator(jobj, class = "ml_binary_classification_evaluator")
}

new_ml_multiclass_classification_evaluator <- function(jobj) {
  new_ml_evaluator(jobj, class = "ml_multiclass_classification_evaluator")
}

new_ml_regression_evaluator <- function(jobj) {
  new_ml_evaluator(jobj, class = "ml_regression_evaluator")
}

new_ml_clustering_evaluator <- function(jobj) {
  new_ml_evaluator(jobj, class = "ml_clustering_evaluator")
}
