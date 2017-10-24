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
#' @name ml_evaluator
NULL

#' @rdname ml_evaluator
#' @export
#' @param raw_prediction_col Name of column contains the scored probability of a success
ml_binary_classification_evaluator <- function(
  x, label_col = "label",
  raw_prediction_col = "rawPrediction",
  metric_name = "areaUnderROC",
  uid = random_string("binary_classification_evaluator_"),
  ...
) {
  UseMethod("ml_binary_classification_evaluator")
}

#' @export
ml_binary_classification_evaluator.spark_connection <- function(
  x, label_col = "label", raw_prediction_col = "rawPrediction",
  metric_name = "areaUnderROC",
  uid = random_string("binary_classification_evaluator_"),
  ...) {

  ml_ratify_args()

  evaluator <- ml_new_identifiable(x, "org.apache.spark.ml.evaluation.BinaryClassificationEvaluator",
                          uid) %>%
    invoke("setLabelCol", label_col) %>%
    invoke("setRawPredictionCol", raw_prediction_col) %>%
    invoke("setMetricName", metric_name) %>%
    new_ml_evaluator()

  evaluator
}

#' @export
ml_binary_classification_evaluator.tbl_spark <- function(
  x, label_col = "label", raw_prediction_col = "rawPrediction",
  metric_name = "areaUnderROC",
  uid = random_string("binary_classification_evaluator_"),
  ...){

  sc <- spark_connection(x)

  evaluator <- ml_binary_classification_evaluator(
    sc, label_col, raw_prediction_col, metric_name)

  evaluator %>%
    ml_evaluate(x)
}

# Validator
ml_validator_binary_classification_evaluator <- function(args, nms) {
  old_new_mapping <- list(
    predicted_tbl_spark = "x",
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

#' @rdname ml_evaluator
#' @details \code{ml_binary_classification_eval()} is an alias for \code{ml_binary_classification_evaluator()} for backwards compatibility.
#' @export
ml_binary_classification_eval <- function(
  x, label_col = "label", prediction_col = "prediction", metric_name = "areaUnderROC") {
  UseMethod("ml_binary_classification_evaluator")
}


#' @rdname ml_evaluator
#' @export
ml_multiclass_classification_evaluator <- function(
  x, label_col = "label", prediction_col = "prediction", metric_name = "f1",
  uid = random_string("multiclass_classification_evaluator_"),
  ...){
  UseMethod("ml_multiclass_classification_evaluator")
}

#' @export
ml_multiclass_classification_evaluator.spark_connection <- function(
  x, label_col = "label", prediction_col = "prediction", metric_name = "f1",
  uid = random_string("multiclass_classification_evaluator_"),
  ...
) {
  ml_ratify_args()

  spark_metric = list(
    "1.6" = c("f1", "precision", "recall", "weightedPrecision", "weightedRecall"),
    "2.0" = c("f1", "weightedPrecision", "weightedRecall", "accuracy")
  )

  if (spark_version(x) >= "2.0.0" && !metric_name %in% spark_metric[["2.0"]] ||
      spark_version(x) <  "2.0.0" && !metric_name %in% spark_metric[["1.6"]]) {
    stop("Metric ", metric_name, " is unsupported in Spark ", spark_version(x))
  }

  evaluator <- ml_new_identifiable(
    x, "org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator", uid) %>%
    invoke("setLabelCol", label_col) %>%
    invoke("setPredictionCol", prediction_col) %>%
    invoke("setMetricName", metric_name) %>%
    new_ml_evaluator()

  evaluator
}

#' @export
ml_multiclass_classification_evaluator.tbl_spark <- function(
  x, label_col = "label", prediction_col = "prediction", metric_name = "f1",
  uid = random_string("multiclass_classification_evaluator_"),
  ...){
  sc <- spark_connection(x)
  evaluator <- ml_multiclass_classification_evaluator(
    sc, label_col, prediction_col, metric_name)
  evaluator %>%
    ml_evaluate(x)
}

# Validator
ml_validator_multiclass_classification_evaluator <- function(args, nms) {
  old_new_mapping <- list(
    predicted_tbl_spark = "x",
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

#' @rdname ml_evaluator
#' @details \code{ml_classification_eval()} is an alias for \code{ml_multiclass_classification_evaluator()} for backwards compatibility.
#' @export
ml_classification_eval <- function(
  x, label_col = "label", prediction_col = "prediction", metric_name = "f1") {
  UseMethod("ml_multiclass_classification_evaluator")
}

#' @rdname ml_evaluator
#' @export
ml_regression_evaluator <- function(
  x, label_col = "label", prediction_col = "prediction", metric_name = "rmse",
  uid = random_string("regression_evaluator_"),
  ...) {
  UseMethod("ml_regression_evaluator")
}

#' @export
ml_regression_evaluator.spark_connection <- function(
  x, label_col = "label", prediction_col = "prediction", metric_name = "rmse",
  uid = random_string("regression_evaluator_"),
  ...) {

  label_col <- ensure_scalar_character(label_col)
  prediction_col <- ensure_scalar_character(prediction_col)
  metric_name <- rlang::arg_match(metric_name, c("rmse", "mse", "r2", "mae"))

  evaluator <- ml_new_identifiable(
    x, "org.apache.spark.ml.evaluation.RegressionEvaluator", uid) %>%
    invoke("setLabelCol", label_col) %>%
    invoke("setPredictionCol", prediction_col) %>%
    invoke("setMetricName", metric_name)  %>%
    new_ml_evaluator()

  evaluator
}

#' @export
ml_regression_evaluator.tbl_spark <- function(
  x, label_col = "label", prediction_col = "prediction", metric_name = "rmse",
  uid = random_string("regression_evaluator_"),
  ...) {
  sc <- spark_connection(x)
  evaluator <- ml_regression_evaluator(
    sc, label_col, prediction_col, metric_name)
  evaluator %>%
    ml_evaluate(x)
}

# Constructors

new_ml_evaluator <- function(jobj, ..., subclass = NULL) {
  structure(
    list(
      uid = invoke(jobj, "uid"),
      type = jobj_info(jobj)$class,
      param_map = ml_get_param_map(jobj),
      ...,
      .jobj = jobj
    ),
    class = c(subclass, "ml_evaluator")
  )
}

new_ml_binary_classification_evaluator <- function(jobj) {
  new_ml_evaluator(jobj, subclass = "ml_binary_classification_evaluator")
}

new_ml_multiclass_classification_evaluator <- function(jobj) {
  new_ml_evaluator(jobj, subclass = "ml_multiclass_classification_evaluator")
}

new_ml_regression_evaluator <- function(jobj) {
  new_ml_evaluator(jobj, subclass = "ml_regression_evaluator")
}

# Generic implementations

#' @export
spark_jobj.ml_evaluator <- function(x, ...) {
  x$.jobj
}

#' @export
print.ml_evaluator <- function(x, ...) {
  cat(ml_short_type(x), "(Evaluator) \n")
  cat(paste0("<", x$uid, ">"),"\n")
  ml_print_column_name_params(x)
  cat(" (Evaluation Metric)\n")
  cat(paste0("  ", "metric_name: ", ml_param(x, "metric_name")))
}

#' Spark ML -- Evaluate prediction frames with evaluators
#'
#' Evaluate a prediction dataset with a Spark ML evaluator
#'
#' @param x A \code{ml_evaluator} object.
#' @param dataset A \code{spark_tbl} with columns as specified in the evaluator object.
#' @export
ml_evaluate <- function(x, dataset) {
  x %>%
    spark_jobj() %>%
    invoke("evaluate", spark_dataframe(dataset))
}
