#' Evaluate the Model on a Validation Set
#'
#' Compute performance metrics.
#'
#' @param x An ML model object or an evaluator object.
#' @param dataset The dataset to be validate the model on.
#' @examples
#' \dontrun{
#' sc <- spark_connect(master = "local")
#' iris_tbl <- sdf_copy_to(sc, iris, name = "iris_tbl", overwrite = TRUE)
#'
#' ml_gaussian_mixture(iris_tbl, Species ~ .) %>%
#'   ml_evaluate(iris_tbl)
#'
#' ml_kmeans(iris_tbl, Species ~ .) %>%
#'   ml_evaluate(iris_tbl)
#'
#' ml_bisecting_kmeans(iris_tbl, Species ~ .) %>%
#'   ml_evaluate(iris_tbl)
#' }
#'
#' @export
ml_evaluate <- function(x, dataset) {
  UseMethod("ml_evaluate")
}

ml_evaluate.default <- function(x, dataset) {
  stop("`ml_evaluate()` is not supported for `", class(x)[[1]], "`.", call. = FALSE)
}

#' @rdname ml_evaluate
#' @export
ml_evaluate.ml_model_logistic_regression <- function(x, dataset) {
  evaluate_ml_model(x, dataset) %>%
    logreg_summary()
}

#' @rdname ml_evaluate
#' @export
ml_evaluate.ml_logistic_regression_model <- function(x, dataset) {
  evaluate_ml_transformer(x, dataset) %>%
    logreg_summary()
}

#' @rdname ml_evaluate
#' @export
ml_evaluate.ml_model_linear_regression <- function(x, dataset) {
  fit_intercept <- ml_param(x$model, "fit_intercept")
  evaluate_ml_model(x, dataset) %>%
    new_ml_linear_regression_summary(fit_intercept = fit_intercept)
}

#' @rdname ml_evaluate
#' @export
ml_evaluate.ml_linear_regression_model <- function(x, dataset) {
  fit_intercept <- ml_param(x, "fit_intercept")
  evaluate_ml_transformer(x, dataset) %>%
    new_ml_linear_regression_summary(fit_intercept = fit_intercept)
}

#' @rdname ml_evaluate
#' @export
ml_evaluate.ml_model_generalized_linear_regression <- function(x, dataset) {
  fit_intercept <- ml_param(x$model, "fit_intercept")
  evaluate_ml_model(x, dataset) %>%
    new_ml_generalized_linear_regression_summary(fit_intercept = fit_intercept)
}

#' @rdname ml_evaluate
#' @export
ml_evaluate.ml_generalized_linear_regression_model <- function(x, dataset) {
  fit_intercept <- ml_param(x, "fit_intercept")
  evaluate_ml_transformer(x, dataset) %>%
    new_ml_generalized_linear_regression_summary(fit_intercept = fit_intercept)
}

#' @rdname ml_evaluate
#' @export
ml_evaluate.ml_model_clustering <- function(x, dataset) {
  if(inherits(x, "ml_model_lda")){
    stop("`ml_evaluate()` is not supported for `ml_model_lda`.", call. = FALSE)
  }

  sc <- spark_connection(x$model)

  if(spark_version(sc) >= "2.3.0"){
    prediction <- x %>%
      spark_jobj() %>%
      invoke("transform", spark_dataframe(dataset))

    silhouette <- sc %>%
      invoke_new("org.apache.spark.ml.evaluation.ClusteringEvaluator") %>%
      invoke("evaluate", prediction)

    dplyr::tibble(Silhouette = silhouette)
  } else{
    stop("Silhouette is only available for spark 2.3.0 or greater.")
  }
}

#' @rdname ml_evaluate
#' @export
ml_evaluate.ml_model_classification <- function(x, dataset) {
  sc <- spark_connection(x$model)

  prediction <- x %>%
    spark_jobj() %>%
    invoke("transform", spark_dataframe(dataset))

  accuracy <- sc %>%
    invoke_new("org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator") %>%
    invoke("evaluate", prediction)

  dplyr::tibble(Accuracy = accuracy)
}

evaluate_ml_model <- function(x, dataset) {
  dataset <- x$pipeline_model %>%
    ml_stage(1) %>%
    ml_transform(dataset)

  evaluate_ml_transformer(x$model, dataset)
}

evaluate_ml_transformer <- function(x, dataset) {
  x %>%
    spark_jobj() %>%
    invoke("evaluate", spark_dataframe(dataset))
}

logreg_summary <- function(jobj) tryCatch(
  new_ml_binary_logistic_regression_summary(invoke(jobj, "asBinary")),
  error = function(e) new_ml_logistic_regression_summary(jobj)
)
