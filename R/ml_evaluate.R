#' Evaluate the Model on a Validation Set
#'
#' Create an \code{ml_summary} object with performance metrics.
#'
#' @param x An ML model object.
#' @param dataset The dataset to be validate the model on.
#' @name ml_evaluate

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
  evaluate_ml_model(x, dataset) %>%
    new_ml_linear_regression_summary()
}

#' @rdname ml_evaluate
#' @export
ml_evaluate.ml_linear_regression_model <- function(x, dataset) {
  evaluate_ml_transformer(x, dataset) %>%
    new_ml_linear_regression_summary()
}

#' @rdname ml_evaluate
#' @export
ml_evaluate.ml_model_generalized_linear_regression <- function(x, dataset) {
  evaluate_ml_model(x, dataset) %>%
    new_ml_generalized_linear_regression_summary()
}

#' @rdname ml_evaluate
#' @export
ml_evaluate.ml_generalized_linear_regression_model <- function(x, dataset) {
  evaluate_ml_transformer(x, dataset) %>%
    new_ml_generalized_linear_regression_summary()
}

evaluate_ml_model <- function(x, dataset) {
  sdf <- x$pipeline_model %>%
    ml_stage(1) %>%
    ml_transform(dataset) %>%
    spark_dataframe()

  x$model %>%
    spark_jobj() %>%
    invoke("evaluate", sdf)
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
