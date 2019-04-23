#' Evaluate the Model on a Validation Set
#'
#' Compute performance metrics.
#'
#' @param x An ML model object or an evaluator object.
#' @param dataset The dataset to be validate the model on.
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
