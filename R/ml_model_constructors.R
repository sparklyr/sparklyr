new_ml_model <- function(
  pipeline, pipeline_model, model, ..., subclass = NULL) {

  structure(
    list(
      pipeline = pipeline,
      pipeline_model = pipeline_model,
      model = model,
      ...
    ),
    class = c(subclass, "ml_model")
  )
}

new_ml_model_prediction <- function(
  pipeline, pipeline_model, model, dataset, formula, ...,
  subclass = NULL) {
  new_ml_model(
    pipeline = pipeline,
    pipeline_model = pipeline_model,
    model = model,
    dataset = dataset,
    formula = formula,
    ...,
    subclass = c(subclass, "ml_model_prediction"))
}

new_ml_model_classification <- function(
  pipeline, pipeline_model,
  model, dataset, formula, ..., subclass = NULL) {
  new_ml_model_prediction(
    pipeline = pipeline,
    pipeline_model = pipeline_model,
    model = model,
    dataset = dataset,
    formula = formula,
    ...,
    subclass = c(subclass, "ml_model_classification"))
}

new_ml_model_regression <- function(
  pipeline, pipeline_model,
  model, dataset, formula, ..., subclass = NULL) {
  new_ml_model_prediction(
    pipeline = pipeline,
    pipeline_model = pipeline_model,
    model = model,
    dataset = dataset,
    formula = formula,
    ...,
    subclass = c(subclass, "ml_model_regression"))
}

#' @export
sdf_predict.ml_model_classification <- function(object, newdata, ...) {
  if (missing(newdata) || is.null(newdata))
    newdata <- object$dataset

  object$pipeline_model %>%
    ml_transform(newdata) %>%
    select(!!!rlang::syms(c(tbl_vars(newdata), "prediction")))
}

#' @export
sdf_predict.ml_model_regression <- function(object, newdata, ...) {
  # when newdata is not supplied, attempt to use original dataset
  if (missing(newdata) || is.null(newdata))
    newdata <- object$dataset

  object$pipeline_model %>%
    ml_transform(newdata) %>%
    select(!!!rlang::syms(c(tbl_vars(newdata), "prediction")))
}

#' @export
print.ml_model <- function(x, ...) {
  cat("Call: ", x$formula, "\n\n", sep = "")
  cat(invoke(spark_jobj(x$model), "toString"), sep = "\n")
}
