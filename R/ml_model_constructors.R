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
