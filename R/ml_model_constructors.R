new_ml_model <- function(pipeline, pipeline_model, model_uid, ..., subclass = NULL) {
  model_params <- pipeline_model %>%
    ml_stages() %>%
    `[[`(grep(model_uid, pipeline_model$stage_uids)) %>%
    `[[`("param_map")

  structure(
    list(
      pipeline = pipeline,
      pipeline_model = pipeline_model,
      model_params = model_params,
      ...
    ),
    class = c(subclass, "ml_model")
  )
}

new_ml_model_prediction <- function(pipeline, pipeline_model, model_uid, formula, ...,
                                    subclass = NULL) {
  new_ml_model(
    pipeline, pipeline_model, model_uid,
    formula = formula,
    ...,
    subclass = c(subclass, "ml_model_prediction"))
}

new_ml_model_classification <- function(pipeline, pipeline_model, model_uid, formula, dataset, ..., subclass = NULL) {
  new_ml_model_prediction(
    pipeline, pipeline_model, model_uid, formula,
    dataset = dataset,
    ...,
    subclass = c(subclass, "ml_model_classification"))
}

new_ml_model_regression <- function(pipeline, pipeline_model, model_uid, formula, dataset, ..., subclass = NULL) {
  new_ml_model_prediction(
    pipeline, pipeline_model, model_uid, formula,
    dataset = dataset,
    ...,
    subclass = c(subclass, "ml_model_regression"))
}
