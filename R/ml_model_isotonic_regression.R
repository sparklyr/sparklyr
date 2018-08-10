new_ml_model_isotonic_regression <- function(pipeline, pipeline_model, model,
                                             dataset, formula,
                                             feature_names, call) {
  new_ml_model_regression(
    pipeline, pipeline_model, model, dataset, formula,
    subclass = "ml_model_isotonic_regression",
    .features = feature_names
  )
}
