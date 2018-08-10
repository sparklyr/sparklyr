new_ml_model_gaussian_mixture <- function(pipeline, pipeline_model, model,
                                          dataset, formula, feature_names, call) {
  summary <- model$summary

  new_ml_model_clustering(
    pipeline, pipeline_model,
    model, dataset, formula,
    weights = model$weights,
    gaussians_df = model$gaussians_df(),
    summary = summary,
    subclass = "ml_model_gaussian_mixture",
    .features = feature_names
  )
}
