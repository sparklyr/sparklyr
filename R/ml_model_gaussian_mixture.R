new_ml_model_gaussian_mixture <- function(pipeline_model, formula, dataset,
                                          features_col) {
  m <- new_ml_model_clustering(
    pipeline_model = pipeline_model,
    formula = formula,
    dataset = dataset,
    features_col = features_col,
    class = "ml_model_gaussian_mixture"
  )

  model <- m$model

  m$weights <- model$weights
  m$gaussians_df <- model$gaussians_df
  m$summary <- model$summary

  m
}
