new_ml_model_isotonic_regression <- function(pipeline_model, formula, dataset, label_col,
                                             features_col) {
  new_ml_model_regression(
    pipeline_model, formula,
    dataset = dataset,
    label_col = label_col, features_col = features_col,
    class = "ml_model_isotonic_regression"
  )
}
