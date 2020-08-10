new_ml_model_one_vs_rest <- function(pipeline_model, formula, dataset, label_col,
                                     features_col, predicted_label_col) {
  m <- new_ml_model_classification(
    pipeline_model, formula,
    dataset = dataset,
    label_col = label_col, features_col = features_col,
    predicted_label_col = predicted_label_col,
    class = "ml_model_one_vs_rest"
  )

  m
}
