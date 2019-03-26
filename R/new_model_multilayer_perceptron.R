new_ml_model_multilayer_perceptron_classification <- function(pipeline_model, formula, dataset, label_col,
                                                      features_col, predicted_label_col) {
  new_ml_model_classification(
    pipeline_model, formula, dataset = dataset,
    label_col = label_col, features_col = features_col,
    predicted_label_col = predicted_label_col,
    class = "ml_model_multilayer_perceptron_classification"
  )
}
