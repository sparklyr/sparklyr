ml_model_classification_pipeline <- function(predictor, dataset, formula, features_col, label_col) {
  sc <- spark_connection(predictor)
  r_formula <- ft_r_formula(sc, formula, features_col, label_col,
                            force_index_label = FALSE)
  pipeline_model <- ml_pipeline(r_formula, predictor) %>%
    ml_fit(dataset)
}

new_ml_model_classification <- function(pipeline_model, dataset, label_col,
                                        features_col, predicted_label_col, ...,
                                        class = character()) {

  m <- new_ml_model_prediction(
    pipeline_model,
    dataset = dataset,
    label_col = label_col,
    features_col = features_col,
    predicted_label_col = predicted_label_col,
    ...,
    class = c(class, "ml_model_classification")
  )

  label_indexer_model <- ml_stages(pipeline_model) %>%
    dplyr::nth(-2) # second from last, either RFormulaModel or StringIndexerModel
  index_labels <- ml_index_labels_metadata(label_indexer_model, dataset, label_col)

  if (!is.null(index_labels)) {
    index_to_string <- ft_index_to_string(
      sc, ml_param(m$model, "prediction_col"), predicted_label_col, index_labels)
    m$pipeline <- pipeline %>%
      ml_add_stage(index_to_string)
    m$pipeline_model <- pipeline_model %>%
      ml_add_stage(index_to_string) %>%
      # ml_fit() here doesn't do any actual computation but simply
      #   returns a PipelineModel since ml_add_stage() returns a
      #   Pipeline (Estimator)
      ml_fit(x)
  }

  m
}
