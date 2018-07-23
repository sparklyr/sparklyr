new_ml_model_one_vs_rest <- function(pipeline, pipeline_model, model,
                                     dataset, formula, feature_names,
                                     index_labels, call) {
  new_ml_model_classification(
    pipeline, pipeline_model,
    model, dataset, formula,
    subclass = "ml_model_one_vs_rest",
    .features = feature_names,
    .index_labels = index_labels
  )
}
