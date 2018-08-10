new_ml_model_multilayer_perceptron_classification <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names,
  index_labels, call) {

  jobj <- spark_jobj(model)
  sc <- spark_connection(model)

  new_ml_model_classification(
    pipeline, pipeline_model, model, dataset, formula,
    subclass = "ml_model_multilayer_perceptron_classification",
    .features = feature_names,
    .index_labels = index_labels
  )
}
