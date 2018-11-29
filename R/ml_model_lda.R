new_ml_model_lda <- function(pipeline, pipeline_model, model,
                                dataset, formula, feature_names,
                                call) {

  vocabulary <- pipeline_model$stages[[2]]$vocabulary

  new_ml_model_clustering(
    pipeline, pipeline_model,
    model, dataset, formula,
    vocabulary = vocabulary,
    subclass = "ml_model_lda"
  )
}
