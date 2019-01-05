new_ml_model_lda <- function(pipeline_model, formula, dataset,
                             features_col) {

  vocabulary <- pipeline_model$stages[[2]]$vocabulary

  new_ml_model_clustering(
    pipeline_model = pipeline_model,
    formula = formula,
    dataset = dataset,
    features_col = features_col,
    vocabulary = vocabulary,
    class = "ml_model_lda"
  )
}
