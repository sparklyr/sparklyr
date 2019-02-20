new_ml_model_lda <- function(pipeline_model, formula, dataset,
                             features_col) {

  vocabulary <- pipeline_model %>%
    ml_stage("count_vectorizer") %>%
    ml_vocabulary()

  new_ml_model_clustering(
    pipeline_model = pipeline_model,
    formula = formula,
    dataset = dataset,
    features_col = features_col,
    vocabulary = vocabulary,
    class = "ml_model_lda"
  )
}
