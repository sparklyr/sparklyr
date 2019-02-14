new_ml_model_als <- function(pipeline_model,
                             formula,
                             dataset,
                             label_col,
                             features_col) {
 m <- new_ml_model_regression(
    pipeline_model, formula, dataset = dataset,
    label_col = label_col, features_col = features_col,
    class = "ml_model_als")

 m$features_col <- NULL
 m$label_col <- NULL
 m$`.jobj` <- spark_jobj(m)

 m
}
