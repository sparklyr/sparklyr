new_ml_model_als <- function(pipeline_model,
                             formula,
                             dataset) {

 m <- new_ml_model_recommendation(
    pipeline_model, formula, dataset = dataset,
    class = "ml_model_als")

 m$`.jobj` <- spark_jobj(m)

 m
}
