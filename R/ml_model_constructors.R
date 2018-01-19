new_ml_model <- function(
  pipeline, pipeline_model, model, ..., subclass = NULL) {

  structure(
    list(
      pipeline = pipeline,
      pipeline_model = pipeline_model,
      model = model,
      ...
    ),
    class = c(subclass, "ml_model")
  )
}

new_ml_model_prediction <- function(
  pipeline, pipeline_model, model, dataset, formula, ...,
  subclass = NULL) {
  new_ml_model(
    pipeline = pipeline,
    pipeline_model = pipeline_model,
    model = model,
    dataset = dataset,
    formula = formula,
    .response = gsub("~.+$", "", formula) %>% trimws(),
    ...,
    subclass = c(subclass, "ml_model_prediction"))
}

new_ml_model_classification <- function(
  pipeline, pipeline_model,
  model, dataset, formula, ..., subclass = NULL) {

  # workaround for partial matching of `pi` to `pipeline` in
  #   ml_naive_bayes()
  do.call(new_ml_model_prediction,
          rlang::ll(pipeline = pipeline,
                    pipeline_model = pipeline_model,
                    model = model,
                    dataset = dataset,
                    formula = formula,
                    !!! rlang::dots_list(...),
                    subclass = c(subclass, "ml_model_classification")))
}

new_ml_model_regression <- function(
  pipeline, pipeline_model,
  model, dataset, formula, ..., subclass = NULL) {
  new_ml_model_prediction(
    pipeline = pipeline,
    pipeline_model = pipeline_model,
    model = model,
    dataset = dataset,
    formula = formula,
    ...,
    subclass = c(subclass, "ml_model_regression"))
}

new_ml_model_clustering <- function(
  pipeline, pipeline_model, model, dataset, formula, ...,
  subclass = NULL) {
  new_ml_model(
    pipeline = pipeline,
    pipeline_model = pipeline_model,
    model = model,
    dataset = dataset,
    formula = formula,
    ...,
    subclass = c(subclass, "ml_model_clustering"))
}

#' @export
spark_jobj.ml_model <- function(x, ...) {
  spark_jobj(x$pipeline_model)
}

#' @export
print.ml_model <- function(x, ...) {
  ml_model_print_call(x)
  print_newline()
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat(invoke(spark_jobj(x$model), "toString"), sep = "\n")
}
