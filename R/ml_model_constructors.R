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
sdf_predict.ml_model_classification <- function(
  object, newdata, predicted_label_col = "predicted_label",
  probability_prefix = "probability_", ...) {
  ensure_scalar_character(predicted_label_col)
  ensure_scalar_character(probability_prefix)

  if (missing(newdata) || is.null(newdata))
    newdata <- object$dataset

  cols <- object$model %>%
    ml_params(c("prediction_col", "probability_col", "raw_prediction_col"),
              allow_null = TRUE) %>%
    (function(x) Filter(length, x)) %>%
    unlist(use.names = FALSE)

  predictions <- object$pipeline_model %>%
    ml_transform(newdata) %>%
    select(!!!rlang::syms(c(tbl_vars(newdata), cols))) %>%
    ft_index_to_string(ml_param(object$model, "prediction_col"), predicted_label_col, labels = object$.index_labels)

  probability_col <- ml_param(object$model, "probability_col", allow_null = TRUE)
  if (rlang::is_null(probability_col))
    predictions
  else
    sdf_separate_column(
      predictions, probability_col,
      paste0(probability_prefix, spark_sanitize_names(object$.index_labels))
      )

}

#' @export
sdf_predict.ml_model_regression <- function(object, newdata, ...) {
  # when newdata is not supplied, attempt to use original dataset
  if (missing(newdata) || is.null(newdata))
    newdata <- object$dataset

  cols <- object$model %>%
    ml_params(c("prediction_col", "variance_col"),
              allow_null = TRUE) %>%
    (function(x) Filter(length, x)) %>%
    unlist(use.names = FALSE)

  object$pipeline_model %>%
    ml_transform(newdata) %>%
    select(!!!rlang::syms(c(tbl_vars(newdata), cols)))
}

#' @export
print.ml_model <- function(x, ...) {
  cat("Call: ", x$formula, "\n\n", sep = "")
  cat(invoke(spark_jobj(x$model), "toString"), sep = "\n")
}
