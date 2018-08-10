#' @export
print.ml_transformer <- function(x, ...) {
  ml_print_class(x)
  ml_print_uid(x)
  ml_print_column_name_params(x)
  ml_print_transformer_info(x)
}

#' @export
print.ml_estimator <- function(x, ...) {
  ml_print_class(x)
  ml_print_uid(x)
  ml_print_column_name_params(x)
  ml_print_params(x)
}

new_ml_transformer <- function(jobj, ..., subclass = NULL) {
  new_ml_pipeline_stage(
    jobj,
    ...,
    subclass = c(subclass, "ml_transformer")
  )
}

new_ml_prediction_model <- function(jobj, ..., subclass = NULL) {
  new_ml_transformer(
    jobj,
    ...,
    subclass = c(subclass, "ml_prediction_model")
  )
}

new_ml_clustering_model <- function(jobj, ..., subclass = NULL) {
  new_ml_transformer(
    jobj,
    ...,
    subclass = c(subclass, "ml_clustering_model")
  )
}

new_ml_estimator <- function(jobj, ..., subclass = NULL) {
  new_ml_pipeline_stage(
    jobj,
    ...,
    subclass = c(subclass, "ml_estimator")
  )
}

new_ml_predictor <- function(jobj, ..., subclass = NULL) {
  new_ml_estimator(
    jobj,
    ...,
    subclass = c(subclass, "ml_predictor")
  )
}

new_ml_classifier <- function(jobj, ..., subclass = NULL) {
  new_ml_predictor(
    jobj,
    ...,
    subclass = c(subclass, "ml_classifier")
  )
}
