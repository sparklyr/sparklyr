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

new_ml_transformer <- function(jobj, ..., class = character()) {
  new_ml_pipeline_stage(
    jobj,
    ...,
    class = c(class, "ml_transformer")
  )
}

#' Constructors for Pipeline Stages
#'
#' Functions for developers writing extensions for Spark ML.
#'
#' @param jobj Pointer to the pipeline stage object.
#' @param class Name of class.
#' @param ... (Optional) additional attributes of the object.
#'
#' @name ml-constructors
#'
#' @export
#' @keywords internal
ml_transformer <- new_ml_transformer

new_ml_prediction_model <- function(jobj, ..., class = character()) {
  new_ml_transformer(
    jobj,
    ...,
    class = c(class, "ml_prediction_model")
  )
}

#' @rdname ml-constructors
#' @export
#' @keywords internal
ml_prediction_model <- new_ml_prediction_model

new_ml_clustering_model <- function(jobj, ..., class = character()) {
  new_ml_transformer(
    jobj,
    ...,
    class = c(class, "ml_clustering_model")
  )
}

#' @rdname ml-constructors
#' @export
#' @keywords internal
ml_clustering_model <- new_ml_clustering_model

new_ml_estimator <- function(jobj, ..., class = character()) {
  new_ml_pipeline_stage(
    jobj,
    ...,
    class = c(class, "ml_estimator")
  )
}

#' @rdname ml-constructors
#' @export
#' @keywords internal
ml_estimator <- new_ml_estimator

new_ml_classifier <- function(jobj, ..., class = character()) {
  new_ml_estimator(
    jobj,
    ...,
    class = c(class, "ml_classifier")
  )
}

#' @rdname ml-constructors
#' @export
#' @keywords internal
ml_classifier <- new_ml_classifier
