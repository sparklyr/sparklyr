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
new_ml_transformer <- function(jobj, ..., class = character()) {
  new_ml_pipeline_stage(
    jobj,
    ...,
    class = c(class, "ml_transformer")
  )
}

#' @rdname ml-constructors
#' @export
#' @keywords internal
new_ml_prediction_model <- function(jobj, ..., class = character()) {
  new_ml_transformer(
    jobj,
    features_col = invoke(jobj, "getFeaturesCol"),
    label_col = invoke(jobj, "getLabelCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    num_features = invoke(jobj, "numFeatures"),
    ...,
    class = c(class, "ml_prediction_model")
  )
}

#' @rdname ml-constructors
#' @export
#' @keywords internal
new_ml_classification_model <- function(jobj, ..., class = character()) {
  new_ml_prediction_model(
    jobj,
    num_classes = possibly_null(invoke)(jobj, "numClasses"),
    ...,
    class = c(class, "ml_classification_model")
  )
}

#' @rdname ml-constructors
#' @export
#' @keywords internal
new_ml_probabilistic_classification_model <- function(jobj, ..., class = character()) {
  new_ml_classification_model(
    jobj,
    probabilitiy_col = invoke(jobj, "getProbabilityCol"),
    thresholds = possibly_null(invoke)(jobj, "getThresholds"),
    ...,
    class = c(class, "ml_probabilistic_classification_model")
  )
}

#' @rdname ml-constructors
#' @export
#' @keywords internal
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
new_ml_predictor <- function(jobj, ..., class = character()) {
  new_ml_estimator(
    jobj,
    features_col = invoke(jobj, "getFeaturesCol"),
    label_col = invoke(jobj, "getLabelCol"),
    prediction_col = invoke(jobj, "getPredictionCol"),
    ...,
    class = c(class, "ml_predictor")
  )
}

#' @rdname ml-constructors
#' @export
#' @keywords internal
new_ml_classifier <- function(jobj, ..., class = character()) {
  new_ml_predictor(
    jobj,
    raw_prediction_col = invoke(jobj, "getRawPredictionCol"),
    ...,
    class = c(class, "ml_classifier")
  )
}

#' @rdname ml-constructors
#' @export
#' @keywords internal
new_ml_probabilistic_classifier <- function(jobj, ..., class = character()) {
  new_ml_classifier(
    jobj,
    probability_col = invoke(jobj, "getProbabilityCol"),
    thresholds = possibly_null(invoke)(jobj, "getThresholds"),
    ...,
    class = c(class, "ml_probabilistic_classifier")
  )
}
