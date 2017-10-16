new_ml_transformer <- function(jobj, ..., subclass = NULL) {
  new_ml_pipeline_stage(jobj,
                        ...,
                        subclass = c(subclass, "ml_transformer"))
}

new_ml_prediction_model <- function(jobj, ..., subclass = NULL) {
  new_ml_transformer(jobj,
                     ...,
                     subclass = c(subclass, "ml_prediction_model"))
}

new_ml_clustering_model <- function(jobj, ..., subclass = NULL) {
  new_ml_transformer(jobj,
                     ...,
                     subclass = c(subclass, "ml_clustering_model"))
}

new_ml_estimator <- function(jobj, ..., subclass = NULL) {
  new_ml_pipeline_stage(jobj,
                        ...,
                        subclass = c(subclass, "ml_estimator"))
}

new_ml_predictor <- function(jobj, ..., subclass = NULL) {
  new_ml_estimator(jobj,
                   ...,
                   subclass = c(subclass, "ml_predictor"))
}

#' @export
print.ml_transformer <- function(x, ...) {
  cat(ml_short_type(x), "(Transformer) \n")
  cat(paste0("<", x$uid, ">"),"\n")
  for (param in names(ml_param_map(x)))
    cat("  ", param, ":", capture.output(str(ml_param(x, param))), "\n")
}

#' @export
print.ml_estimator <- function(x, ...) {
  cat(ml_short_type(x), "(Estimator) \n")
  cat(paste0("<", x$uid, ">"),"\n")
  for (param in names(ml_param_map(x)))
    cat("  ", param, ":", capture.output(str(ml_param(x, param))), "\n")
}

#' Spark ML -- Transformers and Estimators
#'
#' Transformer and Estimator objects.
#'
#' @name ml-pipelines
NULL

#' @rdname ml-pipelines
#' @param x An object.
#' @export
is_ml_transformer <- function(x) inherits(x, "ml_transformer")

#' @rdname ml-pipelines
#' @export
is_ml_estimator <- function(x) inherits(x, "ml_estimator")
