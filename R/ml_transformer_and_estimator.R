#' Spark ML -- Transformers and Estimators
#'
#' Methods for Spark ML Estimators and Transformers
#'
#' @param x An \code{ml_pipeline_stage} object.
#' @param dataset A \code{tbl_spark}.
#' @template roxlate-ml-dots
#'
#' @name ml-estimators-transformers
NULL

#' @rdname ml-estimators-transformers
#' @export
is_ml_transformer <- function(x) inherits(x, "ml_transformer")

#' @rdname ml-estimators-transformers
#' @export
is_ml_estimator <- function(x) inherits(x, "ml_estimator")

#' @rdname ml-estimators-transformers
#' @export
ml_fit <- function(x, dataset, ...) {
  if (!is_ml_estimator(x))
    stop("'ml_fit()' is only applicable to 'ml_estimator' objects")

  spark_jobj(x) %>%
    invoke("fit", spark_dataframe(dataset)) %>%
    ml_constructor_dispatch()
}

#' @rdname ml-estimators-transformers
#' @export
ml_transform <- function(x, dataset, ...) {
  if (!is_ml_transformer(x))
    stop("'ml_transform()' is only applicable to 'ml_transformer' objects")
  sdf <- spark_dataframe(dataset)
  spark_jobj(x) %>%
    invoke("transform", sdf) %>%
    sdf_register()
}

#' @rdname ml-estimators-transformers
#' @export
ml_fit_and_transform <- function(x, dataset, ...) {
  if (!is_ml_estimator(x))
    stop("'ml_fit_and_transform()' is only applicable to 'ml_estimator' objects")
  sdf <- spark_dataframe(dataset)
  spark_jobj(x)%>%
    invoke("fit", sdf) %>%
    invoke("transform", sdf) %>%
    sdf_register()
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

