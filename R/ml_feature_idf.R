#' Feature Transformation -- IDF (Estimator)
#'
#' Compute the Inverse Document Frequency (IDF) given a collection of documents.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @param min_doc_freq The minimum number of documents in which a term should appear. Default: 0
#'
#' @export
ft_idf <- function(x, input_col = NULL, output_col = NULL,
                   min_doc_freq = 0, dataset = NULL, uid = random_string("idf_"), ...) {
  UseMethod("ft_idf")
}

#' @export
ft_idf.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                    min_doc_freq = 0, dataset = NULL, uid = random_string("idf_"), ...) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    min_doc_freq = min_doc_freq,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    ml_validator_idf()

  estimator <- ml_new_transformer(
    x, "org.apache.spark.ml.feature.IDF",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]) %>%
    invoke("setMinDocFreq", .args[["min_doc_freq"]]) %>%
    new_ml_idf()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_idf.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                               min_doc_freq = 0, dataset = NULL, uid = random_string("idf_"), ...) {

  stage <- ft_idf.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    min_doc_freq = min_doc_freq,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)

}

#' @export
ft_idf.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                             min_doc_freq = 0, dataset = NULL, uid = random_string("idf_"), ...) {
  stage <- ft_idf.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    min_doc_freq = min_doc_freq,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

new_ml_idf <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_idf")
}

new_ml_idf_model <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_idf_model")
}

ml_validator_idf <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["min_doc_freq"]] <- cast_scalar_integer(.args[["min_doc_freq"]])
  .args
}
