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
ft_idf <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  min_doc_freq = 0,
  uid = random_string("idf_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_idf")
}

ml_idf <- ft_idf

ft_idf_impl <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  min_doc_freq = 0,
  uid = random_string("idf_"),
  ...
) {
  ml_process_feature(
    x = x,
    r_class = "ml_idf",
    uid = uid,
    stage_constructor = new_ml_idf,
    invoke_steps = list(
      input_col = input_col,
      output_col = output_col,
      min_doc_freq = min_doc_freq
    )
  )
}

#' @export
ft_idf.spark_connection <- ft_idf_impl

#' @export
ft_idf.ml_pipeline <- ft_idf_impl

#' @export
ft_idf.tbl_spark <- ft_idf_impl

new_ml_idf <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_idf")
}

new_ml_idf_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_idf_model")
}
