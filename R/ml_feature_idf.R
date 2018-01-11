#' Feature Tranformation -- IDF (Estimator)
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
  x, input_col, output_col,
  min_doc_freq = 0L, dataset = NULL,
  uid = random_string("idf_"), ...) {
  UseMethod("ft_idf")
}

#' @export
ft_idf.spark_connection <- function(
  x, input_col, output_col,
  min_doc_freq = 0L, dataset = NULL,
  uid = random_string("idf_"), ...) {

  ml_ratify_args()

  estimator <- ml_new_transformer(x, "org.apache.spark.ml.feature.IDF",
                                  input_col, output_col, uid) %>%
    invoke("setMinDocFreq", min_doc_freq) %>%
    new_ml_idf()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_idf.ml_pipeline <- function(
  x, input_col, output_col,
  min_doc_freq = 0L, dataset = NULL,
  uid = random_string("idf_"), ...
) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)

}

#' @export
ft_idf.tbl_spark <- function(
  x, input_col, output_col,
  min_doc_freq = 0L, dataset = NULL,
  uid = random_string("idf_"), ...
) {
  dots <- rlang::dots_list(...)

  stage <- ml_new_stage_modified_args()

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

ml_validator_idf <- function(args, nms) {
  args %>%
    ml_validate_args({
      min_doc_freq <- ensure_scalar_integer(min_doc_freq)
    }) %>%
    ml_extract_args(nms)
}
