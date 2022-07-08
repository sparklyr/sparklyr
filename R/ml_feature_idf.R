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
                   min_doc_freq = 0, uid = random_string("idf_"), ...) {
  check_dots_used()
  UseMethod("ft_idf")
}

ft_idf_impl <- function(x, input_col = NULL, output_col = NULL,
                        min_doc_freq = 0, uid = random_string("idf_"), ...) {
  estimator_process(
    x = x,
    uid = uid,
    spark_class = "org.apache.spark.ml.feature.IDF",
    r_class = "ml_idf_model",
    invoke_steps = list(
      setInputCol = cast_nullable_string(input_col),
      setOutputCol = cast_nullable_string(output_col),
      setMinDocFreq = cast_scalar_integer(min_doc_freq)
    )
  )
}

ml_idf <- ft_idf

#' @export
ft_idf.spark_connection <- ft_idf_impl

#' @export
ft_idf.ml_pipeline <- ft_idf_impl

#' @export
ft_idf.tbl_spark <- ft_idf_impl

new_ml_idf_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_idf_model")
}
