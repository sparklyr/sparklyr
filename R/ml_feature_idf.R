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

ml_idf <- ft_idf

#' @export
ft_idf.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                    min_doc_freq = 0, uid = random_string("idf_"), ...) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    min_doc_freq = min_doc_freq,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_idf()

  estimator <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.IDF",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    invoke("setMinDocFreq", .args[["min_doc_freq"]]) %>%
    new_ml_idf()

  estimator
}

#' @export
ft_idf.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                               min_doc_freq = 0, uid = random_string("idf_"), ...) {
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
                             min_doc_freq = 0, uid = random_string("idf_"), ...) {
  stage <- ft_idf.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    min_doc_freq = min_doc_freq,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_idf <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_idf")
}

new_ml_idf_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_idf_model")
}

validator_ml_idf <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["min_doc_freq"]] <- cast_scalar_integer(.args[["min_doc_freq"]])
  .args
}
