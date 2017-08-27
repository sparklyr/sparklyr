#' @export
ml_tokenizer <- function(x, input_col, output_col, ...) {
  UseMethod("ml_tokenizer")
}

#' @export
ml_tokenizer.spark_connection <- function(x, input_col, output_col, ...) {

  .stage <- ml_new_transformer(x, "org.apache.spark.ml.feature.Tokenizer",
                               input_col, output_col)

  ml_pipeline(.stage)
}

#' @export
ml_tokenizer.tbl_spark <- function(x, input_col, output_col, ...) {
  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)
  tokenizer <- ml_tokenizer(sc, input_col, output_col)
  ml_fit_and_transform(sdf, tokenizer)
}

#' @export
ml_binarizer <- function(x, input_col, output_col, threshold, ...) {
  UseMethod("ml_binarizer")
}

#' @export
ml_binarizer.spark_connection <- function(x, input_col, output_col, threshold, ...) {
  threshold <- ensure_scalar_double(threshold)
  .stage <- ml_new_transformer(x, "org.apache.spark.ml.feature.Binarizer",
                               input_col, output_col) %>%
    invoke("setThreshold", threshold)

  ml_pipeline(.stage)
}

#' @export
ml_binarizer.tbl_spark <- function(x, input_col, output_col, threshold, ...) {
  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)
  binarizer <- ml_binarizer(sc, input_col, output_col, threshold)
  ml_fit_and_transform(sdf, binarizer)
}
