#' @export
ml_tokenizer <- function(...) {
  UseMethod("ml_tokenizer")
}

#' @export
ml_tokenizer.spark_connection <- function(sc, input_col, output_col, name = NULL, ...) {

  .stage <- ml_new_transformer(sc, "org.apache.spark.ml.feature.Tokenizer",
                               input_col, output_col)

  ml_pipeline(.stage, name)
}

#' @export
ml_binarizer <- function(...) {
  UseMethod("ml_binarizer")
}

#' @export
ml_binarizer.spark_connection <- function(sc, input_col, output_col, threshold,
                                          name = NULL, ...) {
  threshold <- ensure_scalar_double(threshold)
  .stage <- ml_new_transformer(sc, "org.apache.spark.ml.feature.Binarizer",
                               input_col, output_col) %>%
    invoke("setThreshold", threshold)

  ml_pipeline(.stage, name)
}

#' @export
ml_binarizer.tbl_spark <- function(x, input_col, output_col, threshold,
                                   name = NULL, ...) {
  sc <- spark_connection(x)
  sdf <- spark_dataframe(x)
  binarizer <- ml_binarizer(sc, input_col, output_col, threshold, name)
  binarizer$.pipeline %>%
    invoke("fit", sdf) %>%
    invoke("transform", sdf) %>%
    sdf_register()
}
