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
