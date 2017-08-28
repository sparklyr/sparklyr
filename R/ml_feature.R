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
ml_tokenizer.ml_pipeline <- function(x, input_col, output_col, ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_stages(x, transformer)
}

#' @export
ml_tokenizer.tbl_spark <- function(x, input_col, output_col, ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_fit_and_transform(x, transformer)

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
ml_binarizer.ml_pipeline <- function(x, input_col, output_col, threshold, ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_stages(x, transformer)
}

#' @export
ml_binarizer.tbl_spark <- function(x, input_col, output_col, threshold, ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_fit_and_transform(x, transformer)
}
