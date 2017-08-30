#' @export
ml_tokenizer <- function(x, input_col, output_col, uid, ...) {
  UseMethod("ml_tokenizer")
}

#' @export
ml_tokenizer.spark_connection <- function(x, input_col, output_col, uid = "tokenizer", ...) {

  .jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.Tokenizer",
                               input_col, output_col, uid)

  ml_pipeline_stage(.jobj)
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

#' @export
ml_hashing_tf <- function(x, input_col, output_col, binary = FALSE,
                          num_features = 2^18, ...) {
  UseMethod("ml_hashing_tf")
}

#' @export
ml_hashing_tf.spark_connection <- function(x, input_col, output_col, binary = FALSE,
                                           num_features = 2^18, ...) {
  ensure_scalar_boolean(binary)
  num_features <- ensure_scalar_integer(num_features)
  .stage <- ml_new_transformer(x, "org.apache.spark.ml.feature.HashingTF",
                               input_col, output_col) %>%
    invoke("setBinary", binary) %>%
    invoke("setNumFeatures", num_features)

  ml_pipeline(.stage)
}

#' @export
ml_hashing_tf.ml_pipeline <- function(x, input_col, output_col, binary = FALSE,
                                      num_features = 2^18, ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_stages(x, transformer)
}

#' @export
ml_hashing_tf.tbl_spark <- function(x, input_col, output_col, binary = FALSE,
                                    num_features = 2^18, ....) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_fit_and_transform(x, transformer)

}
