#' @export
ml_tokenizer <- function(x, input_col, output_col, uid = "tokenizer", ...) {
  UseMethod("ml_tokenizer")
}

#' @export
ml_tokenizer.spark_connection <- function(x, input_col, output_col, uid = "tokenizer", ...) {

  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.Tokenizer",
                             input_col, output_col, uid)

  ml_pipeline_stage_info(jobj)
}

#' @export
ml_tokenizer.ml_pipeline <- function(x, input_col, output_col, uid = "tokenizer", ...) {

  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_add_stage(x, transformer)

}

#' @export
ml_tokenizer.tbl_spark <- function(x, input_col, output_col, uid = "tokenizer", ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_transform(transformer, x)

}

#' @export
ml_binarizer <- function(x, input_col, output_col, threshold, uid = "binarizer", ...) {
  UseMethod("ml_binarizer")
}

#' @export
ml_binarizer.spark_connection <- function(x, input_col, output_col, threshold,
                                          uid = "binarizer", ...) {
  threshold <- ensure_scalar_double(threshold)
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.Binarizer",
                             input_col, output_col, uid) %>%
    invoke("setThreshold", threshold)

  ml_pipeline_stage_info(jobj)
}

#' @export
ml_binarizer.ml_pipeline <- function(x, input_col, output_col, threshold,
                                     uid = "binarizer", ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_add_stage(x, transformer)
}

#' @export
ml_binarizer.tbl_spark <- function(x, input_col, output_col, threshold,
                                   uid = "binarizer", ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_transform(transformer, x)
}

#' @export
ml_hashing_tf <- function(x, input_col, output_col, binary = FALSE,
                          num_features = 2^18, uid = "hashing_tf", ...) {
  UseMethod("ml_hashing_tf")
}

#' @export
ml_hashing_tf.spark_connection <- function(x, input_col, output_col, binary = FALSE,
                                           num_features = 2^18, uid = "hashing_tf", ...) {
  ensure_scalar_boolean(binary)
  ensure_scalar_character(uid)
  num_features <- ensure_scalar_integer(num_features)
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.HashingTF",
                             input_col, output_col, uid) %>%
    invoke("setBinary", binary) %>%
    invoke("setNumFeatures", num_features)

  ml_pipeline_stage_info(jobj)
}

#' @export
ml_hashing_tf.ml_pipeline <- function(x, input_col, output_col, binary = FALSE,
                                      num_features = 2^18, uid = "hashing_tf", ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_stages(x, transformer)
}

#' @export
ml_hashing_tf.tbl_spark <- function(x, input_col, output_col, binary = FALSE,
                                    num_features = 2^18, uid = "hashing_tf", ....) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_transform(transformer, x)

}
