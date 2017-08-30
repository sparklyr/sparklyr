#' @export
ml_tokenizer <- function(x, input_col, output_col, uid = random_string("tokenizer_"), ...) {
  UseMethod("ml_tokenizer")
}

#' @export
ml_tokenizer.spark_connection <- function(x, input_col, output_col, uid = random_string("tokenizer_"), ...) {

  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.Tokenizer",
                             input_col, output_col, uid)

  ml_info(jobj)
}

#' @export
ml_tokenizer.ml_pipeline <- function(x, input_col, output_col, uid = random_string("tokenizer_"), ...) {

  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_add_stage(x, transformer)

}

#' @export
ml_tokenizer.tbl_spark <- function(x, input_col, output_col, uid = random_string("tokenizer_"), ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_transform(transformer, x)

}

#' @export
ml_binarizer <- function(x, input_col, output_col, threshold, uid = random_string("binarizer_"), ...) {
  UseMethod("ml_binarizer")
}

#' @export
ml_binarizer.spark_connection <- function(x, input_col, output_col, threshold,
                                          uid = random_string("binarizer_"), ...) {
  threshold <- ensure_scalar_double(threshold)
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.Binarizer",
                             input_col, output_col, uid) %>%
    invoke("setThreshold", threshold)

  ml_info(jobj)
}

#' @export
ml_binarizer.ml_pipeline <- function(x, input_col, output_col, threshold,
                                     uid = random_string("binarizer_"), ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_add_stage(x, transformer)
}

#' @export
ml_binarizer.tbl_spark <- function(x, input_col, output_col, threshold,
                                   uid = random_string("binarizer_"), ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_transform(transformer, x)
}

#' @export
ml_hashing_tf <- function(x, input_col, output_col, binary = FALSE,
                          num_features = 2^18, uid = random_string("hashing_tf_"), ...) {
  UseMethod("ml_hashing_tf")
}

#' @export
ml_hashing_tf.spark_connection <- function(x, input_col, output_col, binary = FALSE,
                                           num_features = 2^18, uid = random_string("hashing_tf_"), ...) {
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.HashingTF",
                             input_col, output_col, uid)

  ml_info(jobj)
}

#' @export
ml_hashing_tf.ml_pipeline <- function(x, input_col, output_col, binary = FALSE,
                                      num_features = 2^18, uid = random_string("hashing_tf_"), ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_add_stage(x, transformer)
}

#' @export
ml_hashing_tf.tbl_spark <- function(x, input_col, output_col, binary = FALSE,
                                    num_features = 2^18, uid = random_string("hashing_tf_"), ....) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_transform(transformer, x)

}
