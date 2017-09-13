# Tokenizer

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

# Binarizer

#' @export
ml_binarizer <- function(x, input_col, output_col, threshold = 0, uid = random_string("binarizer_"), ...) {
  UseMethod("ml_binarizer")
}

#' @export
ml_binarizer.spark_connection <- function(x, input_col, output_col, threshold = 0,
                                          uid = random_string("binarizer_"), ...) {

  ml_validate_args(rlang::caller_env())
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.Binarizer",
                             input_col, output_col, uid) %>%
    invoke("setThreshold", threshold)

  ml_info(jobj)
}

#' @export
ml_binarizer.ml_pipeline <- function(x, input_col, output_col, threshold = 0,
                                     uid = random_string("binarizer_"), ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_add_stage(x, transformer)
}

#' @export
ml_binarizer.tbl_spark <- function(x, input_col, output_col, threshold = 0,
                                   uid = random_string("binarizer_"), ...) {
  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_transform(transformer, x)
}

# HashingTF

#' @export
ml_hashing_tf <- function(x, input_col, output_col, binary = FALSE,
                          num_features = 2^18, uid = random_string("hashing_tf_"), ...) {
  UseMethod("ml_hashing_tf")
}

#' @export
ml_hashing_tf.spark_connection <- function(x, input_col, output_col, binary = FALSE,
                                           num_features = 2^18, uid = random_string("hashing_tf_"), ...) {

  ml_validate_args(rlang::caller_env())
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

# OneHotEncoder

#' @export
ml_one_hot_encoder <- function(
  x, input_col, output_col, drop_last = TRUE,
  uid = random_string("one_hot_encoder_"), ...) {
  UseMethod("ml_one_hot_encoder")
}

#' @export
ml_one_hot_encoder.spark_connection <- function(
  x, input_col, output_col, drop_last = TRUE,
  uid = random_string("one_hot_encoder_"), ...) {

  ml_validate_args(rlang::caller_env())

  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.OneHotEncoder",
                             input_col, output_col, uid) %>%
    invoke("setDropLast", drop_last)

  ml_info(jobj)
}

#' @export
ml_one_hot_encoder.ml_pipeline <- function(
  x, input_col, output_col, drop_last = TRUE,
  uid = random_string("one_hot_encoder_"), ...) {

  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_add_stage(x, transformer)
}

#' @export
ml_one_hot_encoder.tbl_spark <- function(
  x, input_col, output_col, drop_last = TRUE,
  uid = random_string("one_hot_encoder_"), ...) {

  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_transform(transformer, x)
}

# VectorAssembler

#' @export
ml_vector_assembler <- function(
  x, input_cols, output_col,
  uid = random_string("vector_assembler_"), ...) {
  UseMethod("ml_vector_assembler")
}

#' @export
ml_vector_assembler.spark_connection <- function(
  x, input_cols, output_col,
  uid = random_string("vector_assembler_"), ...) {

  input_cols <- input_cols %>%
    lapply(ensure_scalar_character)
  ensure_scalar_character(output_col)
  ensure_scalar_character(uid)

  jobj <- invoke_new(x, "org.apache.spark.ml.feature.VectorAssembler", uid) %>%
    invoke("setInputCols", input_cols) %>%
    invoke("setOutputCol", output_col)

  ml_info(jobj)
}

#' @export
ml_vector_assembler.ml_pipeline <- function(
  x, input_cols, output_col,
  uid = random_string("vector_assembler_"), ...) {

  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_add_stage(x, transformer)
}

#' @export
ml_vector_assembler.tbl_spark <- function(
  x, input_cols, output_col,
  uid = random_string("vector_assembler_"), ...) {

  transformer <- ml_new_stage_modified_args(rlang::call_frame())
  ml_transform(transformer, x)
}
