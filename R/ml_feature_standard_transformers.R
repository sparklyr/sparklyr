# Tokenizer

#' @export
ft_tokenizer <- function(x, input_col, output_col, uid = random_string("tokenizer_"), ...) {
  UseMethod("ft_tokenizer")
}

#' @export
ft_tokenizer.spark_connection <- function(x, input_col, output_col, uid = random_string("tokenizer_"), ...) {

  ml_validate_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.Tokenizer",
                             input_col, output_col, uid)

  new_ml_transformer(jobj)
}

#' @export
ft_tokenizer.ml_pipeline <- function(x, input_col, output_col, uid = random_string("tokenizer_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_tokenizer.tbl_spark <- function(x, input_col, output_col, uid = random_string("tokenizer_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

# Binarizer

#' @export
ft_binarizer <- function(x, input_col, output_col, threshold = 0, uid = random_string("binarizer_"), ...) {
  UseMethod("ft_binarizer")
}

#' @export
ft_binarizer.spark_connection <- function(x, input_col, output_col, threshold = 0,
                                          uid = random_string("binarizer_"), ...) {

  ml_validate_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.Binarizer",
                             input_col, output_col, uid) %>%
    invoke("setThreshold", threshold)

  new_ml_transformer(jobj)
}

#' @export
ft_binarizer.ml_pipeline <- function(x, input_col, output_col, threshold = 0,
                                     uid = random_string("binarizer_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_binarizer.tbl_spark <- function(x, input_col, output_col, threshold = 0,
                                   uid = random_string("binarizer_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

# HashingTF

#' @export
ft_hashing_tf <- function(x, input_col, output_col, binary = FALSE,
                          num_features = 2^18, uid = random_string("hashing_tf_"), ...) {
  UseMethod("ft_hashing_tf")
}

#' @export
ft_hashing_tf.spark_connection <- function(x, input_col, output_col, binary = FALSE,
                                           num_features = 2^18, uid = random_string("hashing_tf_"), ...) {

  ml_validate_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.HashingTF",
                             input_col, output_col, uid)

  new_ml_transformer(jobj)
}

#' @export
ft_hashing_tf.ml_pipeline <- function(x, input_col, output_col, binary = FALSE,
                                      num_features = 2^18, uid = random_string("hashing_tf_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_hashing_tf.tbl_spark <- function(x, input_col, output_col, binary = FALSE,
                                    num_features = 2^18, uid = random_string("hashing_tf_"), ....) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)

}

# OneHotEncoder

#' @export
ft_one_hot_encoder <- function(
  x, input_col, output_col, drop_last = TRUE,
  uid = random_string("one_hot_encoder_"), ...) {
  UseMethod("ft_one_hot_encoder")
}

#' @export
ft_one_hot_encoder.spark_connection <- function(
  x, input_col, output_col, drop_last = TRUE,
  uid = random_string("one_hot_encoder_"), ...) {

  ml_validate_args()

  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.OneHotEncoder",
                             input_col, output_col, uid) %>%
    invoke("setDropLast", drop_last)

  new_ml_transformer(jobj)
}

#' @export
ft_one_hot_encoder.ml_pipeline <- function(
  x, input_col, output_col, drop_last = TRUE,
  uid = random_string("one_hot_encoder_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_one_hot_encoder.tbl_spark <- function(
  x, input_col, output_col, drop_last = TRUE,
  uid = random_string("one_hot_encoder_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

# VectorAssembler

#' @export
ft_vector_assembler <- function(
  x, input_cols, output_col,
  uid = random_string("vector_assembler_"), ...) {
  UseMethod("ft_vector_assembler")
}

#' @export
ft_vector_assembler.spark_connection <- function(
  x, input_cols, output_col,
  uid = random_string("vector_assembler_"), ...) {

  ml_validate_args()

  jobj <- invoke_new(x, "org.apache.spark.ml.feature.VectorAssembler", uid) %>%
    invoke("setInputCols", input_cols) %>%
    invoke("setOutputCol", output_col)

  new_ml_transformer(jobj)
}

#' @export
ft_vector_assembler.ml_pipeline <- function(
  x, input_cols, output_col,
  uid = random_string("vector_assembler_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_vector_assembler.tbl_spark <- function(
  x, input_cols, output_col,
  uid = random_string("vector_assembler_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

# DCT

#' @export
ft_dct <- function(x, input_col, output_col, inverse = FALSE, uid = random_string("dct_"), ...) {
  UseMethod("ft_dct")
}

#' @export
ft_dct.spark_connection <- function(x, input_col, output_col, inverse = FALSE, uid = random_string("dct_"), ...) {

  ml_validate_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.DCT",
                             input_col, output_col, uid) %>%
    invoke("setInverse", inverse)

  new_ml_transformer(jobj)
}

#' @export
ft_dct.ml_pipeline <- function(x, input_col, output_col, inverse = FALSE, uid = random_string("dct_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_dct.tbl_spark <- function(x, input_col, output_col, inverse = FALSE, uid = random_string("dct_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

#' @export
ft_discrete_cosine_transform <- ft_dct
