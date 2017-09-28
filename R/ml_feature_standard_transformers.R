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
                             input_col, output_col, uid) %>%
    invoke("setBinary", binary) %>%
    invoke("setNumFeatures", num_features)

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

# IndexToString

#' @export
ft_index_to_string <- function(x, input_col, output_col, labels = NULL,
                               uid = random_string("index_to_string_"), ...) {
  UseMethod("ft_index_to_string")
}

#' @export
ft_index_to_string.spark_connection <- function(
  x, input_col, output_col, labels = NULL, uid = random_string("index_to_string_"), ...) {

  ml_validate_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.IndexToString",
                             input_col, output_col, uid)

  if (!rlang::is_null(labels))
    jobj <- invoke(jobj, "setLabels", labels)

  new_ml_transformer(jobj)
}

#' @export
ft_index_to_string.ml_pipeline <- function(
  x, input_col, output_col, labels = NULL,
  uid = random_string("index_to_string_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_index_to_string.tbl_spark <- function(
  x, input_col, output_col, labels = NULL,
  uid = random_string("index_to_string_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

# Bucketizer

#' @export
ft_bucketizer <- function(
  x, input_col, output_col, splits, handle_invalid = "error",
  uid = random_string("bucketizer_"), ...) {
  UseMethod("ft_bucketizer")
}

#' @export
ft_bucketizer.spark_connection <- function(
  x, input_col, output_col, splits, handle_invalid = "error",
  uid = random_string("bucketizer_"), ...) {

  ml_validate_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.Bucketizer",
                             input_col, output_col, uid) %>%
    invoke("setSplits", splits) %>%
    invoke("setHandleInvalid", handle_invalid)

  new_ml_transformer(jobj)
}

#' @export
ft_bucketizer.ml_pipeline <- function(
  x, input_col, output_col, splits, handle_invalid = "error",
  uid = random_string("bucketizer_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_bucketizer.tbl_spark <- function(
  x, input_col, output_col, splits, handle_invalid = "error",
  uid = random_string("bucketizer_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

# ElementwiseProduct

#' @export
ft_elementwise_product <- function(
  x, input_col, output_col, scaling_vec,
  uid = random_string("elementwise_product_"), ...) {
  if (spark_version(spark_connection(x)) < "2.0.0")
    stop("'ft_elementwise_product()' is only supported for Spark 2.0+")
  UseMethod("ft_elementwise_product")
}

#' @export
ft_elementwise_product.spark_connection <- function(
  x, input_col, output_col, scaling_vec,
  uid = random_string("elementwise_product_"), ...) {

  ml_validate_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.ElementwiseProduct",
                             input_col, output_col, uid) %>%
    (function(jobj) invoke_static(x,
                               "sparklyr.MLUtils2",
                               "setScalingVec",
                               jobj, scaling_vec))

  new_ml_transformer(jobj)
}

#' @export
ft_elementwise_product.ml_pipeline <- function(
  x, input_col, output_col, scaling_vec,
  uid = random_string("elementwise_product_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_elementwise_product.tbl_spark <- function(
  x, input_col, output_col, scaling_vec,
  uid = random_string("elementwise_product_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

# RegexTokenizer

#' @export
ft_regex_tokenizer <- function(
  x, input_col, output_col, gaps = TRUE,
  min_token_length = 1L, pattern = "\\s+", to_lower_case = TRUE,
  uid = random_string("regex_tokenizer_"), ...) {
  UseMethod("ft_regex_tokenizer")
}

#' @export
ft_regex_tokenizer.spark_connection <- function(
  x, input_col, output_col, gaps = TRUE,
  min_token_length = 1L, pattern = "\\s+", to_lower_case = TRUE,
  uid = random_string("regex_tokenizer_"), ...) {

  ml_validate_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.RegexTokenizer",
                             input_col, output_col, uid) %>%
    invoke("setGaps", gaps) %>%
    invoke("setMinTokenLength", min_token_length) %>%
    invoke("setPattern", pattern) %>%
    invoke("setToLowercase", to_lower_case)

  new_ml_transformer(jobj)
}

#' @export
ft_regex_tokenizer.ml_pipeline <- function(
  x, input_col, output_col, gaps = TRUE,
  min_token_length = 1L, pattern = "\\s+", to_lower_case = TRUE,
  uid = random_string("regex_tokenizer_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_regex_tokenizer.tbl_spark <- function(
  x, input_col, output_col, gaps = TRUE,
  min_token_length = 1L, pattern = "\\s+", to_lower_case = TRUE,
  uid = random_string("regex_tokenizer_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

# StopWordsRemover

#' @export
ft_default_stop_words <- function(
  sc, language = c("danish", "dutch", "english", "finnish",
                   "french", "german", "hungarian", "italian",
                   "norwegian", "portuguese", "russian", "spanish",
                   "swedish", "turkish"), ...) {
  language <- rlang::arg_match(language)
  invoke_static(sc, "org.apache.spark.ml.feature.StopWordsRemover",
                "loadDefaultStopWords", language)
}

#' @export
ft_stop_words_remover <- function(
  x, input_col, output_col, case_sensitive = FALSE,
  stop_words = ft_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"), ...) {
  UseMethod("ft_stop_words_remover")
}

#' @export
ft_stop_words_remover.spark_connection <- function(
  x, input_col, output_col, case_sensitive = FALSE,
  stop_words = ft_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"), ...) {

  ml_validate_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.StopWordsRemover",
                             input_col, output_col, uid) %>%
    invoke("setCaseSensitive", case_sensitive) %>%
    invoke("setStopWords", stop_words)

  new_ml_transformer(jobj)
}

#' @export
ft_stop_words_remover.ml_pipeline <- function(
  x, input_col, output_col, case_sensitive = FALSE,
  stop_words = ft_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_stop_words_remover.tbl_spark <- function(
  x, input_col, output_col, case_sensitive = FALSE,
  stop_words = ft_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}
