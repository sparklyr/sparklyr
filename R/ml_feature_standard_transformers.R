# Tokenizer

#' Feature Tranformation -- Tokenizer
#'
#' A tokenizer that converts the input string to lowercase and then splits it
#' by white spaces.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
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

#' Feature Transformation -- Binarizer
#'
#' Apply thresholding to a column, such that values less than or equal to the
#' \code{threshold} are assigned the value 0.0, and values greater than the
#' threshold are assigned the value 1.0. Column output is numeric for
#' compatibility with other modeling functions.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param threshold Threshold used to binarize continuous features.
#'
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

#' Feature Transformation -- OneHotEncoder
#'
#' One-hot encoding maps a column of label indices to a column of binary
#' vectors, with at most a single one-value. This encoding allows algorithms
#' which expect continuous features, such as Logistic Regression, to use
#' categorical features. Typically, used with  \code{ft_string_indexer()} to
#' index a column first.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @param drop_last Whether to drop the last category. Defaults to \code{TRUE}.
#'
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

#' Feature Transformation -- VectorAssembler
#'
#' Combine multiple vectors into a single row-vector; that is,
#' where each row element of the newly generated column is a
#' vector formed by concatenating each row element from the
#' specified input columns.
#'
#' @param input_cols The names of the input columns
#' @param output_col The name of the output column.
#' @template roxlate-ml-feature-transformer
#'
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

#' Feature Transformation -- Discrete Cosine Transform (DCT)
#'
#' A feature transformer that takes the 1D discrete cosine transform of a real
#'   vector. No zero padding is performed on the input vector. It returns a real
#'   vector of the same length representing the DCT. The return vector is scaled
#'   such that the transform matrix is unitary (aka scaled DCT-II).
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param inverse Indicates whether to perform the inverse DCT (TRUE) or forward DCT (FALSE).
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

#' Feature Transformation -- IndexToString
#'
#' A Transformer that maps a column of indices back to a new column of
#'   corresponding string values. The index-string mapping is either from
#'   the ML attributes of the input column, or from user-supplied labels
#'    (which take precedence over ML attributes).
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param labels Optional param for array of labels specifying index-string mapping.
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

#' Feature Transformation -- Bucketizer
#'
#' Similar to \R's \code{\link{cut}} function, this transforms a numeric column
#' into a discretized column, with breaks specified through the \code{splits}
#' parameter.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-handle-invalid
#'
#' @param splits A numeric vector of cutpoints, indicating the bucket boundaries.
#'
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

#' Feature Transformation -- ElementwiseProduct
#'
#' Outputs the Hadamard product (i.e., the element-wise product) of each input vector
#'   with a provided "weight" vector. In other words, it scales each column of the
#'   dataset by a scalar multiplier.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @param scaling_vec the vector to multiply with input vectors
#'
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

#' Feature Tranformation -- RegexTokenizer
#'
#' A regex based tokenizer that extracts tokens either by using the provided
#' regex pattern to split the text (default) or repeatedly matching the regex
#' (if \code{gaps} is false). Optional parameters also allow filtering tokens using a
#' minimal length. It returns an array of strings that can be empty.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param gaps Indicates whether regex splits on gaps (TRUE) or matches tokens (FALSE).
#' @param min_token_length Minimum token length, greater than or equal to 0.
#' @param pattern The regular expression pattern to be used.
#' @param to_lower_case Indicates whether to convert all characters to lowercase before tokenizing.
#'
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

#' Default stop words
#'
#' Loads the default stop words for the given language.
#'
#' @param sc A \code{spark_connection}
#' @param language A character string.
#'
#' @details Supported languages: danish, dutch, english, finnish, french,
#'   german, hungarian, italian, norwegian, portuguese, russian, spanish,
#'   swedish, turkish. See \url{http://anoncvs.postgresql.org/cvsweb.cgi/pgsql/src/backend/snowball/stopwords/}
#'   for more details
#'
#' @return A list of stop words.
#'
#' @seealso ft_stop_words_remover
#' @export
ml_default_stop_words <- function(
  sc, language = c("danish", "dutch", "english", "finnish",
                   "french", "german", "hungarian", "italian",
                   "norwegian", "portuguese", "russian", "spanish",
                   "swedish", "turkish"), ...) {
  language <- rlang::arg_match(language)
  invoke_static(sc, "org.apache.spark.ml.feature.StopWordsRemover",
                "loadDefaultStopWords", language)
}

#' Feature Tranformation -- StopWordsRemover
#'
#' A feature transformer that filters out stop words from input.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#'
#' @param case_sensitive Whether to do a case sensitive comparison over the stop words.
#' @param stop_words The words to be filtered out.
#'
#' @seealso ml_default_stop_words
#'
#' @export
ft_stop_words_remover <- function(
  x, input_col, output_col, case_sensitive = FALSE,
  stop_words = ml_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"), ...) {
  UseMethod("ft_stop_words_remover")
}

#' @export
ft_stop_words_remover.spark_connection <- function(
  x, input_col, output_col, case_sensitive = FALSE,
  stop_words = ml_default_stop_words(spark_connection(x), "english"),
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
  stop_words = ml_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_stop_words_remover.tbl_spark <- function(
  x, input_col, output_col, case_sensitive = FALSE,
  stop_words = ml_default_stop_words(spark_connection(x), "english"),
  uid = random_string("stop_words_remover_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}
