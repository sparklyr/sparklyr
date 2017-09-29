
#' #' Feature Transformation -- QuantileDiscretizer
#' #'
#' #' Takes a column with continuous features and outputs a column with binned
#' #' categorical features. The bin ranges are chosen by taking a sample of the
#' #' data and dividing it into roughly equal parts. The lower and upper bin bounds
#' #' will be -Infinity and +Infinity, covering all real values. This attempts to
#' #' find numBuckets partitions based on a sample of the given input data, but it
#' #' may find fewer depending on the data sample values.
#' #'
#' #' Note that the result may be different every time you run it, since the sample
#' #' strategy behind it is non-deterministic.
#' #'
#' #' @template roxlate-ml-transformation
#' #'
#' #' @param n.buckets The number of buckets to use.
#' #'
#' #' @export
#' ft_quantile_discretizer <- function(x,
#'                                     input.col,
#'                                     output.col,
#'                                     n.buckets = 5L,
#'                                     ...)
#' {
#'   ml_backwards_compatibility_api()
#'   class <- "org.apache.spark.ml.feature.QuantileDiscretizer"
#'   invoke_simple_transformer(x, class, list(
#'     setInputCol   = ensure_scalar_character(input.col),
#'     setOutputCol  = ensure_scalar_character(output.col),
#'     setNumBuckets = ensure_scalar_integer(n.buckets),
#'     function(transformer, sdf) invoke(transformer, "fit", sdf)
#'   ))
#' }


# TODO
# #' Feature Transformations -- HashingTF
# #'
# #' Maps a sequence of terms to their term frequencies.
# #'
# #' @template roxlate-ml-transformation
# #'
# #' @param n_features Number of features.
# #' @param binary Boolean; binary?
# #'
# #' @export
# ft_hashing_tf <- function(x,
#                           input.col,
#                           output.col,
#                           n.features = NULL,
#                           binary = FALSE,
#                           ...)
# {
#   ml_backwards_compatibility_api()
#   class <- "org.apache.spark.ml.feature.HashingTF"
#   invoke_simple_transformer(x, class, list(
#     setInputCol    = ensure_scalar_character(input.col),
#     setOutputCol   = ensure_scalar_character(output.col),
#     setNumFeatures = ensure_scalar_integer(n.features),
#     setBinary      = ensure_scalar_boolean(binary)
#   ))
# }

#' #' Feature Tranformation -- CountVectorizer
#' #'
#' #' Extracts a vocabulary from document collections.
#' #'
#' #' @template roxlate-ml-transformation
#' #'
#' #' @param min.df Specifies the minimum number of different documents a
#' #' term must appear in to be included in the vocabulary. If this is an
#' #' integer greater than or equal to 1, this specifies the number of
#' #' documents the term must appear in; if this is a double in [0,1), then
#' #' this specifies the fraction of documents
#' #' @param min.tf Filter to ignore rare words in a document. For each
#' #' document, terms with frequency/count less than the given threshold
#' #' are ignored. If this is an integer greater than or equal to 1, then
#' #' this specifies a count (of times the term must appear in the document);
#' #' if this is a double in [0,1), then this specifies a fraction (out of
#' #' the document's token count).
#' #' @param vocab.size Build a vocabulary that only considers the top
#' #' vocab.size terms ordered by term frequency across the corpus.
#' #' @param vocabulary.only Boolean; should the vocabulary only be returned?
#' #'
#' #' @export
#' ft_count_vectorizer <- function(x,
#'                                 input.col,
#'                                 output.col,
#'                                 min.df = NULL,
#'                                 min.tf = NULL,
#'                                 vocab.size = NULL,
#'                                 vocabulary.only = FALSE,
#'                                 ...)
#' {
#'   ml_backwards_compatibility_api()
#'   class <- "org.apache.spark.ml.feature.CountVectorizer"
#'   result <- invoke_simple_transformer(x, class, list(
#'     setInputCol    = ensure_scalar_character(input.col),
#'     setOutputCol   = ensure_scalar_character(output.col),
#'     setMinDF       = min.df,
#'     setMinTF       = min.tf,
#'     setVocabSize   = vocab.size,
#'     fit            = spark_dataframe(x)
#'   ),
#'   only.model = vocabulary.only)
#'
#'   if (vocabulary.only) as.character(invoke(result, "vocabulary")) else result
#' }
