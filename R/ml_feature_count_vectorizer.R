#' Feature Transformation -- CountVectorizer (Estimator)
#'
#' Extracts a vocabulary from document collections.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#'
#' @param binary Binary toggle to control the output vector values.
#'   If \code{TRUE}, all nonzero counts (after \code{min_tf} filter applied)
#'   are set to 1. This is useful for discrete probabilistic models that
#'    model binary events rather than integer counts. Default: \code{FALSE}
#' @param min_df Specifies the minimum number of different documents a
#'   term must appear in to be included in the vocabulary. If this is an
#'   integer greater than or equal to 1, this specifies the number of
#'   documents the term must appear in; if this is a double in [0,1), then
#'   this specifies the fraction of documents. Default: 1.
#' @param min_tf Filter to ignore rare words in a document. For each
#'   document, terms with frequency/count less than the given threshold
#'   are ignored. If this is an integer greater than or equal to 1, then
#'   this specifies a count (of times the term must appear in the document);
#'   if this is a double in [0,1), then this specifies a fraction (out of
#'   the document's token count). Default: 1.
#' @param vocab_size Build a vocabulary that only considers the top
#'   \code{vocab_size} terms ordered by term frequency across the corpus.
#'   Default: \code{2^18}.
#'
#' @export
ft_count_vectorizer <- function(x, input_col = NULL, output_col = NULL, binary = FALSE,
                                min_df = 1, min_tf = 1, vocab_size = 2^18,
                                uid = random_string("count_vectorizer_"), ...) {
  UseMethod("ft_count_vectorizer")
}

ft_count_vectorizer_impl <- function(x, input_col = NULL, output_col = NULL,
                                     binary = FALSE, min_df = 1, min_tf = 1,
                                     vocab_size = 2^18, uid = random_string("count_vectorizer_"),
                                     ...) {
  estimator_process(
    x = x,
    uid = uid,
    spark_class = "org.apache.spark.ml.feature.CountVectorizer",
    r_class = "ml_count_vectorizer",
    invoke_steps = list(
      setInputCol = cast_nullable_string(input_col),
      setOutputCol = cast_nullable_string(output_col),
      setBinary = cast_scalar_logical(binary),
      setMinDF = cast_scalar_double(min_df),
      setMinTF = cast_scalar_double(min_tf),
      setVocabSize = cast_scalar_integer(vocab_size)
      )
  )
}


ml_count_vectorizer <- ft_count_vectorizer

#' @export
ft_count_vectorizer.spark_connection <- ft_count_vectorizer_impl

#' @export
ft_count_vectorizer.ml_pipeline <- ft_count_vectorizer_impl

#' @export
ft_count_vectorizer.tbl_spark <- ft_count_vectorizer_impl

new_ml_count_vectorizer_model <- function(jobj) {
  new_ml_transformer(jobj,
    vocabulary = invoke(jobj, "vocabulary"),
    class = "ml_count_vectorizer_model"
  )
}

#' @rdname ft_count_vectorizer
#' @param model A \code{ml_count_vectorizer_model}.
#' @return \code{ml_vocabulary()} returns a vector of vocabulary built.
#' @export
ml_vocabulary <- function(model) {
  unlist(model$vocabulary)
}
