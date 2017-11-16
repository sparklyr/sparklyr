#' Feature Tranformation -- CountVectorizer (Estimator)
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
ft_count_vectorizer <- function(
  x, input_col, output_col, binary = FALSE, min_df = 1, min_tf = 1,
  vocab_size = as.integer(2^18), dataset = NULL,
  uid = random_string("count_vectorizer_"), ...) {
  UseMethod("ft_count_vectorizer")
}

#' @export
ft_count_vectorizer.spark_connection <- function(
  x, input_col, output_col, binary = FALSE, min_df = 1, min_tf = 1,
  vocab_size = as.integer(2^18), dataset = NULL,
  uid = random_string("count_vectorizer_"), ...) {

  ml_ratify_args()

  estimator <- ml_new_transformer(x, "org.apache.spark.ml.feature.CountVectorizer",
                                  input_col, output_col, uid) %>%
    jobj_set_param("setBinary", binary, FALSE, "2.0.0") %>%
    invoke("setMinDF", min_df) %>%
    invoke("setMinTF", min_tf) %>%
    invoke("setVocabSize", vocab_size) %>%
    new_ml_count_vectorizer()

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_count_vectorizer.ml_pipeline <- function(
  x, input_col, output_col, binary = FALSE, min_df = 1, min_tf = 1,
  vocab_size = as.integer(2^18), dataset = NULL,
  uid = random_string("count_vectorizer_"), ...
) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)

}

#' @export
ft_count_vectorizer.tbl_spark <- function(
  x, input_col, output_col, binary = FALSE, min_df = 1, min_tf = 1,
  vocab_size = as.integer(2^18), dataset = NULL,
  uid = random_string("count_vectorizer_"), ...
) {
  stage <- ml_new_stage_modified_args()

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

# Validator

ml_validator_count_vectorizer <- function(args, nms) {
  old_new_mapping <- c(
    list(
      min.df = "min_df",
      min.tf = "min_tf",
      vocab.size = "vocab_size"
    ), input_output_mapping
  )

  args %>%
    ml_validate_args(
      {
        binary <- ensure_scalar_boolean(binary)
        min_df <- ensure_scalar_double(min_df)
        min_tf <- ensure_scalar_double(min_tf)
        vocab_size <- ensure_scalar_integer(vocab_size)
      }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_count_vectorizer <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_count_vectorizer")
}

new_ml_count_vectorizer_model <- function(jobj) {
  new_ml_transformer(jobj,
                     vocabulary = invoke(jobj, "vocabulary"),
                     subclass = "ml_count_vectorizer_model")
}
