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
                                min_df = 1, min_tf = 1,
                                vocab_size = 2^18,
                                uid = random_string("count_vectorizer_"), ...) {
  UseMethod("ft_count_vectorizer")
}

ml_count_vectorizer <- ft_count_vectorizer

#' @export
ft_count_vectorizer.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                                 binary = FALSE, min_df = 1, min_tf = 1,
                                                 vocab_size = 2^18,
                                                 uid = random_string("count_vectorizer_"), ...) {

  .args <- list(
    input_col = input_col,
    output_col = output_col,
    binary = binary,
    min_df = min_df,
    min_tf = min_tf,
    vocab_size = vocab_size,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_count_vectorizer()

  estimator <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.CountVectorizer",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    jobj_set_param("setBinary", .args[["binary"]], "2.0.0", FALSE) %>%
    invoke("setMinDF", .args[["min_df"]]) %>%
    invoke("setMinTF", .args[["min_tf"]]) %>%
    invoke("setVocabSize", .args[["vocab_size"]]) %>%
    new_ml_count_vectorizer()

  estimator
}

#' @export
ft_count_vectorizer.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                                            binary = FALSE, min_df = 1, min_tf = 1,
                                            vocab_size = 2^18,
                                            uid = random_string("count_vectorizer_"), ...) {
  stage <- ft_count_vectorizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    binary = binary,
    min_df = min_df,
    min_tf = min_tf,
    vocab_size = vocab_size,
    uid = uid,
    ...
  )

  ml_add_stage(x, stage)
}

#' @export
ft_count_vectorizer.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                                          binary = FALSE, min_df = 1, min_tf = 1,
                                          vocab_size = 2^18,
                                          uid = random_string("count_vectorizer_"), ...) {
  stage <- ft_count_vectorizer.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    binary = binary,
    min_df = min_df,
    min_tf = min_tf,
    vocab_size = vocab_size,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

# Constructors

new_ml_count_vectorizer <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_count_vectorizer")
}

new_ml_count_vectorizer_model <- function(jobj) {
  new_ml_transformer(jobj,
                     vocabulary = invoke(jobj, "vocabulary"),
                     class = "ml_count_vectorizer_model")
}

#' @rdname ft_count_vectorizer
#' @param model A \code{ml_count_vectorizer_model}.
#' @return \code{ml_vocabulary()} returns a vector of vocabulary built.
#' @export
ml_vocabulary <- function(model) {
  unlist(model$vocabulary)
}

validator_ml_count_vectorizer <- function(.args) {
  .args <- validate_args_transformer(.args)

  .args[["binary"]] <- cast_scalar_logical(.args[["binary"]])
  .args[["min_df"]] <- cast_scalar_double(.args[["min_df"]])
  .args[["min_tf"]] <- cast_scalar_double(.args[["min_tf"]])
  .args[["vocab_size"]] <- cast_scalar_integer(.args[["vocab_size"]])
  .args
}
