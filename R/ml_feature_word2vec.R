#' Feature Tranformation -- Word2Vec (Estimator)
#'
#' Word2Vec transforms a word into a code for further natural language processing or machine learning process.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @template roxlate-ml-max-iter
#' @param max_sentence_length (Spark 2.0.0+) Sets the maximum length (in words) of each sentence
#'   in the input data. Any sentence longer than this threshold will be divided into
#'   chunks of up to \code{max_sentence_length} size. Default: 1000
#' @param min_count The minimum number of times a token must appear to be included in
#'   the word2vec model's vocabulary. Default: 5
#' @param num_partitions Number of partitions for sentences of words. Default: 1
#' @template roxlate-ml-seed
#' @param step_size Param for Step size to be used for each iteration of optimization (> 0).
#' @param vector_size The dimension of the code that you want to transform from words. Default: 100
#'
#' @export
ft_word2vec <- function(
  x, input_col, output_col, vector_size = 100L, min_count = 5L,
  max_sentence_length = 1000L, num_partitions = 1L, step_size = 0.025, max_iter = 1L,
  seed = NULL, dataset = NULL, uid = random_string("word2vec_"), ...) {
  UseMethod("ft_word2vec")
}

#' @export
ft_word2vec.spark_connection <- function(
  x, input_col, output_col, vector_size = 100L, min_count = 5L,
  max_sentence_length = 1000L, num_partitions = 1L, step_size = 0.025, max_iter = 1L,
  seed = NULL, dataset = NULL, uid = random_string("word2vec_"), ...) {

  ml_ratify_args()

  jobj <- ml_new_transformer(x, "org.apache.spark.ml.feature.Word2Vec",
                                  input_col, output_col, uid) %>%
    invoke("setVectorSize", vector_size) %>%
    invoke("setMinCount", min_count) %>%
    invoke("setNumPartitions", num_partitions) %>%
    invoke("setStepSize", step_size) %>%
    invoke("setMaxIter", max_iter) %>%
    jobj_set_param("setMaxSentenceLength", max_sentence_length, 1000L, "2.0.0")

  if (!rlang::is_null(seed))
    jobj <- invoke(jobj, "setSeed", seed)

  estimator <- new_ml_word2vec(jobj)

  if (is.null(dataset))
    estimator
  else
    ml_fit(estimator, dataset)
}

#' @export
ft_word2vec.ml_pipeline <- function(
  x, input_col, output_col, vector_size = 100L, min_count = 5L,
  max_sentence_length = 1000L, num_partitions = 1L, step_size = 0.025, max_iter = 1L,
  seed = NULL, dataset = NULL, uid = random_string("word2vec_"), ...
) {

  stage <- ml_new_stage_modified_args()
  ml_add_stage(x, stage)

}

#' @export
ft_word2vec.tbl_spark <- function(
  x, input_col, output_col, vector_size = 100L, min_count = 5L,
  max_sentence_length = 1000L, num_partitions = 1L, step_size = 0.025, max_iter = 1L,
  seed = NULL, dataset = NULL, uid = random_string("word2vec_"), ...
) {
  dots <- rlang::dots_list(...)

  stage <- ml_new_stage_modified_args()

  if (is_ml_transformer(stage))
    ml_transform(stage, x)
  else
    ml_fit_and_transform(stage, x)
}

new_ml_word2vec <- function(jobj) {
  new_ml_estimator(jobj, subclass = "ml_word2vec")
}

new_ml_word2vec_model <- function(jobj) {
  new_ml_transformer(jobj,
                     find_synonyms = function(word, num) {
                       word <- ensure_scalar_character(word)
                       num <- ensure_scalar_integer(num)
                       invoke(jobj, "findSynonyms", word, num) %>%
                         sdf_register()
                     },
                     find_synonyms_array = function(word, num) {
                       word <- ensure_scalar_character(word)
                       num <- ensure_scalar_integer(num)
                       invoke(jobj, "findSynonymsArray", word, num)
                     },
                     vectors = invoke(jobj, "getVectors"),
                     subclass = "ml_word2vec_model")
}

ml_validator_word2vec <- function(args, nms) {
  args %>%
    ml_validate_args({
      vector_size <- ensure_scalar_integer(vector_size)
      min_count <- ensure_scalar_integer(min_count)
      max_sentence_length <- ensure_scalar_integer(max_sentence_length)
      num_partitions <- ensure_scalar_integer(num_partitions)
      step_size <- ensure_scalar_double(step_size)
      max_iter <- ensure_scalar_integer(max_iter)
      seed <- ensure_scalar_integer(seed, allow.null = TRUE)
    }) %>%
    ml_extract_args(nms)
}

#' @rdname ft_word2vec
#' @param model A fitted \code{Word2Vec} model, returned by \code{ft_word2vec()}.
#' @param word A word, as a length-one character vector.
#' @param num Number of words closest in similarity to the given word to find.
#' @return \code{ml_find_synonyms()} returns a DataFrame of synonyms and cosine similarities
#' @export
ml_find_synonyms <- function(model, word, num) {
  model$find_synonyms(word, num)
}
