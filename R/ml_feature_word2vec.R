#' Feature Transformation -- Word2Vec (Estimator)
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
ft_word2vec <- function(x, input_col = NULL, output_col = NULL, vector_size = 100, min_count = 5,
                        max_sentence_length = 1000, num_partitions = 1, step_size = 0.025, max_iter = 1,
                        seed = NULL, uid = random_string("word2vec_"), ...) {
  check_dots_used()
  UseMethod("ft_word2vec")
}

ml_word2vec <- ft_word2vec

#' @export
ft_word2vec.spark_connection <- function(x, input_col = NULL, output_col = NULL, vector_size = 100, min_count = 5,
                                         max_sentence_length = 1000, num_partitions = 1, step_size = 0.025, max_iter = 1,
                                         seed = NULL, uid = random_string("word2vec_"), ...) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    vector_size = vector_size,
    min_count = min_count,
    max_sentence_length = max_sentence_length,
    num_partitions = num_partitions,
    step_size = step_size,
    max_iter = max_iter,
    seed = seed,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_word2vec()

  jobj <- spark_pipeline_stage(
    x, "org.apache.spark.ml.feature.Word2Vec",
    input_col = .args[["input_col"]], output_col = .args[["output_col"]], uid = .args[["uid"]]
  ) %>%
    invoke("setVectorSize", .args[["vector_size"]]) %>%
    invoke("setMinCount", .args[["min_count"]]) %>%
    invoke("setNumPartitions", .args[["num_partitions"]]) %>%
    invoke("setStepSize", .args[["step_size"]]) %>%
    invoke("setMaxIter", .args[["max_iter"]]) %>%
    jobj_set_param("setMaxSentenceLength", .args[["max_sentence_length"]], "2.0.0", 1000)

  if (!is.null(.args[["seed"]])) {
    jobj <- invoke(jobj, "setSeed", .args[["seed"]])
  }

  estimator <- new_ml_word2vec(jobj)

  estimator
}

#' @export
ft_word2vec.ml_pipeline <- function(x, input_col = NULL, output_col = NULL, vector_size = 100, min_count = 5,
                                    max_sentence_length = 1000, num_partitions = 1, step_size = 0.025, max_iter = 1,
                                    seed = NULL, uid = random_string("word2vec_"), ...) {
  stage <- ft_word2vec.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    vector_size = vector_size,
    min_count = min_count,
    max_sentence_length = max_sentence_length,
    num_partitions = num_partitions,
    step_size = step_size,
    max_iter = max_iter,
    seed = seed,
    uid = uid
  )
  ml_add_stage(x, stage)
}

#' @export
ft_word2vec.tbl_spark <- function(x, input_col = NULL, output_col = NULL, vector_size = 100, min_count = 5,
                                  max_sentence_length = 1000, num_partitions = 1, step_size = 0.025, max_iter = 1,
                                  seed = NULL, uid = random_string("word2vec_"), ...) {
  stage <- ft_word2vec.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    vector_size = vector_size,
    min_count = min_count,
    max_sentence_length = max_sentence_length,
    num_partitions = num_partitions,
    step_size = step_size,
    max_iter = max_iter,
    seed = seed,
    uid = uid
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_word2vec <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_word2vec")
}

new_ml_word2vec_model <- function(jobj) {
  new_ml_transformer(jobj,
    find_synonyms = function(word, num) {
      word <- cast_string(word)
      num <- cast_scalar_integer(num)
      invoke(jobj, "findSynonyms", word, num) %>%
        sdf_register()
    },
    find_synonyms_array = function(word, num) {
      word <- cast_string(word)
      num <- cast_scalar_integer(num)
      invoke(jobj, "findSynonymsArray", word, num)
    },
    vectors = invoke(jobj, "getVectors"),
    class = "ml_word2vec_model"
  )
}

validator_ml_word2vec <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["vector_size"]] <- cast_scalar_integer(.args[["vector_size"]])
  .args[["min_count"]] <- cast_scalar_integer(.args[["min_count"]])
  .args[["max_sentence_length"]] <- cast_scalar_integer(.args[["max_sentence_length"]])
  .args[["num_partitions"]] <- cast_scalar_integer(.args[["num_partitions"]])
  .args[["step_size"]] <- cast_scalar_double(.args[["step_size"]])
  .args[["max_iter"]] <- cast_scalar_integer(.args[["max_iter"]])
  .args[["seed"]] <- cast_nullable_scalar_integer(.args[["seed"]])
  .args
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
