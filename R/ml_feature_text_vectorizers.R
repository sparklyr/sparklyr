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
ft_count_vectorizer <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  binary = FALSE,
  min_df = 1,
  min_tf = 1,
  vocab_size = 2^18,
  uid = random_string("count_vectorizer_"),
  ...
) {
  UseMethod("ft_count_vectorizer")
}

ml_count_vectorizer <- ft_count_vectorizer

#' @export
ft_count_vectorizer.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  binary = FALSE,
  min_df = 1,
  min_tf = 1,
  vocab_size = 2^18,
  uid = random_string("count_vectorizer_"),
  ...
) {
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
    x,
    "org.apache.spark.ml.feature.CountVectorizer",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    (function(obj) {
      do.call(
        invoke,
        c(
          obj,
          "%>%",
          Filter(
            function(x) !is.null(x),
            list(
              jobj_set_param_helper(
                obj,
                "setBinary",
                .args[["binary"]],
                "2.0.0",
                FALSE
              ),
              list("setMinDF", .args[["min_df"]]),
              list("setMinTF", .args[["min_tf"]]),
              list("setVocabSize", .args[["vocab_size"]])
            )
          )
        )
      )
    }) %>%
    new_ml_count_vectorizer()

  estimator
}

#' @export
ft_count_vectorizer.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  binary = FALSE,
  min_df = 1,
  min_tf = 1,
  vocab_size = 2^18,
  uid = random_string("count_vectorizer_"),
  ...
) {
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
ft_count_vectorizer.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  binary = FALSE,
  min_df = 1,
  min_tf = 1,
  vocab_size = 2^18,
  uid = random_string("count_vectorizer_"),
  ...
) {
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

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

# Constructors

new_ml_count_vectorizer <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_count_vectorizer")
}

new_ml_count_vectorizer_model <- function(jobj) {
  new_ml_transformer(
    jobj,
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

validator_ml_count_vectorizer <- function(.args) {
  .args <- validate_args_transformer(.args)

  .args[["binary"]] <- cast_scalar_logical(.args[["binary"]])
  .args[["min_df"]] <- cast_scalar_double(.args[["min_df"]])
  .args[["min_tf"]] <- cast_scalar_double(.args[["min_tf"]])
  .args[["vocab_size"]] <- cast_scalar_integer(.args[["vocab_size"]])
  .args
}

#' Feature Transformation -- HashingTF (Transformer)
#'
#' Maps a sequence of terms to their term frequencies using the hashing trick.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @param binary Binary toggle to control term frequency counts.
#'   If true, all non-zero counts are set to 1. This is useful for discrete
#'   probabilistic models that model binary events rather than integer
#'   counts. (default = \code{FALSE})
#' @param num_features Number of features. Should be greater than 0. (default = \code{2^18})
#'
#' @export
ft_hashing_tf <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  binary = FALSE,
  num_features = 2^18,
  uid = random_string("hashing_tf_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_hashing_tf")
}

ml_hashing_tf <- ft_hashing_tf

#' @export
ft_hashing_tf.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  binary = FALSE,
  num_features = 2^18,
  uid = random_string("hashing_tf_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    binary = binary,
    num_features = num_features,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_hashing_tf()

  jobj <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.HashingTF",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    jobj_set_param("setBinary", .args[["binary"]], "2.0.0", FALSE) %>%
    invoke("setNumFeatures", .args[["num_features"]])

  new_ml_hashing_tf(jobj)
}

#' @export
ft_hashing_tf.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  binary = FALSE,
  num_features = 2^18,
  uid = random_string("hashing_tf_"),
  ...
) {
  stage <- ft_hashing_tf.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    binary = binary,
    num_features = num_features,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_hashing_tf.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  binary = FALSE,
  num_features = 2^18,
  uid = random_string("hashing_tf_"),
  ...
) {
  stage <- ft_hashing_tf.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    binary = binary,
    num_features = num_features,
    uid = uid,
    ...
  )
  ml_transform(stage, x)
}

new_ml_hashing_tf <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_hashing_tf")
}

validator_ml_hashing_tf <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["binary"]] <- cast_scalar_logical(.args[["binary"]])
  .args[["num_features"]] <- cast_scalar_integer(.args[["num_features"]])
  .args
}

#' Feature Transformation -- IDF (Estimator)
#'
#' Compute the Inverse Document Frequency (IDF) given a collection of documents.
#'
#' @template roxlate-ml-feature-input-output-col
#' @template roxlate-ml-feature-transformer
#' @template roxlate-ml-feature-estimator-transformer
#' @param min_doc_freq The minimum number of documents in which a term should appear. Default: 0
#'
#' @export
ft_idf <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  min_doc_freq = 0,
  uid = random_string("idf_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_idf")
}

ml_idf <- ft_idf

#' @export
ft_idf.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  min_doc_freq = 0,
  uid = random_string("idf_"),
  ...
) {
  .args <- list(
    input_col = input_col,
    output_col = output_col,
    min_doc_freq = min_doc_freq,
    uid = uid
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_idf()

  estimator <- spark_pipeline_stage(
    x,
    "org.apache.spark.ml.feature.IDF",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke("setMinDocFreq", .args[["min_doc_freq"]]) %>%
    new_ml_idf()

  estimator
}

#' @export
ft_idf.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  min_doc_freq = 0,
  uid = random_string("idf_"),
  ...
) {
  stage <- ft_idf.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    min_doc_freq = min_doc_freq,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ft_idf.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  min_doc_freq = 0,
  uid = random_string("idf_"),
  ...
) {
  stage <- ft_idf.spark_connection(
    x = spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    min_doc_freq = min_doc_freq,
    uid = uid,
    ...
  )

  if (is_ml_transformer(stage)) {
    ml_transform(stage, x)
  } else {
    ml_fit_and_transform(stage, x)
  }
}

new_ml_idf <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_idf")
}

new_ml_idf_model <- function(jobj) {
  new_ml_transformer(jobj, class = "ml_idf_model")
}

validator_ml_idf <- function(.args) {
  .args <- validate_args_transformer(.args)
  .args[["min_doc_freq"]] <- cast_scalar_integer(.args[["min_doc_freq"]])
  .args
}

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
ft_word2vec <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  vector_size = 100,
  min_count = 5,
  max_sentence_length = 1000,
  num_partitions = 1,
  step_size = 0.025,
  max_iter = 1,
  seed = NULL,
  uid = random_string("word2vec_"),
  ...
) {
  check_dots_used()
  UseMethod("ft_word2vec")
}

ml_word2vec <- ft_word2vec

#' @export
ft_word2vec.spark_connection <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  vector_size = 100,
  min_count = 5,
  max_sentence_length = 1000,
  num_partitions = 1,
  step_size = 0.025,
  max_iter = 1,
  seed = NULL,
  uid = random_string("word2vec_"),
  ...
) {
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
    x,
    "org.apache.spark.ml.feature.Word2Vec",
    input_col = .args[["input_col"]],
    output_col = .args[["output_col"]],
    uid = .args[["uid"]]
  ) %>%
    invoke("setVectorSize", .args[["vector_size"]]) %>%
    invoke("setMinCount", .args[["min_count"]]) %>%
    invoke("setNumPartitions", .args[["num_partitions"]]) %>%
    invoke("setStepSize", .args[["step_size"]]) %>%
    invoke("setMaxIter", .args[["max_iter"]]) %>%
    jobj_set_param(
      "setMaxSentenceLength",
      .args[["max_sentence_length"]],
      "2.0.0",
      1000
    )

  if (!is.null(.args[["seed"]])) {
    jobj <- invoke(jobj, "setSeed", .args[["seed"]])
  }

  estimator <- new_ml_word2vec(jobj)

  estimator
}

#' @export
ft_word2vec.ml_pipeline <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  vector_size = 100,
  min_count = 5,
  max_sentence_length = 1000,
  num_partitions = 1,
  step_size = 0.025,
  max_iter = 1,
  seed = NULL,
  uid = random_string("word2vec_"),
  ...
) {
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
ft_word2vec.tbl_spark <- function(
  x,
  input_col = NULL,
  output_col = NULL,
  vector_size = 100,
  min_count = 5,
  max_sentence_length = 1000,
  num_partitions = 1,
  step_size = 0.025,
  max_iter = 1,
  seed = NULL,
  uid = random_string("word2vec_"),
  ...
) {
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
  new_ml_transformer(
    jobj,
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
  .args[["max_sentence_length"]] <- cast_scalar_integer(.args[[
    "max_sentence_length"
  ]])
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
