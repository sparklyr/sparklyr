#' @export
ml_lda <- function(
  x,
  k = 10L,
  max_iter = 20L,
  doc_concentration = NULL,
  topic_concentration = NULL,
  subsampling_rate = 0.05,
  optimizer = "online",
  checkpoint_interval = 10L,
  keep_last_checkpoint = TRUE,
  learning_decay = 0.51,
  learning_offset = 1024,
  optimize_doc_concentration = TRUE,
  seed = NULL,
  features_col = "features",
  topic_distribution_col = "topicDistribution",
  uid = random_string("lda_"), ...
) {
  UseMethod("ml_lda")
}

#' @export
ml_lda.spark_connection <- function(
  x,
  k = 10L,
  max_iter = 20L,
  doc_concentration = NULL,
  topic_concentration = NULL,
  subsampling_rate = 0.05,
  optimizer = "online",
  checkpoint_interval = 10L,
  keep_last_checkpoint = TRUE,
  learning_decay = 0.51,
  learning_offset = 1024,
  optimize_doc_concentration = TRUE,
  seed = NULL,
  features_col = "features",
  topic_distribution_col = "topicDistribution",
  uid = random_string("lda_"), ...) {

  ml_ratify_args()

  jobj <- invoke_new(x, "org.apache.spark.ml.clustering.LDA", uid) %>%
    invoke("setK", k) %>%
    invoke("setMaxIter", max_iter) %>%
    invoke("setSubsamplingRate", subsampling_rate) %>%
    invoke("setOptimizer", optimizer) %>%
    invoke("setCheckpointInterval", checkpoint_interval) %>%
    invoke("setKeepLastCheckpoint", keep_last_checkpoint) %>%
    invoke("setLearningDecay", learning_decay) %>%
    invoke("setLearningOffset", learning_offset) %>%
    invoke("setOptimizeDocConcentration", optimize_doc_concentration) %>%
    invoke("setFeaturesCol", features_col) %>%
    invoke("setTopicDistributionCol", topic_distribution_col)

  if (!rlang::is_null(doc_concentration))
    jobj <- invoke(jobj, "setDocConcentration", doc_concentration)

  if (!rlang::is_null(topic_concentration))
    jobj <- invoke(jobj, "setTopicConcentration", topic_concentration)

  if (!rlang::is_null(seed))
    jobj <- invoke(jobj, "setSeed", seed)

  new_ml_lda(jobj)
}

#' @export
ml_lda.ml_pipeline <- function(
  x,
  k = 10L,
  max_iter = 20L,
  doc_concentration = NULL,
  topic_concentration = NULL,
  subsampling_rate = 0.05,
  optimizer = "online",
  checkpoint_interval = 10L,
  keep_last_checkpoint = TRUE,
  learning_decay = 0.51,
  learning_offset = 1024,
  optimize_doc_concentration = TRUE,
  seed = NULL,
  features_col = "features",
  topic_distribution_col = "topicDistribution",
  uid = random_string("lda_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ml_lda.tbl_spark <- function(
  x,
  k = 10L,
  max_iter = 20L,
  doc_concentration = NULL,
  topic_concentration = NULL,
  subsampling_rate = 0.05,
  optimizer = "online",
  checkpoint_interval = 10L,
  keep_last_checkpoint = TRUE,
  learning_decay = 0.51,
  learning_offset = 1024,
  optimize_doc_concentration = TRUE,
  seed = NULL,
  features_col = "features",
  topic_distribution_col = "topicDistribution",
  uid = random_string("lda_"), ...) {

  predictor <- ml_new_stage_modified_args()

  predictor %>%
    ml_fit(x)
}

# Validator
ml_validator_lda <- function(args, nms) {
  old_new_mapping <- c(
    ml_tree_param_mapping(),
    list(
      alpha = "doc_concentration",
      beta = "topic_concentration",
      max.iterations = "max_iter"
    ))

  args %>%
    ml_validate_args({
      k <- ensure_scalar_integer(k)
      max_iter <- ensure_scalar_integer(max_iter)
      doc_concentration <- ensure_scalar_double(doc_concentration, allow.null = TRUE)
      topic_concentration <- ensure_scalar_double(topic_concentration, allow.null = TRUE)
      subsampling_rate <- ensure_scalar_double(subsampling_rate)
      optimizer <- rlang::arg_match(optimizer, c("online", "em"))
      checkpoint_interval <- ensure_scalar_integer(checkpoint_interval)
      keep_last_checkpoint <- ensure_scalar_boolean(keep_last_checkpoint)
      learning_decay <- ensure_scalar_double(learning_decay)
      learning_offset <- ensure_scalar_double(learning_offset)
      optimize_doc_concentration <- ensure_scalar_boolean(optimize_doc_concentration)
      seed <- ensure_scalar_integer(seed, allow.null = TRUE)
      features_col <- ensure_scalar_character(features_col)
      topic_distribution_col <- ensure_scalar_character(topic_distribution_col)
    }, old_new_mapping) %>%
    ml_extract_args(nms, old_new_mapping)
}

# Constructors

new_ml_lda <- function(jobj) {
  new_ml_predictor(jobj, subclass = "ml_lda")
}

new_ml_lda_model <- function(jobj) {

  new_ml_clustering_model(
    jobj,
    is_distributed = invoke(jobj, "isDistributed"),
    describe_topics = function(max_terms_per_topic = NULL) {
      max_terms_per_topic <- ensure_scalar_integer(max_terms_per_topic, allow.null = TRUE)

      (if (rlang::is_null(max_terms_per_topic))
        invoke(jobj, "describeTopics")
      else
        invoke(jobj, "describeTopics", max_terms_per_topic)
      ) %>%
        sdf_register()
    },
    estimated_doc_concentration = try_null(invoke(jobj, "estimatedDocConcentration")),
    log_likelihood = function(dataset) invoke(jobj, "logLikelihood", spark_dataframe(dataset)),
    log_perplexity = function(dataset) invoke(jobj, "logPerplexity", spark_dataframe(dataset)),
    topicsMatrix = try_null(read_spark_matrix(jobj, "topicsMatrix")),
    vocab_size = invoke(jobj, "vocabSize"),
    subclass = "ml_lda_model")
}

# Generic implementations

#' @export
ml_fit.ml_lda <- function(x, data, ...) {
  jobj <- spark_jobj(x) %>%
    invoke("fit", spark_dataframe(data))
  new_ml_lda_model(jobj)
}
