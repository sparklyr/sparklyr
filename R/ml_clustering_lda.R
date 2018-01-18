#' Spark ML -- Latent Dirichlet Allocation
#'
#' Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
#'
#' @details
#'
#' Terminology for LDA:
#' \itemize{
#'   \item "term" = "word": an element of the vocabulary
#'   \item "token": instance of a term appearing in a document
#'   \item "topic": multinomial distribution over terms representing some concept
#'   \item "document": one piece of text, corresponding to one row in the input data
#' }
#'
#' Original LDA paper (journal version): Blei, Ng, and Jordan. "Latent Dirichlet Allocation." JMLR, 2003.
#'
#' Input data (\code{features_col}): LDA is given a collection of documents as input data, via the \code{features_col} parameter. Each document is specified as a Vector of length \code{vocab_size}, where each entry is the count for the corresponding term (word) in the document. Feature transformers such as \code{\link{ft_tokenizer}} and \code{\link{ft_count_vectorizer}} can be useful for converting text to word count vectors
#'
#' @section Parameter details:
#'
#' \subsection{\code{doc_concentration}}{
#'   This is the parameter to a Dirichlet distribution, where larger values mean more smoothing (more regularization). If not set by the user, then \code{doc_concentration} is set automatically. If set to singleton vector [alpha], then alpha is replicated to a vector of length k in fitting. Otherwise, the \code{doc_concentration} vector must be length k. (default = automatic)
#'
#'   Optimizer-specific parameter settings:
#'
#' EM
#'
#' \itemize{
#'   \item Currently only supports symmetric distributions, so all values in the vector should be the same.
#'   \item Values should be greater than 1.0
#'   \item default = uniformly (50 / k) + 1, where 50/k is common in LDA libraries and +1 follows from Asuncion et al. (2009), who recommend a +1 adjustment for EM.
#' }
#'
#' Online
#'
#' \itemize{
#'   \item Values should be greater than or equal to 0
#'   \item default = uniformly (1.0 / k), following the implementation from \href{https://github.com/Blei-Lab/onlineldavb}{here}
#'   }
#' }
#'
#' \subsection{\code{topic_concentration}}{
#'
#' This is the parameter to a symmetric Dirichlet distribution.
#'
#' Note: The topics' distributions over terms are called "beta" in the original LDA paper by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
#'
#' If not set by the user, then \code{topic_concentration} is set automatically. (default = automatic)
#'
#' Optimizer-specific parameter settings:
#'
#' EM
#'
#' \itemize{
#'   \item Value should be greater than 1.0
#'   \item default = 0.1 + 1, where 0.1 gives a small amount of smoothing and +1 follows Asuncion et al. (2009), who recommend a +1 adjustment for EM.
#' }
#'
#' Online
#'
#' \itemize{
#'   \item Value should be greater than or equal to 0
#'   \item default = (1.0 / k), following the implementation from \href{https://github.com/Blei-Lab/onlineldavb}{here}.
#'   }
#' }
#'
#' \subsection{\code{topic_distribution_col}}{
#'   This uses a variational approximation following Hoffman et al. (2010), where the approximate distribution is called "gamma." Technically, this method returns this approximation "gamma" for each document.
#' }
#'
#'
#' @template roxlate-ml-clustering-algo
#' @template roxlate-ml-clustering-params
#' @param doc_concentration Concentration parameter (commonly named "alpha") for the prior placed on documents' distributions over topics ("theta"). See details.
#' @param topic_concentration Concentration parameter (commonly named "beta" or "eta") for the prior placed on topics' distributions over terms.
#' @param topic_distribution_col Output column with estimates of the topic mixture distribution for each document (often called "theta" in the literature). Returns a vector of zeros for an empty document.
#' @template roxlate-ml-checkpoint-interval
#' @param optimizer Optimizer or inference algorithm used to estimate the LDA model. Supported: "online" for Online Variational Bayes (default) and "em" for Expectation-Maximization.
#' @param subsampling_rate (For Online optimizer only) Fraction of the corpus to be sampled and used in each iteration of mini-batch gradient descent, in range (0, 1]. Note that this should be adjusted in synch with \code{max_iter} so the entire corpus is used. Specifically, set both so that maxIterations * miniBatchFraction greater than or equal to 1.
#' @param learning_decay (For Online optimizer only) Learning rate, set as an exponential decay rate. This should be between (0.5, 1.0] to guarantee asymptotic convergence. This is called "kappa" in the Online LDA paper (Hoffman et al., 2010). Default: 0.51, based on Hoffman et al.
#' @param learning_offset (For Online optimizer only) A (positive) learning parameter that downweights early iterations. Larger values make early iterations count less. This is called "tau0" in the Online LDA paper (Hoffman et al., 2010) Default: 1024, following Hoffman et al.
#' @param optimize_doc_concentration (For Online optimizer only) Indicates whether the \code{doc_concentration} (Dirichlet parameter for document-topic distribution) will be optimized during training. Setting this to true will make the model more expressive and fit the training data better. Default: \code{FALSE}
#'
#' @param keep_last_checkpoint (Spark 2.0.0+) (For EM optimizer only) If using checkpointing, this indicates whether to keep the last checkpoint. If \code{FALSE}, then the checkpoint will be deleted. Deleting the checkpoint can cause failures if a data partition is lost, so set this bit with care. Note that checkpoints will be cleaned up via reference counting, regardless.
#'
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
    jobj_set_param("setKeepLastCheckpoint", keep_last_checkpoint,
                   TRUE, "2.0.0") %>%
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
    describe_topics = function(max_terms_per_topic = 10L) {
      max_terms_per_topic <- ensure_scalar_integer(max_terms_per_topic, allow.null = TRUE)
      invoke(jobj, "describeTopics", max_terms_per_topic) %>%
        sdf_register()
    },
    estimated_doc_concentration = try_null(invoke(jobj, "estimatedDocConcentration")),
    log_likelihood = function(dataset) invoke(jobj, "logLikelihood", spark_dataframe(dataset)),
    log_perplexity = function(dataset) invoke(jobj, "logPerplexity", spark_dataframe(dataset)),
    topicsMatrix = try_null(read_spark_matrix(jobj, "topicsMatrix")),
    vocab_size = invoke(jobj, "vocabSize"),
    subclass = "ml_lda_model")
}

#' @rdname ml_lda
#' @return \code{ml_describe_topics} returns a DataFrame with topics and their top-weighted terms.
#' @param model A fitted LDA model returned by \code{ml_lda()}.
#' @param max_terms_per_topic Maximum number of terms to collect for each topic. Default value of 10.
#' @export
ml_describe_topics <- function(model, max_terms_per_topic = 10L) {
  model$describe_topics(max_terms_per_topic)
}

#' @rdname ml_lda
#' @return \code{ml_log_likelihood} calculates a lower bound on the log likelihood of
#'   the entire corpus
#' @param dataset test corpus to use for calculating log likelihood or log perplexity
#' @export
ml_log_likelihood <- function(model, dataset) model$log_likelihood(dataset)

#' @rdname ml_lda
#' @export
ml_log_perplexity <- function(model, dataset) model$log_perplexity(dataset)

# Generic implementations

# TODO
#' #' @export
#' print.ml_model_lda <- function(x, ...) {
#'
#'   header <- sprintf(
#'     "An LDA model fit on %s features",
#'     length(x$features)
#'   )
#'
#'   cat(header, sep = "\n")
#'   print_newline()
#'
#'   cat("Topics Matrix:", sep = "\n")
#'   print(x$topics.matrix)
#'   print_newline()
#'
#'   cat("Estimated Document Concentration:", sep = "\n")
#'   print(x$estimated.doc.concentration)
#'   print_newline()
#'
#' }
