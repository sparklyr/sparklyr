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
#' @template roxlate-ml-formula-params
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
#' @param keep_last_checkpoint (Spark 2.0.0+) (For EM optimizer only) If using checkpointing, this indicates whether to keep the last checkpoint. If \code{FALSE}, then the checkpoint will be deleted. Deleting the checkpoint can cause failures if a data partition is lost, so set this bit with care. Note that checkpoints will be cleaned up via reference counting, regardless.
#'
#' @examples
#' \dontrun{
#' library(janeaustenr)
#' library(dplyr)
#' sc <-  spark_connect(master = "local")
#'
#' lines_tbl <- sdf_copy_to(sc,
#'                          austen_books()[c(1:30),],
#'                          name = "lines_tbl",
#'                          overwrite = TRUE)
#'
#' # transform the data in a tidy form
#' lines_tbl_tidy <- lines_tbl %>%
#'   ft_tokenizer(input_col = "text",
#'                output_col = "word_list") %>%
#'   ft_stop_words_remover(input_col = "word_list",
#'                         output_col = "wo_stop_words") %>%
#'   mutate(text = explode(wo_stop_words)) %>%
#'   filter(text != "") %>%
#'   select(text, book)
#'
#' lda_model <- lines_tbl_tidy %>%
#'   ml_lda(~text, k = 4)
#'
#' # vocabulary and topics
#' tidy(lda_model)
#' }
#'
#' @export
ml_lda <- function(x, formula = NULL, k = 10, max_iter = 20, doc_concentration = NULL, topic_concentration = NULL,
                   subsampling_rate = 0.05, optimizer = "online", checkpoint_interval = 10,
                   keep_last_checkpoint = TRUE, learning_decay = 0.51, learning_offset = 1024,
                   optimize_doc_concentration = TRUE, seed = NULL, features_col = "features",
                   topic_distribution_col = "topicDistribution", uid = random_string("lda_"), ...) {
  check_dots_used()
  UseMethod("ml_lda")
}

#' @export
ml_lda.spark_connection <- function(x, formula = NULL, k = 10, max_iter = 20, doc_concentration = NULL, topic_concentration = NULL,
                                    subsampling_rate = 0.05, optimizer = "online", checkpoint_interval = 10,
                                    keep_last_checkpoint = TRUE, learning_decay = 0.51, learning_offset = 1024,
                                    optimize_doc_concentration = TRUE, seed = NULL, features_col = "features",
                                    topic_distribution_col = "topicDistribution", uid = random_string("lda_"), ...) {

  .args <- list(
    k = k,
    max_iter = max_iter,
    doc_concentration = doc_concentration,
    topic_concentration = topic_concentration,
    subsampling_rate = subsampling_rate,
    optimizer = optimizer,
    checkpoint_interval = checkpoint_interval,
    keep_last_checkpoint = keep_last_checkpoint,
    learning_decay = learning_decay,
    learning_offset = learning_offset,
    optimize_doc_concentration = optimize_doc_concentration,
    seed = seed,
    features_col = features_col,
    topic_distribution_col = topic_distribution_col
  ) %>%
    c(rlang::dots_list(...)) %>%
    validator_ml_lda()

  uid <- cast_string(uid)

  jobj <- invoke_new(x, "org.apache.spark.ml.clustering.LDA", uid) %>%
    invoke("setK", .args[["k"]]) %>%
    invoke("setMaxIter", .args[["max_iter"]]) %>%
    invoke("setSubsamplingRate", .args[["subsampling_rate"]]) %>%
    invoke("setOptimizer", .args[["optimizer"]]) %>%
    invoke("setCheckpointInterval", .args[["checkpoint_interval"]]) %>%
    jobj_set_param("setKeepLastCheckpoint", .args[["keep_last_checkpoint"]], "2.0.0", TRUE) %>%
    invoke("setLearningDecay", .args[["learning_decay"]]) %>%
    invoke("setLearningOffset", .args[["learning_offset"]]) %>%
    invoke("setOptimizeDocConcentration", .args[["optimize_doc_concentration"]]) %>%
    invoke("setFeaturesCol", .args[["features_col"]]) %>%
    invoke("setTopicDistributionCol", .args[["topic_distribution_col"]]) %>%
    jobj_set_param("setDocConcentration", .args[["doc_concentration"]]) %>%
    jobj_set_param("setTopicConcentration", .args[["topic_concentration"]]) %>%
    jobj_set_param("setSeed", .args[["seed"]])

  new_ml_lda(jobj)
}

#' @export
ml_lda.ml_pipeline <- function(x, formula = NULL, k = 10, max_iter = 20, doc_concentration = NULL, topic_concentration = NULL,
                               subsampling_rate = 0.05, optimizer = "online", checkpoint_interval = 10,
                               keep_last_checkpoint = TRUE, learning_decay = 0.51, learning_offset = 1024,
                               optimize_doc_concentration = TRUE, seed = NULL, features_col = "features",
                               topic_distribution_col = "topicDistribution", uid = random_string("lda_"), ...) {

  stage <- ml_lda.spark_connection(
    x = spark_connection(x),
    formula = formula,
    k = k,
    max_iter = max_iter,
    doc_concentration = doc_concentration,
    topic_concentration = topic_concentration,
    subsampling_rate = subsampling_rate,
    optimizer = optimizer,
    checkpoint_interval = checkpoint_interval,
    keep_last_checkpoint = keep_last_checkpoint,
    learning_decay = learning_decay,
    learning_offset = learning_offset,
    optimize_doc_concentration = optimize_doc_concentration,
    seed = seed,
    features_col = features_col,
    topic_distribution_col = topic_distribution_col,
    uid = uid,
    ...
  )
  ml_add_stage(x, stage)
}

#' @export
ml_lda.tbl_spark <- function(x, formula = NULL, k = 10, max_iter = 20, doc_concentration = NULL, topic_concentration = NULL,
                             subsampling_rate = 0.05, optimizer = "online", checkpoint_interval = 10,
                             keep_last_checkpoint = TRUE, learning_decay = 0.51, learning_offset = 1024,
                             optimize_doc_concentration = TRUE, seed = NULL, features_col = "features",
                             topic_distribution_col = "topicDistribution", uid = random_string("lda_"), ...) {
  formula <- ml_standardize_formula(formula)

   stage <- ml_lda.spark_connection(
    x = spark_connection(x),
    formula = NULL,
    k = k,
    max_iter = max_iter,
    doc_concentration = doc_concentration,
    topic_concentration = topic_concentration,
    subsampling_rate = subsampling_rate,
    optimizer = optimizer,
    checkpoint_interval = checkpoint_interval,
    keep_last_checkpoint = keep_last_checkpoint,
    learning_decay = learning_decay,
    learning_offset = learning_offset,
    optimize_doc_concentration = optimize_doc_concentration,
    seed = seed,
    features_col = features_col,
    topic_distribution_col = topic_distribution_col,
    uid = uid,
    ...
  )

  if (is.null(formula)) {
  stage %>%
    ml_fit(x)
   } else {
     ml_construct_model_clustering(
       new_ml_model_lda,
       predictor = stage,
       dataset = x,
       formula = formula,
       features_col = features_col
     )
   }
}

# Validator
validator_ml_lda <- function(.args) {
  .args <- validate_args_clustering(.args)

  .args[["doc_concentration"]] <- cast_nullable_scalar_double(.args[["doc_concentration"]])
  .args[["topic_concentration"]] <- cast_nullable_scalar_double(.args[["topic_concentration"]])
  .args[["subsampling_rate"]] <- cast_scalar_double(.args[["subsampling_rate"]])
  .args[["optimizer"]] <- cast_choice(.args[["optimizer"]], c("online", "em"))
  .args[["checkpoint_interval"]] <- cast_scalar_integer(.args[["checkpoint_interval"]])
  .args[["keep_last_checkpoint"]] <- cast_scalar_logical(.args[["keep_last_checkpoint"]])
  .args[["learning_decay"]] <- cast_scalar_double(.args[["learning_decay"]])
  .args[["learning_offset"]] <- cast_scalar_double(.args[["learning_offset"]])
  .args[["optimize_doc_concentration"]] <- cast_scalar_logical(.args[["optimize_doc_concentration"]])
  .args[["topic_distribution_col"]] <- cast_string( .args[["topic_distribution_col"]])
  .args
}

new_ml_lda <- function(jobj) {
  new_ml_estimator(jobj, class = "ml_lda")
}

new_ml_lda_model <- function(jobj) {
  new_ml_clustering_model(
    jobj,
    is_distributed = invoke(jobj, "isDistributed"),
    describe_topics = function(max_terms_per_topic = 10) {
      max_terms_per_topic <- cast_nullable_scalar_integer(max_terms_per_topic)
      invoke(jobj, "describeTopics", max_terms_per_topic) %>%
        sdf_register()
    },
    estimated_doc_concentration = possibly_null(~ invoke(jobj, "estimatedDocConcentration")), # def
    log_likelihood = function(dataset) invoke(jobj, "logLikelihood", spark_dataframe(dataset)),
    log_perplexity = function(dataset) invoke(jobj, "logPerplexity", spark_dataframe(dataset)),
    # topicsMatrix deprecated
    topicsMatrix = function() {
      warning("`topicMatrix()` is deprecated; please use `topics_matrix()` instead.")
      possibly_null(read_spark_matrix)(jobj, "topicsMatrix") # def
    },
    topics_matrix = possibly_null(~ read_spark_matrix(jobj, "topicsMatrix")), # def
    vocab_size = invoke(jobj, "vocabSize"),
    class = "ml_lda_model")
}

#' @rdname ml_lda
#' @return \code{ml_describe_topics} returns a DataFrame with topics and their top-weighted terms.
#' @param model A fitted LDA model returned by \code{ml_lda()}.
#' @param max_terms_per_topic Maximum number of terms to collect for each topic. Default value of 10.
#' @export
ml_describe_topics <- function(model, max_terms_per_topic = 10) {
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

#' @rdname ml_lda
#' @export
ml_topics_matrix <- function(model) model$topics_matrix()
