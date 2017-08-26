#' Spark ML -- Latent Dirichlet Allocation
#'
#' Fit a Latent Dirichlet Allocation (LDA) model to a Spark DataFrame.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-features
#' @param k The number of topics to estimate.
#' @template roxlate-ml-options
#' @template roxlate-ml-dots
#' @param alpha Concentration parameter for the prior placed on documents' distributions over topics. This is a singleton which is replicated to a vector of length \code{k} in fitting (as currently EM optimizer only supports symmetric distributions, so all values in the vector should be the same). For Expectation-Maximization optimizer values should be > 1.0.
#' By default \code{alpha = (50 / k) + 1}, where \code{50/k} is common in LDA libraries and +1 follows from Asuncion et al. (2009), who recommend a +1 adjustment for EM.
#' @param beta Concentration parameter for the prior placed on topics' distributions over terms. For Expectation-Maximization optimizer value should be > 1.0 and by default \code{beta = 0.1 + 1}, where 0.1 gives a small amount of smoothing and +1 follows Asuncion et al. (2009), who recommend a +1 adjustment for EM.
#' @param optimizer The optimizer, either \code{online} for Online Variational Bayes or \code{em} for Expectation-Maximization.
#' @param max.iterations Maximum number of iterations.
#' @examples
#' \dontrun{
#' library(janeaustenr)
#' library(sparklyr)
#' library(dplyr)
#'
#' sc <- spark_connect(master = "local")
#'
#' austen_books <- austen_books()
#' books_tbl <- sdf_copy_to(sc, austen_books, overwrite = TRUE)
#' first_tbl <- books_tbl %>% filter(nchar(text) > 0) %>% head(100)
#'
#' first_tbl %>%
#'   ft_tokenizer("text", "tokens") %>%
#'   ft_count_vectorizer("tokens", "features") %>%
#'   ml_lda("features", k = 4)
#' }
#' @references
#' Original LDA paper (journal version): Blei, Ng, and Jordan. "Latent Dirichlet Allocation." JMLR, 2003.
#'
#' Asuncion et al. (2009)
#'
#'
#' @family Spark ML routines
#'
#'
#' @note
#' The topics' distributions over terms are called "beta" in the original LDA paper by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
#'
#' For terminology used in LDA model see \href{https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.clustering.LDA}{Spark LDA documentation}.
#'
#' Expectation-Maximization: Asuncion et al. \href{http://arxiv.org/pdf/1205.2662.pdf}{On Smoothing and Inference for Topic Models.} Uncertainty in Artificial Intelligence, 2009.
#'
#' @export
#' @importFrom dplyr tbl_vars
ml_lda <- function(x,
                   features = tbl_vars(x),
                   k = length(features),
                   alpha = (50 / k) + 1,
                   beta = 0.1 + 1,
                   optimizer = "online",
                   max.iterations = 20,
                   ml.options = ml_options(),
                   ...)
{
  ml_backwards_compatibility_api()

  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  df <- ml_prepare_features(
    x = df,
    features = features,
    envir = environment(),
    ml.options = ml.options
  )

  alpha                <- ensure_scalar_double(alpha)
  beta                 <- ensure_scalar_double(beta)
  k                    <- ensure_scalar_integer(k)
  optimizer            <- ensure_scalar_character(optimizer)
  max.iterations       <- ensure_scalar_integer(max.iterations)
  only.model <- ensure_scalar_boolean(ml.options$only.model)

  stopifnot(alpha > 1)

  envir <- new.env(parent = emptyenv())

  envir$id <- ml.options$id.column
  df <- df %>%
    sdf_with_unique_id(envir$id) %>%
    spark_dataframe()

  tdf <- ml_prepare_dataframe(df, features, ml.options = ml.options, envir = envir)

  envir$model <- "org.apache.spark.ml.clustering.LDA"
  lda <- invoke_new(sc, envir$model)

  model <- lda %>%
    invoke("setK", k) %>%
    invoke("setOptimizer", optimizer) %>%
    invoke("setMaxIter", max.iterations) %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setTopicConcentration", as.double(beta)) %>%
    invoke("setDocConcentration", as.double(alpha))

  if (is.function(ml.options$model.transform))
    model <- ml.options$model.transform(model)

  if (only.model)
    return(model)

  fit <- model %>%
    invoke("fit", tdf)

  topics.matrix <- read_spark_matrix(fit, "topicsMatrix")
  topics.description <- invoke(fit, "describeTopics") %>% sdf_collect()
  estimated.doc.concentration <- read_spark_vector(fit, "estimatedDocConcentration")

  ml_model("lda", fit,
    features = features,
    topics.matrix = topics.matrix,
    topics.description = topics.description,
    estimated.doc.concentration = estimated.doc.concentration,
    data = df,
    ml.options = ml.options,
    model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_lda <- function(x, ...) {

  header <- sprintf(
    "An LDA model fit on %s features",
    length(x$features)
  )

  cat(header, sep = "\n")
  print_newline()

  cat("Topics Matrix:", sep = "\n")
  print(x$topics.matrix)
  print_newline()

  cat("Estimated Document Concentration:", sep = "\n")
  print(x$estimated.doc.concentration)
  print_newline()

}
