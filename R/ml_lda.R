#' Spark ML -- Latent Dirichlet Allocation
#'
#' Fit a Latent Dirichlet Allocation (LDA) model to a Spark DataFrame.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-features
#' @param k The number of topics to estimate.
#' @template roxlate-ml-options
#' @template roxlate-ml-dots
#'
#' @family Spark ML routines
#'
#' @export
ml_lda <- function(x,
                   features = dplyr::tbl_vars(x),
                   k = length(features),
                   ml.options = NULL,
                   ...)
{
  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  ml_prepare_features(df, features)

  k <- ensure_scalar_integer(k)
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)

  envir <- new.env(parent = emptyenv())

  envir$id <- random_string("id_")
  df <- df %>%
    sdf_with_unique_id(envir$id) %>%
    spark_dataframe()

  tdf <- ml_prepare_dataframe(df, features, envir = envir)

  envir$model <- "org.apache.spark.ml.clustering.LDA"
  lda <- invoke_new(sc, envir$model)

  model <- lda %>%
    invoke("setK", k) %>%
    invoke("setFeaturesCol", envir$features)

  if (only_model) return(model)

  fit <- model %>%
    invoke("fit", tdf)

  topics.matrix <- read_spark_matrix(fit, "topicsMatrix")
  estimated.doc.concentration <- read_spark_vector(fit, "estimatedDocConcentration")

  ml_model("lda", fit,
    features = features,
    topics.matrix = topics.matrix,
    estimated.doc.concentration = estimated.doc.concentration,
    data = df,
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
