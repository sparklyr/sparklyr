#' Spark ML -- Latent Dirichlet Allocation
#'
#' Fit a Latent Dirichlet Allocation (LDA) model to a Spark DataFrame.
#'
#' @param x An object convertable to a Spark DataFrame (typically, a \code{tbl_spark}).
#' @param features The columns to use in the principal components
#'   analysis. Defaults to all columns in \code{x}.
#' @param ... Other arguments passed on to methods.
#' 
#' @family Spark ML routines
#'
#' @export
ml_lda <- function(x,
                   features = dplyr::tbl_vars(x),
                   ...) {
  
  df <- spark_dataframe(x)
  sc <- spark_connection(df)
  
  features <- as.character(features)
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)
  
  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, envir = envir)
  
  lda <- invoke_new(
    sc,
    "org.apache.spark.ml.clustering.LDA"
  )
  
  model <- lda %>%
    invoke("setK", length(features)) %>%
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
