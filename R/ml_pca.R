#' Spark ML -- Principal Components Analysis
#'
#' Perform principal components analysis on a \code{spark_tbl}.
#'
#' @param x An object convertable to a Spark DataFrame (typically, a \code{tbl_spark}).
#' @param features The columns to use in the principal components
#'   analysis. Defaults to all columns in \code{x}.
#'
#' @family Spark ML routines
#'
#' @export
ml_pca <- function(x, features = dplyr::tbl_vars(x)) {
  
  df <- sparkapi_dataframe(x)
  sc <- spark_connection(df)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, envir = envir)

  pca <- spark_invoke_new(
    sc,
    "org.apache.spark.ml.feature.PCA"
  )

  fit <- pca %>%
    spark_invoke("setK", length(features)) %>%
    spark_invoke("setInputCol", envir$features) %>%
    spark_invoke("fit", tdf)

  # extract principal components
  pc <- fit %>% spark_invoke("pc")
  nrow <- pc %>% spark_invoke("numRows")
  ncol <- pc %>% spark_invoke("numCols")
  values <- pc %>% spark_invoke("values") %>% as.numeric()

  # convert to matrix
  components <- matrix(values, nrow = nrow, ncol = ncol)

  # set names
  pcNames <- paste("PC", seq_len(ncol(components)), sep = "")
  rownames(components) <- features
  colnames(components) <- pcNames

  # get explained variance as vector
  # (NOTE: not available in Spark 1.6.1)
  explainedVariance <- try_null({
    fit %>%
      spark_invoke("explainedVariance") %>%
      spark_invoke("toArray") %>%
      as.numeric()
  })

  if (!is.null(explainedVariance))
    names(explainedVariance) <- pcNames

  ml_model("pca", fit,
           components = components,
           explained.variance = explainedVariance,
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_pca <- function(x, ...) {
  cat("Explained variance:", sep = "\n")
  print(x$explained.variance)
  cat("\nRotation:", sep = "\n")
  print(x$components)
}
