#' Spark ML -- Principal Components Analysis
#'
#' Perform principal components analysis on a \code{spark_tbl}.
#'
#' @template roxlate-ml-x
#' @param features The columns to use in the principal components
#'   analysis. Defaults to all columns in \code{x}.
#' @template roxlate-ml-dots
#' 
#' @family Spark ML routines
#'
#' @export
ml_pca <- function(x,
                   features = dplyr::tbl_vars(x),
                   ...) {
  
  df <- spark_dataframe(x)
  sc <- spark_connection(df)
  
  prepare_features(features)
  
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, envir = envir)

  pca <- invoke_new(
    sc,
    "org.apache.spark.ml.feature.PCA"
  )

  model <- pca %>%
    invoke("setK", length(features)) %>%
    invoke("setInputCol", envir$features)
    
  if (only_model) return(model)
  
  fit <- model %>%
    invoke("fit", tdf)

  # extract principal components
  pc <- fit %>% invoke("pc")
  nrow <- pc %>% invoke("numRows")
  ncol <- pc %>% invoke("numCols")
  values <- pc %>% invoke("values") %>% as.numeric()

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
      invoke("explainedVariance") %>%
      invoke("toArray") %>%
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
  if (is.null(x$explained.variance)) {
    cat("[not available in this version of Spark]", sep = "\n")
  } else {
    print_newline()
    print(x$explained.variance)
  }
  
  print_newline()
  cat("Rotation:", sep = "\n")
  print(x$components)
}
