#' Spark ML -- Principal Components Analysis
#'
#' Perform principal components analysis on a Spark DataFrame.
#'
#' @template roxlate-ml-x
#' @param features The columns to use in the principal components
#'   analysis. Defaults to all columns in \code{x}.
#' @template roxlate-ml-options
#' @template roxlate-ml-dots
#'
#' @family Spark ML routines
#'
#' @export
ml_pca <- function(x,
                   features = dplyr::tbl_vars(x),
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

  only.model <- ensure_scalar_boolean(ml.options$only.model)

  envir <- new.env(parent = emptyenv())

  envir$id <- ml.options$id.column
  df <- df %>%
    sdf_with_unique_id(envir$id) %>%
    spark_dataframe()

  tdf <- ml_prepare_dataframe(df, features, ml.options = ml.options, envir = envir)

  envir$model <- "org.apache.spark.ml.feature.PCA"
  pca <- invoke_new(sc, envir$model)

  model <- pca %>%
    invoke("setK", length(features)) %>%
    invoke("setInputCol", envir$features)

  if (is.function(ml.options$model.transform))
    model <- ml.options$model.transform(model)

  if (only.model)
    return(model)

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
           data = df,
           ml.options = ml.options,
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
