spark_ml_pca <- function(x, features = NULL) {
  scon <- spark_scon(x)
  df <- as_spark_dataframe(x)

  # collect vectors of interest into single column
  if (is.null(features))
    features <- as.list(spark_invoke(df, "columns"))

  tdf <- spark_assemble_vector(scon, df, as.list(features), "features")

  # invoke pca
  pca <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.feature.PCA"
  )

  model <- pca %>%
    spark_invoke("setK", length(features)) %>%
    spark_invoke("setInputCol", "features") %>%
    spark_invoke("fit", tdf)

  model
}

as_pca_result <- function(model, features) {

  # extract principal components
  pc <- model %>% spark_invoke("pc")
  nrow <- pc %>% spark_invoke("numRows")
  ncol <- pc %>% spark_invoke("numCols")
  values <- pc %>% spark_invoke("values") %>% as.numeric()

  # convert to matrix
  components <- matrix(values, nrow = nrow, ncol = ncol)

  # get explained variance as vector
  explainedVariance <- model %>%
    spark_invoke("explainedVariance") %>%
    spark_invoke("toArray") %>%
    as.numeric()

  # set names
  rownames(components) <- features
  names(explainedVariance) <- features

  list(
    components = components,
    explained.variance = explainedVariance
  )

}

#' Perform Principal Components Analaysis using spark.ml
#'
#' @param x A \code{tbl_spark}.
#' @param features The columns to use in the principal components
#'   analysis. Defaults to all columns in \code{x}.
#' @export
ml_pca <- function(x, features = NULL) {
  model <- spark_ml_pca(x, features)
  as_pca_result(model, features)
}

