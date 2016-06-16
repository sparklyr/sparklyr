#' Spark ML -- Multilayer Perceptron
#'
#' Creates and trains multilayer perceptron on a \code{spark_tbl}.
#'
#' @param x An object convertable to a Spark DataFrame (typically, a \code{tbl_spark}).
#'
#' @family Spark ML routines
#'
#' @export
ml_multilayer_perceptron <- function(x) {
  scon <- spark_scon(x)
  df <- as_spark_dataframe(x)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)

  lr <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.classification.MultilayerPerceptronClassifier"
  )

  layers <- as.integer(c(4, 5, 4, 3))

  fit <- lr %>%
    spark_invoke("setLayers", envir$features) %>%
    spark_invoke("setBlockSize", envir$features) %>%
    spark_invoke("setSeed", envir$response) %>%
    spark_invoke("setMaxIter", as.logical(intercept)) %>%

    spark_invoke("fit", tdf)
}

#' @export
print.ml_multilayer_perceptron <- function(x, ...) {
}

#' @export
residuals.ml_multilayer_perceptron <- function(object, ...) {
  stop("residuals not yet available for Spark multilayer perceptron")
}

