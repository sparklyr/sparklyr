#' Spark ML -- Multilayer Perceptron
#'
#' Creates and trains multilayer perceptron on a \code{spark_tbl}.
#'
#' @param x An object convertable to a Spark DataFrame (typically, a \code{tbl_spark}).
#' @param response The name of the response vector.
#' @param features The names of features (terms) to use as linear predictors
#'   for the response.
#' @param layers A numeric vector describing the layers -- each element in the vector
#'   gives the size of a layer. For example, \code{c(4, 5, 2)} would imply three layers,
#'   with an input (feature) layer of size 4, an intermediate layer of size 5, and an
#'   output (class) layer of size 2.
#' @param max.iter Maximum number of iterations to perform in model fit.
#' @param seed A random seed.
#'
#' @family Spark ML routines
#'
#' @export
ml_multilayer_perceptron <- function(x,
                                     response,
                                     features,
                                     layers,
                                     max.iter = 100,
                                     seed = sample(.Machine$integer.max, 1))
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)
  
  response <- ensure_scalar_character(response)
  features <- as.character(features)
  layers <- as.integer(layers)
  max.iter <- ensure_scalar_integer(max.iter)
  seed <- ensure_scalar_integer(seed)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)

  ml_multilayer_perceptron_validate_layers(x, response, features, layers)

  mpc <- sparkapi_invoke_new(
    sc,
    "org.apache.spark.ml.classification.MultilayerPerceptronClassifier"
  )

  fit <- mpc %>%
    sparkapi_invoke("setFeaturesCol", envir$features) %>%
    sparkapi_invoke("setLabelCol", envir$response) %>%
    sparkapi_invoke("setLayers", as.list(layers)) %>%
    sparkapi_invoke("setSeed", seed) %>%
    sparkapi_invoke("setMaxIter", max.iter) %>%
    sparkapi_invoke("fit", tdf)

  ml_model("multilayer_perceptron", fit,
    features = features,
    response = response,
    model.parameters = as.list(envir)
  )
}

ml_multilayer_perceptron_validate_layers <- function(x,
                                                     response,
                                                     features,
                                                     layers)
{
  if (!is.numeric(layers) || length(layers) < 2)
    stop("'layers' should be a numeric vector of length >= 2")

  if (length(features) != layers[[1]])
    stop("the first element of 'layers' should be the same length as the 'features' vector")

  # TODO: validate length of last layer?
  TRUE
}

#' @export
print.ml_multilayer_perceptron <- function(x, ...) {
}

#' @export
residuals.ml_multilayer_perceptron <- function(object, ...) {
  stop("residuals not yet available for Spark multilayer perceptron")
}

