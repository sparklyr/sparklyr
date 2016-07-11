#' Spark ML -- Multilayer Perceptron
#'
#' Creates and trains multilayer perceptron on a \code{spark_tbl}.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @param layers A numeric vector describing the layers -- each element in the vector
#'   gives the size of a layer. For example, \code{c(4, 5, 2)} would imply three layers,
#'   with an input (feature) layer of size 4, an intermediate layer of size 5, and an
#'   output (class) layer of size 2.
#' @template roxlate-ml-max-iter
#' @template roxlate-ml-seed
#' @template roxlate-ml-dots
#'
#' @family Spark ML routines
#'
#' @export
ml_multilayer_perceptron <- function(x,
                                     response,
                                     features,
                                     layers,
                                     max.iter = 100,
                                     seed = sample(.Machine$integer.max, 1),
                                     ...)
{
  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  df <- prepare_response_features_intercept(df, response, features, NULL)

  layers <- as.integer(layers)
  max.iter <- ensure_scalar_integer(max.iter)
  seed <- ensure_scalar_integer(seed)
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)

  ml_multilayer_perceptron_validate_layers(x, response, features, layers)

  mpc <- invoke_new(
    sc,
    "org.apache.spark.ml.classification.MultilayerPerceptronClassifier"
  )

  model <- mpc %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setLayers", as.list(layers)) %>%
    invoke("setSeed", seed) %>%
    invoke("setMaxIter", max.iter)

  if (only_model) return(model)

  fit <- model %>%
    invoke("fit", tdf)

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

