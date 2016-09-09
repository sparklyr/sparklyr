#' Spark ML -- Multilayer Perceptron
#'
#' Creates and trains multilayer perceptron on a Spark DataFrame.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @param layers A numeric vector describing the layers -- each element in the vector
#'   gives the size of a layer. For example, \code{c(4, 5, 2)} would imply three layers,
#'   with an input (feature) layer of size 4, an intermediate layer of size 5, and an
#'   output (class) layer of size 2.
#' @template roxlate-ml-iter-max
#' @template roxlate-ml-seed
#' @template roxlate-ml-options
#' @template roxlate-ml-dots
#'
#' @family Spark ML routines
#'
#' @export
ml_multilayer_perceptron <- function(x,
                                     response,
                                     features,
                                     layers,
                                     iter.max = 100,
                                     seed = sample(.Machine$integer.max, 1),
                                     ml.options = ml_options(),
                                     ...)
{
  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  ml_backwards_compatibility_api()

  categorical.transformations <- new.env(parent = emptyenv())
  df <- ml_prepare_response_features_intercept(
    df,
    response,
    features,
    NULL,
    environment(),
    categorical.transformations
  )

  layers <- as.integer(layers)
  iter.max <- ensure_scalar_integer(iter.max)
  seed <- ensure_scalar_integer(seed)
  only.model <- ensure_scalar_boolean(ml.options$only.model)

  envir <- new.env(parent = emptyenv())

  envir$id <- ml.options$id.column
  df <- df %>%
    sdf_with_unique_id(envir$id) %>%
    spark_dataframe()

  tdf <- ml_prepare_dataframe(df, features, response, ml.options = ml.options, envir = envir)

  ml_multilayer_perceptron_validate_layers(x, response, features, layers)

  envir$model <- "org.apache.spark.ml.classification.MultilayerPerceptronClassifier"
  mpc <- invoke_new(sc, envir$model)

  model <- mpc %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setLayers", as.list(layers)) %>%
    invoke("setSeed", seed) %>%
    invoke("setMaxIter", iter.max)

  if (is.function(ml.options$model.transform))
    model <- ml.options$model.transform(model)

  if (only.model)
    return(model)

  fit <- model %>%
    invoke("fit", tdf)

  ml_model("multilayer_perceptron", fit,
    features = features,
    response = response,
    data = df,
    ml.options = ml.options,
    categorical.transformations = categorical.transformations,
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

