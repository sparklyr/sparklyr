#' Spark ML -- Naive-Bayes
#'
#' Perform regression or classification using naive bayes.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @param lambda The (Laplace) smoothing parameter. Defaults to zero.
#' @template roxlate-ml-options
#' @template roxlate-ml-dots
#'
#' @family Spark ML routines
#'
#' @export
ml_naive_bayes <- function(x,
                           response,
                           features,
                           lambda = 0,
                           ml.options = ml_options(),
                           ...)
{
  ml_backwards_compatibility_api()

  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  categorical.transformations <- new.env(parent = emptyenv())
  df <- ml_prepare_response_features_intercept(
    x = df,
    response = response,
    features = features,
    intercept = NULL,
    envir = environment(),
    categorical.transformations = categorical.transformations,
    ml.options = ml.options
  )

  only.model <- ensure_scalar_boolean(ml.options$only.model)

  envir <- new.env(parent = emptyenv())

  envir$id <- ml.options$id.column
  df <- df %>%
    sdf_with_unique_id(envir$id) %>%
    spark_dataframe()

  tdf <- ml_prepare_dataframe(df, features, response, ml.options = ml.options, envir = envir)

  envir$model <- "org.apache.spark.ml.classification.NaiveBayes"
  rf <- invoke_new(sc, envir$model)

  model <- rf %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setSmoothing", lambda)

  if (is.function(ml.options$model.transform))
    model <- ml.options$model.transform(model)

  if (only.model)
    return(model)

  fit <- model %>%
    invoke("fit", tdf)

  pi <- fit %>%
    invoke("pi") %>%
    invoke("toArray")
  names(pi) <- envir$labels

  thetaMatrix <- fit %>% invoke("theta")
  thetaValues <- thetaMatrix %>% invoke("toArray")
  theta <- matrix(thetaValues,
                  nrow = invoke(thetaMatrix, "numRows"),
                  ncol = invoke(thetaMatrix, "numCols"))
  rownames(theta) <- envir$labels
  colnames(theta) <- features

  ml_model("naive_bayes", fit,
           pi = pi,
           theta = t(theta),
           features = features,
           response = response,
           data = df,
           ml.options = ml.options,
           categorical.transformations = categorical.transformations,
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_naive_bayes <- function(x, ...) {

  ml_model_print_call(x)
  print_newline()

  printf("A-priority probabilities:\n")
  print(exp(x$pi))
  print_newline()

  printf("Conditional probabilities:\n")
  print(exp(x$theta))
  print_newline()

  x
}

#' @export
summary.ml_model_naive_bayes <- function(object, ...) {
  print(object, ...)
  object
}
