#' Spark ML -- One vs Rest
#'
#' Perform regression or classification using one vs rest.
#'
#' @template roxlate-ml-x
#' @param classifier The classifier model. These model objects can be obtained through
#'   the use of the \code{only.model} parameter supplied with \code{\link{ml_options}}.
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @template roxlate-ml-options
#' @template roxlate-ml-dots
#'
#' @family Spark ML routines
#'
#' @export
ml_one_vs_rest <- function(x,
                           classifier,
                           response,
                           features,
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

  envir <- new.env(parent = emptyenv())

  envir$id <- ml.options$id.column
  df <- df %>%
    sdf_with_unique_id(envir$id) %>%
    spark_dataframe()

  tdf <- ml_prepare_dataframe(df, features, response, ml.options = ml.options, envir = envir)

  envir$model <- "org.apache.spark.ml.classification.OneVsRest"
  ovrc <- invoke_new(sc, envir$model)

  model <- ovrc %>%
    invoke("setClassifier", classifier) %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response)

  if (is.function(ml.options$model.transform))
    model <- ml.options$model.transform(model)

  fit <- model %>%
    invoke("fit", tdf)

  ml_model("one_vs_rest", fit,
           features = features,
           response = response,
           data = df,
           ml.options = ml.options,
           categorical.transformations = categorical.transformations,
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_one_vs_rest <- function(x, ...) {
}

