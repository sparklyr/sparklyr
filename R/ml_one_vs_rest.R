#' Spark ML -- One vs Rest
#'
#' Perform regression or classification using one vs rest.
#'
#' @template roxlate-ml-x
#' @param classifier The classifier model. Can be obtained using the \code{only_model} parameter.
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
  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  categorical.transformations <- new.env(parent = emptyenv())
  df <- ml_prepare_response_features_intercept(
    df,
    response,
    features,
    NULL,
    environment(),
    categorical.transformations
  )

  envir <- new.env(parent = emptyenv())

  envir$id <- ml.options$id
  df <- df %>%
    sdf_with_unique_id(envir$id) %>%
    spark_dataframe()

  tdf <- ml_prepare_dataframe(df, features, response, ml.options = ml.options, envir = envir)

  envir$model <- "org.apache.spark.ml.classification.OneVsRest"
  ovrc <- invoke_new(sc, envir$model)

  fit <- ovrc %>%
    invoke("setClassifier", classifier) %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("fit", tdf)

  ml_model("one_vs_rest", fit,
           features = features,
           response = response,
           data = df,
           categorical.transformations = categorical.transformations,
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_one_vs_rest <- function(x, ...) {
}

