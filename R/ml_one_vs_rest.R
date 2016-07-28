#' Spark ML -- One vs Rest
#'
#' Perform regression or classification using one vs rest.
#'
#' @template roxlate-ml-x
#' @param classifier The classifier model. Can be obtained using the \code{only_model} parameter.
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#'
#' @family Spark ML routines
#'
#' @export
ml_one_vs_rest <- function(x,
                           classifier,
                           response,
                           features)
{
  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  df <- ml_prepare_response_features_intercept(df, response, features, NULL)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)

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
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_one_vs_rest <- function(x, ...) {
}

