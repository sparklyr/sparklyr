#' Spark ML -- One vs Rest
#'
#' Perform regression or classification using one vs rest.
#'
#' @param x An object convertable to a Spark DataFrame (typically, a \code{tbl_spark}).
#' @param classifier The classifier model. Can be obtained using the \code{only_model} parameter.
#' @param response The name of the response vector.
#' @param features The names of features (terms) to use as linear predictors
#'   for the response.
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
  
  prepare_response_features_intercept(df, response, features, NULL)
  
  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)
  
  ovrc <- invoke_new(
    sc,
    "org.apache.spark.ml.classification.OneVsRest"
  )
  
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

