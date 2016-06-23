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
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)
  
  response <- ensure_scalar_character(response)
  features <- as.character(features)
  
  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)
  
  ovrc <- sparkapi_invoke_new(
    sc,
    "org.apache.spark.ml.classification.OneVsRest"
  )
  
  fit <- ovrc %>%
    sparkapi_invoke("setClassifier", classifier) %>%
    sparkapi_invoke("fit", tdf)
  
  ml_model("one_vs_rest", fit,
           features = features,
           response = response,
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_one_vs_rest <- function(x, ...) {
}

