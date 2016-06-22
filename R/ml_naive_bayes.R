#' Spark ML -- Naive-Bayes
#'
#' Perform regression or classification using naive bayes.
#'
#' @param x An object convertable to a Spark DataFrame (typically, a \code{tbl_spark}).
#' @param response The name of the response vector.
#' @param features The names of features (terms) included in the model.
#'
#' @family Spark ML routines
#'
#' @export
ml_naive_bayes <- function(x,
                           response,
                           features)
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)
  
  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)
  
  schema <- spark_dataframe_schema(df)
  model <- "org.apache.spark.ml.classification.NaiveBayes"
  
  rf <- sparkapi_invoke_new(sc, model)
  
  fit <- rf %>%
    sparkapi_invoke("setFeaturesCol", envir$features) %>%
    sparkapi_invoke("setLabelCol", envir$response) %>%
    sparkapi_invoke("fit", tdf)
  
  ml_model("ml_naive_bayes", fit,
           features = features,
           response = response,
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_naive_bayes <- function(x, ...) {
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")
  cat(sparkapi_invoke(x$.model, "toString"), sep = "\n")
}
