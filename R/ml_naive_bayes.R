#' Spark ML -- Naive-Bayes
#'
#' Perform regression or classification using naive bayes.
#'
#' @param x An object convertable to a Spark DataFrame (typically, a \code{tbl_spark}).
#' @param response The name of the response vector.
#' @param features The names of features (terms) included in the model.
#' @param lambda The (Laplace) smoothing parameter. Defaults to zero.
#'
#' @family Spark ML routines
#'
#' @export
ml_naive_bayes <- function(x,
                           response,
                           features,
                           lambda = 0)
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)
  
  response <- ensure_scalar_character(response)
  features <- as.character(features)
  
  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)
  
  model <- "org.apache.spark.ml.classification.NaiveBayes"
  
  rf <- sparkapi_invoke_new(sc, model)
  
  fit <- rf %>%
    sparkapi_invoke("setFeaturesCol", envir$features) %>%
    sparkapi_invoke("setLabelCol", envir$response) %>%
    sparkapi_invoke("setSmoothing", lambda) %>%
    sparkapi_invoke("fit", tdf)
  
  pi <- fit %>%
    sparkapi_invoke("pi") %>%
    sparkapi_invoke("toArray")
  names(pi) <- envir$labels
  
  thetaMatrix <- fit %>% sparkapi_invoke("theta")
  thetaValues <- thetaMatrix %>% sparkapi_invoke("toArray")
  theta <- matrix(thetaValues,
                  nrow = sparkapi_invoke(thetaMatrix, "numRows"),
                  ncol = sparkapi_invoke(thetaMatrix, "numCols"))
  rownames(theta) <- envir$labels
  colnames(theta) <- features
  
  ml_model("naive_bayes", fit,
           pi = sparkapi_invoke(fit, "pi"),
           theta = sparkapi_invoke(fit, "theta"),
           features = features,
           response = response,
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_naive_bayes <- function(x, ...) {
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")
  cat(sparkapi_invoke(x$.model, "toString"), sep = "\n")
}
