#' Spark ML -- Naive-Bayes
#'
#' Perform regression or classification using naive bayes.
#' 
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @param lambda The (Laplace) smoothing parameter. Defaults to zero.
#' @template roxlate-ml-dots
#' 
#' @family Spark ML routines
#'
#' @export
ml_naive_bayes <- function(x,
                           response,
                           features,
                           lambda = 0,
                           ...)
{
  df <- spark_dataframe(x)
  sc <- spark_connection(df)
  
  prepare_response_features_intercept(df, response, features, NULL)
  
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)
  
  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)
  
  model <- "org.apache.spark.ml.classification.NaiveBayes"
  
  rf <- invoke_new(sc, model)
  
  model <- rf %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setSmoothing", lambda)
  
  if (only_model) return(model)
  
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
           pi = invoke(fit, "pi"),
           theta = invoke(fit, "theta"),
           features = features,
           response = response,
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_naive_bayes <- function(x, ...) {
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")
  cat(invoke(x$.model, "toString"), sep = "\n")
}
