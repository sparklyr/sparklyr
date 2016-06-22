#' Spark ML -- Survival Regression
#'
#' Perform survival regression on a Spark DataFrame, using an Accelerated
#' failure time (AFT) model with potentially right-censored data.
#'
#' @param x An object convertable to a Spark DataFrame (typically, a \code{tbl_spark}).
#' @param response The name of the response vector.
#' @param features The names of features (terms) to use in the model.
#' @param intercept Fit the model with an intercept term?
#' @param censor The name of the vector that provides censoring information.
#'   This should be a numeric vector, with 0 marking uncensored data, and
#'   1 marking right-censored data.
#'
#' @family Spark ML routines
#'
#' @export
ml_survival_regression <- function(x,
                                   response,
                                   features,
                                   intercept = TRUE,
                                   censor = "censor")
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)
  
  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)
  
  model <- "org.apache.spark.ml.regression.AFTSurvivalRegression"
  
  rf <- sparkapi_invoke_new(sc, model)
  
  fit <- rf %>%
    sparkapi_invoke("setFeaturesCol", envir$features) %>%
    sparkapi_invoke("setLabelCol", envir$response) %>%
    sparkapi_invoke("setFitIntercept", as.logical(intercept)) %>%
    sparkapi_invoke("setCensorCol", censor) %>%
    sparkapi_invoke("fit", tdf)
  
  coefficients <- fit %>%
    sparkapi_invoke("coefficients") %>%
    sparkapi_invoke("toArray")
  names(coefficients) <- features
  
  hasIntercept <- sparkapi_invoke(fit, "getFitIntercept")
  if (hasIntercept) {
    intercept <- sparkapi_invoke(fit, "intercept")
    coefficients <- c(coefficients, intercept)
    names(coefficients) <- c(features, "(Intercept)")
  }
  
  coefficients <- intercept_first(coefficients)
  
  ml_model("survival_regression", fit,
           features = features,
           response = response,
           coefficients = coefficients,
           intercept = intercept,
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_survival_regression <- function(x, ...) {
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")
  cat(sparkapi_invoke(x$.model, "toString"), sep = "\n")
}
