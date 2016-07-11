#' Spark ML -- Survival Regression
#'
#' Perform survival regression on a Spark DataFrame, using an Accelerated
#' failure time (AFT) model with potentially right-censored data.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @template roxlate-ml-intercept
#' @param censor The name of the vector that provides censoring information.
#'   This should be a numeric vector, with 0 marking uncensored data, and
#'   1 marking right-censored data.
#' @template roxlate-ml-max-iter
#' @template roxlate-ml-dots
#'
#' @family Spark ML routines
#'
#' @export
ml_survival_regression <- function(x,
                                   response,
                                   features,
                                   intercept = TRUE,
                                   censor = "censor",
                                   max.iter = 100L,
                                   ...)
{
  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  prepare_response_features_intercept(df, response, features, intercept)

  censor <- ensure_scalar_character(censor)
  max.iter <- ensure_scalar_integer(max.iter)
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)

  model <- "org.apache.spark.ml.regression.AFTSurvivalRegression"

  rf <- invoke_new(sc, model)

  model <- rf %>%
    invoke("setMaxIter", max.iter) %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setFitIntercept", as.logical(intercept)) %>%
    invoke("setCensorCol", censor)

  if (only_model) return(model)

  fit <- model %>%
    invoke("fit", tdf)

  coefficients <- fit %>%
    invoke("coefficients") %>%
    invoke("toArray")
  names(coefficients) <- features

  hasIntercept <- invoke(fit, "getFitIntercept")
  if (hasIntercept) {
    intercept <- invoke(fit, "intercept")
    coefficients <- c(coefficients, intercept)
    names(coefficients) <- c(features, "(Intercept)")
  }

  coefficients <- intercept_first(coefficients)

  ml_model("survival_regression", fit,
    features = features,
    response = response,
    intercept = intercept,
    coefficients = coefficients,
    intercept = intercept,
    model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_survival_regression <- function(x, ...) {
  ml_model_print_call(x)
}
