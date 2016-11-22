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
#' @template roxlate-ml-iter-max
#' @template roxlate-ml-options
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
                                   iter.max = 100L,
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
    intercept = intercept,
    envir = environment(),
    categorical.transformations = categorical.transformations,
    ml.options = ml.options
  )

  censor <- ensure_scalar_character(censor)
  iter.max <- ensure_scalar_integer(iter.max)
  only.model <- ensure_scalar_boolean(ml.options$only.model)

  envir <- new.env(parent = emptyenv())

  envir$id <- ml.options$id.column
  df <- df %>%
    sdf_with_unique_id(envir$id) %>%
    spark_dataframe()

  tdf <- ml_prepare_dataframe(df, features, response, ml.options = ml.options, envir = envir)

  envir$model <- "org.apache.spark.ml.regression.AFTSurvivalRegression"
  rf <- invoke_new(sc, envir$model)

  model <- rf %>%
    invoke("setMaxIter", iter.max) %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setFitIntercept", as.logical(intercept)) %>%
    invoke("setCensorCol", censor)

  if (is.function(ml.options$model.transform))
    model <- ml.options$model.transform(model)

  if (only.model)
    return(model)

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
  scale <- invoke(fit, "scale")

  ml_model("survival_regression", fit,
    features = features,
    response = response,
    intercept = intercept,
    coefficients = coefficients,
    intercept = intercept,
    scale = scale,
    data = df,
    ml.options = ml.options,
    categorical.transformations = categorical.transformations,
    model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_survival_regression <- function(x, ...) {
  ml_model_print_call(x)
}
