#' Spark ML -- Linear Regression
#'
#' Perform linear regression on a Spark DataFrame.
#'
#' @template ml-regression
#'
#' @family Spark ML routines
#'
#' @export
ml_linear_regression <- function(x,
                                 response,
                                 features,
                                 intercept = TRUE,
                                 alpha = 0,
                                 lambda = 0)
{
  df <- sparkapi_dataframe(x)
  sc <- sparkapi_connection(df)
  
  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)

  lr <- sparkapi_invoke_new(
    sc,
    "org.apache.spark.ml.regression.LinearRegression"
  )

  fit <- lr %>%
    sparkapi_invoke("setMaxIter", 10L) %>%
    sparkapi_invoke("setFeaturesCol", envir$features) %>%
    sparkapi_invoke("setLabelCol", envir$response) %>%
    sparkapi_invoke("setFitIntercept", as.logical(intercept)) %>%
    sparkapi_invoke("setElasticNetParam", as.double(alpha)) %>%
    sparkapi_invoke("setRegParam", as.double(lambda)) %>%
    sparkapi_invoke("fit", tdf)

  coefficients <- fit %>%
    sparkapi_invoke("coefficients") %>%
    sparkapi_invoke("toArray")
  names(coefficients) <- features

  has_intercept <- sparkapi_invoke(fit, "getFitIntercept")
  if (has_intercept) {
    intercept <- sparkapi_invoke(fit, "intercept")
    coefficients <- c(coefficients, intercept)
    names(coefficients) <- c(features, "(Intercept)")
  }

  summary <- sparkapi_invoke(fit, "summary")

  errors <- try_null(sparkapi_invoke(summary, "coefficientStandardErrors"))
  if (!is.null(errors))
    names(errors) <- names(coefficients)

  tvalues <- try_null(sparkapi_invoke(summary, "tValues"))
  if (!is.null(tvalues))
    names(tvalues) <- names(coefficients)

  ml_model("linear_regression", fit,
           features = features,
           response = response,
           coefficients = coefficients,
           standard.errors = errors,
           t.values = tvalues,
           p.values = try_null(as.numeric(sparkapi_invoke(summary, "pValues"))),
           explained.variance = sparkapi_invoke(summary, "explainedVariance"),
           mean.absolute.error = sparkapi_invoke(summary, "meanAbsoluteError"),
           mean.squared.error = sparkapi_invoke(summary, "meanSquaredError"),
           r.squared = sparkapi_invoke(summary, "r2"),
           root.mean.squared.error = sparkapi_invoke(summary, "rootMeanSquaredError"),
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_linear_regression <- function(x, ...) {

  # report what model was fitted
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")

  # report coefficients
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}

#' @export
summary.ml_model_linear_regression <- function(object, ...) {
  ml_model_print_call(object)
  print_newline()
  ml_model_print_residuals(object)
  print_newline()
  ml_model_print_coefficients(object)
  print_newline()
}
