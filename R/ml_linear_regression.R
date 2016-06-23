#' Spark ML -- Linear Regression
#'
#' Perform linear regression on a Spark DataFrame.
#'
#' @template roxlate-ml-regression
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
  
  response <- ensure_scalar_character(response)
  features <- as.character(features)
  intercept <- ensure_scalar_boolean(intercept)
  alpha <- ensure_scalar_double(alpha)
  lambda <- ensure_scalar_double(lambda)
  
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
    sparkapi_invoke("setFitIntercept", intercept) %>%
    sparkapi_invoke("setElasticNetParam", alpha) %>%
    sparkapi_invoke("setRegParam", lambda) %>%
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

  summary <- sparkapi_invoke(fit, "summary")

  errors <- try_null(sparkapi_invoke(summary, "coefficientStandardErrors"))
  if (!is.null(errors))
    names(errors) <- names(coefficients)

  tvalues <- try_null(sparkapi_invoke(summary, "tValues"))
  if (!is.null(tvalues))
    names(tvalues) <- names(coefficients)
  
  pvalues <- try_null(as.numeric(sparkapi_invoke(summary, "pValues")))
  if (!is.null(pvalues))
    names(pvalues) <- names(coefficients)
  
  # reorder coefficient names to place intercept first if available
  coefficients <- intercept_first(coefficients)
  errors <- intercept_first(errors)
  tvalues <- intercept_first(tvalues)
  pvalues <- intercept_first(pvalues)

  ml_model("linear_regression", fit,
           features = features,
           response = response,
           coefficients = coefficients,
           standard.errors = errors,
           t.values = tvalues,
           p.values = pvalues,
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
  ml_model_print_call(x)
  print_newline()
  ml_model_print_coefficients(x)
  print_newline()
}

#' @export
summary.ml_model_linear_regression <- function(object, ...) {
  
  columns <- c("coefficients", "standard.errors", "t.values", "p.values")
  values <- as.list(object[columns])
  matrix <- do.call(base::cbind, values)
  colnames(matrix) <- c("Estimate", "Std. Error", "t value", "Pr(>|t|)")
  
  ml_model_print_call(object)
  print_newline()
  ml_model_print_residuals(object)
  print_newline()
  
  cat("Coefficients:", sep = "\n")
  stats::printCoefmat(matrix)
  print_newline()
  
  cat(paste("R-Squared:", signif(object$r.squared, 4)), sep = "\n")
  cat(paste("Root Mean Squared Error:", signif(object$root.mean.squared.error, 4)), sep = "\n")
}
