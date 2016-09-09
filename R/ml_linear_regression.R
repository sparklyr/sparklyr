#' Spark ML -- Linear Regression
#'
#' Perform linear regression on a Spark DataFrame.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @template roxlate-ml-intercept
#' @template roxlate-ml-regression-penalty
#' @template roxlate-ml-iter-max
#' @template roxlate-ml-options
#' @template roxlate-ml-dots
#'
#' @family Spark ML routines
#'
#' @export
ml_linear_regression <- function(x,
                                 response,
                                 features,
                                 intercept = TRUE,
                                 alpha = 0,
                                 lambda = 0,
                                 iter.max = 100L,
                                 ml.options = ml_options(),
                                 ...)
{
  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  ml_backwards_compatibility_api()

  categorical.transformations <- new.env(parent = emptyenv())
  df <- ml_prepare_response_features_intercept(
    df,
    response,
    features,
    intercept,
    environment(),
    categorical.transformations
  )

  alpha <- ensure_scalar_double(alpha)
  lambda <- ensure_scalar_double(lambda)
  iter.max <- ensure_scalar_integer(iter.max)
  only.model <- ensure_scalar_boolean(ml.options$only.model)

  envir <- new.env(parent = emptyenv())

  envir$id <- ml.options$id.column
  df <- df %>%
    sdf_with_unique_id(envir$id) %>%
    spark_dataframe()

  tdf <- ml_prepare_dataframe(df, features, response, ml.options = ml.options, envir = envir)

  envir$model <- "org.apache.spark.ml.regression.LinearRegression"
  lr <- invoke_new(sc, envir$model)

  model <- lr %>%
    invoke("setMaxIter", iter.max) %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setFitIntercept", as.logical(intercept)) %>%
    invoke("setElasticNetParam", as.double(alpha)) %>%
    invoke("setRegParam", as.double(lambda))

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

  summary <- invoke(fit, "summary")

  errors <- try_null(invoke(summary, "coefficientStandardErrors"))
  if (!is.null(errors))
    names(errors) <- names(coefficients)

  tvalues <- try_null(invoke(summary, "tValues"))
  if (!is.null(tvalues))
    names(tvalues) <- names(coefficients)

  pvalues <- try_null(as.numeric(invoke(summary, "pValues")))
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
           intercept = intercept,
           coefficients = coefficients,
           standard.errors = errors,
           t.values = tvalues,
           p.values = pvalues,
           explained.variance = invoke(summary, "explainedVariance"),
           mean.absolute.error = invoke(summary, "meanAbsoluteError"),
           mean.squared.error = invoke(summary, "meanSquaredError"),
           r.squared = invoke(summary, "r2"),
           root.mean.squared.error = invoke(summary, "rootMeanSquaredError"),
           data = df,
           ml.options = ml.options,
           categorical.transformations = categorical.transformations,
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

  ml_model_print_call(object)
  print_newline()
  ml_model_print_residuals(object, residuals.header = "Deviance Residuals:")
  print_newline()
  ml_model_print_coefficients_detailed(object)
  print_newline()

  cat(paste("R-Squared:", signif(object$r.squared, 4)), sep = "\n")
  cat(paste("Root Mean Squared Error:", signif(object$root.mean.squared.error, 4)), sep = "\n")
}
