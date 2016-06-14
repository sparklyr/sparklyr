spark_ml_linear_regression <- function(x, response, features, intercept = TRUE,
                                       alpha = 0, lambda = 0)
{
  scon <- spark_scon(x)
  df <- as_spark_dataframe(x)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)

  lr <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.regression.LinearRegression"
  )

  fit <- lr %>%
    spark_invoke("setMaxIter", 10L) %>%
    spark_invoke("setFeaturesCol", envir$features) %>%
    spark_invoke("setLabelCol", envir$response) %>%
    spark_invoke("setFitIntercept", as.logical(intercept)) %>%
    spark_invoke("setElasticNetParam", as.double(alpha)) %>%
    spark_invoke("setRegParam", as.double(lambda)) %>%
    spark_invoke("fit", tdf)

  coefficients <- fit %>%
    spark_invoke("coefficients") %>%
    spark_invoke("toArray")
  names(coefficients) <- features

  has_intercept <- spark_invoke(fit, "getFitIntercept")
  if (has_intercept) {
    intercept <- spark_invoke(fit, "intercept")
    coefficients <- c(coefficients, intercept)
    names(coefficients) <- c(features, "(Intercept)")
  }

  summary <- spark_invoke(fit, "summary")

  errors <- try_null(spark_invoke(summary, "coefficientStandardErrors"))
  if (!is.null(errors))
    names(errors) <- names(coefficients)

  tvalues <- try_null(spark_invoke(summary, "tValues"))
  if (!is.null(tvalues))
    names(tvalues) <- names(coefficients)

  ml_model("linear_regression", fit,
    features = features,
    response = response,
    coefficients = coefficients,
    standard.errors = errors,
    t.values = tvalues,
    p.values = try_null(as.numeric(spark_invoke(summary, "pValues"))),
    explained.variance = spark_invoke(summary, "explainedVariance"),
    mean.absolute.error = spark_invoke(summary, "meanAbsoluteError"),
    mean.squared.error = spark_invoke(summary, "meanSquaredError"),
    r.squared = spark_invoke(summary, "r2"),
    root.mean.squared.error = spark_invoke(summary, "rootMeanSquaredError"),
    model.parameters = as.list(envir)
  )
}

#' Spark ML -- Linear Regression
#'
#' Perform linear regression on a \code{spark_tbl}.
#'
#' @template ml-regression
#'
#' @family Spark ML routines
#'
#' @export
ml_linear_regression <- function(x, response, features, intercept = TRUE,
                  alpha = 0, lambda = 0) {
  spark_ml_linear_regression(x, response, features, intercept, alpha, lambda)
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

