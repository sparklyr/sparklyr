spark_ml_linear_regression <- function(x, response, features, intercept = TRUE,
                                       alpha = 0, lambda = 0)
{
  scon <- spark_scon(x)
  df <- as_spark_dataframe(x)

  lr <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.regression.LinearRegression"
  )

  tdf <- spark_dataframe_assemble_vector(df, features, "features")

  fit <- lr %>%
    spark_invoke("setMaxIter", 10L) %>%
    spark_invoke("setLabelCol", response) %>%
    spark_invoke("setFeaturesCol", "features") %>%
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
    coefficients = coefficients,
    standard.errors = errors,
    t.values = tvalues,
    p.values = try_null(as.numeric(spark_invoke(summary, "pValues"))),
    features = features,
    response = response,
    explained.variance = spark_invoke(summary, "explainedVariance"),
    mean.absolute.error = spark_invoke(summary, "meanAbsoluteError"),
    mean.squared.error = spark_invoke(summary, "meanSquaredError"),
    r.squared = spark_invoke(summary, "r2"),
    root.mean.squared.error = spark_invoke(summary, "rootMeanSquaredError")
  )
}

#' Linear regression from a dplyr source
#'
#' Fit a linear model using Spark LinearRegression
#'
#' See \url{https://spark.apache.org/docs/latest/ml-classification-regression.html}
#' for more information on how linear regression is implemented in Spark.
#'
#' @param x A dplyr source.
#' @param response The prediction column
#' @param features List of columns to use as features
#' @param intercept TRUE to fit the intercept
#' @param alpha The \emph{elastic net} mixing parameter.
#' @param lambda The \emph{regularization penalty}.
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

#' @export
residuals.ml_model_linear_regression <- function(x, ...) {
  x$.model %>%
    spark_invoke("summary") %>%
    spark_invoke("residuals") %>%
    spark_dataframe_read_column("residuals")
}

#' @export
fitted.ml_model_linear_regression <- function(x, ...) {
  x$.model %>%
    spark_invoke("summary") %>%
    spark_invoke("predictions") %>%
    spark_dataframe_read_column("prediction")
}

#' @export
predict.ml_model_linear_regression <- function(object, newdata, ...) {
  sdf <- as_spark_dataframe(newdata)
  assembled <- spark_dataframe_assemble_vector(sdf, features(object), "features")
  predicted <- spark_invoke(object$.model, "transform", assembled)
  spark_dataframe_read_column(predicted, "prediction")
}
