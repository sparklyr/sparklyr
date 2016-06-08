spark_ml_linear_regression <- function(x, response, features, intercept = TRUE) {
  scon <- spark_scon(x)
  df <- as_spark_dataframe(x)

  lr <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.regression.LinearRegression"
  )

  tdf <- spark_assemble_vector(scon, df, features, "features")

  fit <- lr %>%
    spark_invoke("setMaxIter", 10L) %>%
    spark_invoke("setLabelCol", response) %>%
    spark_invoke("setFeaturesCol", "features") %>%
    spark_invoke("setFitIntercept", as.logical(intercept)) %>%
    spark_invoke("fit", tdf)

  fit
}

as_lm_result <- function(model, features, response) {

  coefficients <- model %>%
    spark_invoke("coefficients") %>%
    spark_invoke("toArray")
  names(coefficients) <- features

  has_intercept <- spark_invoke(model, "getFitIntercept")
  if (has_intercept) {
    intercept <- spark_invoke(model, "intercept")
    coefficients <- c(coefficients, intercept)
    names(coefficients) <- c(features, "(Intercept)")
  }

  summary <- spark_invoke(model, "summary")

  errors <- spark_invoke(summary, "coefficientStandardErrors")
  names(errors) <- names(coefficients)

  tvalues <- spark_invoke(summary, "tValues")
  names(tvalues) <- names(coefficients)

  ml_model("lm", model,
    coefficients = coefficients,
    standard.errors = errors,
    t.values = tvalues,
    p.values = as.numeric(spark_invoke(summary, "pValues")),
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
#' @export
#' @param x A dplyr source.
#' @param response The prediction column
#' @param features List of columns to use as features
#' @param intercept TRUE to fit the intercept
ml_lm <- function(x, response, features, intercept = TRUE) {
  fit <- spark_ml_linear_regression(x, response, features, intercept)
  as_lm_result(fit, features, response)
}


#' @export
print.ml_model_lm <- function(x, ...) {

  # report what model was fitted
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")

  # report coefficients
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}

#' @export
residuals.ml_model_lm <- function(x, ...) {
  x$.model %>%
    spark_invoke("summary") %>%
    spark_invoke("residuals") %>%
    spark_dataframe_read_column("residuals", "DoubleType")
}

#' @export
fitted.ml_model_lm <- function(x, ...) {
  x$.model %>%
    spark_invoke("summary") %>%
    spark_invoke("predictions") %>%
    spark_dataframe_read_column("prediction", "DoubleType")
}
