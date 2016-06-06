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

  as_lm_result(fit, features)
}

as_lm_result <- function(model, features) {

  coefficients <- model %>%
    spark_invoke("coefficients") %>%
    spark_invoke("toArray") %>%
    unlist(recursive = FALSE)
  names(coefficients) <- features

  has_intercept <- spark_invoke(model, "getFitIntercept")
  if (has_intercept) {
    intercept <- spark_invoke(model, "intercept")
    coefficients <- c(coefficients, intercept)
    names(coefficients) <- c(features, "(Intercept)")
  }

  summary <- spark_invoke(model, "summary")

  residuals <- spark_invoke(summary, "residuals") %>%
    spark_invoke("collect") %>%
    unlist(recursive = TRUE) %>%
    unname()

  predictions <- spark_invoke(summary, "predictions") %>%
    spark_invoke("select", "prediction", list()) %>%
    spark_invoke("collect") %>%
    unlist(recursive = TRUE) %>%
    unname()

  errors <- spark_invoke(summary, "coefficientStandardErrors") %>%
    unlist(recursive = FALSE)
  names(errors) <- names(coefficients)

  tvalues <- spark_invoke(summary, "tValues") %>%
    unlist(recursive = FALSE)
  names(tvalues) <- names(coefficients)

  list(
    coefficients = coefficients,
    residuals = residuals,
    rank = length(coefficients),
    fitted.values = predictions,
    standard.errors = errors,
    t.values = tvalues,
    p.values = as.numeric(spark_invoke(summary, "pValues")),
    explained.variance = spark_invoke(summary, "explainedVariance"),
    mean.absolute.error = spark_invoke(summary, "meanAbsoluteError"),
    mean.squared.error = spark_invoke(summary, "meanSquaredError"),
    r.squared = spark_invoke(summary, "r2"),
    root.mean.squared.error = spark_invoke(summary, "rootMeanSquaredError")
  )

}
