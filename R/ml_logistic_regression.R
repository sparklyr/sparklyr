#' Spark ML -- Logistic Regression
#'
#' Perform logistic regression on a \code{spark_tbl}.
#'
#' @template ml-regression
#'
#' @family Spark ML routines
#'
#' @export
ml_logistic_regression <- function(x,
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

  lr <- spark_invoke_new(
    sc,
    "org.apache.spark.ml.classification.LogisticRegression"
  )

  fit <- lr %>%
    spark_invoke("setMaxIter", 100L) %>%
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
  areaUnderROC <- spark_invoke(summary, "areaUnderROC")
  roc <- spark_dataframe_collect(spark_invoke(summary, "roc"))

  ml_model("logistic_regression", fit,
           features = features,
           response = response,
           coefficients = coefficients,
           roc = roc,
           area.under.roc = areaUnderROC,
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_logistic_regression <- function(x, ...) {

  # report what model was fitted
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")

  # report coefficients
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}

#' @export
residuals.ml_model_logistic_regression <- function(object, ...) {
  stop("residuals not yet available for Spark logistic regression")
}

