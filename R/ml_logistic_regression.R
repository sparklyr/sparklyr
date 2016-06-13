spark_ml_logistic_regression <- function(x, response, features, intercept = TRUE,
                                         alpha = 0, lambda = 0)
{
  scon <- spark_scon(x)
  df <- as_spark_dataframe(x)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)

  lr <- spark_invoke_static_ctor(
    scon,
    "org.apache.spark.ml.classification.LogisticRegression"
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

#' Logistic regression from a dplyr source
#'
#' Fit a logistic model using \code{spark.lm}.
#'
#' See \url{https://spark.apache.org/docs/latest/ml-classification-regression.html}
#' for more information on how regression is implemented in Spark.
#'
#' @param x A dplyr source.
#' @param response The prediction column
#' @param features List of columns to use as features
#' @param intercept TRUE to fit the intercept
#' @param alpha The \emph{elastic net} mixing parameter.
#' @param lambda The \emph{regularization penalty}.
#'
#' @export
ml_logistic_regression <- function(x, response, features, intercept = TRUE,
                                   alpha = 0, lambda = 0) {
  spark_ml_logistic_regression(x, response, features, intercept, alpha, lambda)
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
residuals.ml_model_logistic_regression <- function(x, ...) {
  stop("residuals not yet available for Spark logistic regression")
}

