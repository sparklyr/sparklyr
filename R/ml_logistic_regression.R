#' Spark ML -- Logistic Regression
#'
#' Perform logistic regression on a \code{spark_tbl}.
#'
#' @template roxlate-ml-regression
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

  lr <- sparkapi_invoke_new(
    sc,
    "org.apache.spark.ml.classification.LogisticRegression"
  )

  fit <- lr %>%
    sparkapi_invoke("setMaxIter", 100L) %>%
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

  hasIntercept <- sparkapi_invoke(fit, "getFitIntercept")
  if (hasIntercept) {
    intercept <- sparkapi_invoke(fit, "intercept")
    coefficients <- c(coefficients, intercept)
    names(coefficients) <- c(features, "(Intercept)")
  }

  summary <- sparkapi_invoke(fit, "summary")
  areaUnderROC <- sparkapi_invoke(summary, "areaUnderROC")
  roc <- spark_dataframe_collect(sparkapi_invoke(summary, "roc"))
  
  coefficients <- intercept_first(coefficients)

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
summary.ml_model_logistic_regression <- function(object, ...) {
  ml_model_print_call(object)
  print_newline()
  # ml_model_print_residuals(object)
  # print_newline()
  ml_model_print_coefficients(object)
  print_newline()
}


#' @export
residuals.ml_model_logistic_regression <- function(object, ...) {
  stop("residuals not yet available for Spark logistic regression")
}

