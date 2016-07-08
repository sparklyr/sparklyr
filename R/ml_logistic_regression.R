#' Spark ML -- Logistic Regression
#'
#' Perform logistic regression on a \code{spark_tbl}.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @template roxlate-ml-intercept
#' @template roxlate-ml-regression-penalty
#' @template roxlate-ml-max-iter
#' @template roxlate-ml-dots
#'
#' @family Spark ML routines
#'
#' @export
ml_logistic_regression <- function(x,
                                   response,
                                   features,
                                   intercept = TRUE,
                                   alpha = 0,
                                   lambda = 0,
                                   max.iter = 100L,
                                   ...)
{
  df <- spark_dataframe(x)
  sc <- spark_connection(df)
  
  prepare_response_features_intercept(df, response, features, intercept)
  
  alpha <- ensure_scalar_double(alpha)
  lambda <- ensure_scalar_double(lambda)
  max.iter <- ensure_scalar_integer(max.iter)
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)

  lr <- invoke_new(
    sc,
    "org.apache.spark.ml.classification.LogisticRegression"
  )

  model <- lr %>%
    invoke("setMaxIter", max.iter) %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setFitIntercept", as.logical(intercept)) %>%
    invoke("setElasticNetParam", as.double(alpha)) %>%
    invoke("setRegParam", as.double(lambda))
  
  if (only_model) return(model)  
  
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
  areaUnderROC <- invoke(summary, "areaUnderROC")
  roc <- sdf_collect(invoke(summary, "roc"))
  
  coefficients <- intercept_first(coefficients)

  ml_model("logistic_regression", fit,
           features = features,
           response = response,
           intercept = intercept,
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

