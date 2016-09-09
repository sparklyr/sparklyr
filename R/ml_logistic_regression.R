#' Spark ML -- Logistic Regression
#'
#' Perform logistic regression on a Spark DataFrame.
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
ml_logistic_regression <- function(x,
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

  envir$model <- "org.apache.spark.ml.classification.LogisticRegression"
  lr <- invoke_new(sc, envir$model)

  model <- lr %>%
    invoke("setMaxIter", iter.max) %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setFitIntercept", as.logical(intercept)) %>%
    invoke("setElasticNetParam", as.double(alpha)) %>%
    invoke("setRegParam", as.double(lambda))

  if (only.model) return(model)

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
           data = df,
           ml.options = ml.options,
           categorical.transformations = categorical.transformations,
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

