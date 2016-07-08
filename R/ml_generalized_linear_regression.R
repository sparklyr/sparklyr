#' Spark ML -- Generalized Linear Regression
#'
#' Perform generalized linear regression on a Spark DataFrame. Only available
#' with Spark 2.0.0 and above.
#'
#' In contrast to \code{\link{ml_linear_regression}()} and 
#' \code{\link{ml_logistic_regression}()}, these routines do not allow you to
#' tweak the loss function (e.g. for elastic net regression); however, the model
#' fits returned by this routine are generally richer in regards to information
#' provided for assessing the quality of fit.
#' 
#' @param x An object convertable to a Spark DataFrame (typically, a \code{tbl_spark}).
#' @param response The name of the response vector.
#' @param features The names of features (terms) to use as linear predictors
#'   for the response.
#' @param intercept Fit the model with an intercept term?
#' @param family The family / link function to use; analogous to those normally
#'   passed in to calls to \R's own \code{glm}.
#' @param max.iter Maximum number of iterations used in model fitting process.
#' @param ... Optional arguments; currently unused.
#' 
#' @family Spark ML routines
#'
#' @export
ml_generalized_linear_regression <-
  function(x,
           response,
           features,
           intercept = TRUE,
           family = gaussian(link = "identity"),
           max.iter = 100L,
           ...)
{
  spark_require_version(sc, "2.0.0")
  
  df <- spark_dataframe(x)
  sc <- spark_connection(df)
  
  prepare_response_features_intercept(df, response, features, intercept)
  
  max.iter <- ensure_scalar_integer(max.iter)
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)
  
  # parse 'family' argument in similar way to R's glm
  if (is.character(family)) 
    family <- get(family, mode = "function", envir = parent.frame())
  
  if (is.function(family)) 
    family <- family()
  
  if (is.null(family$family)) {
    print(family)
    stop("'family' not recognized")
  }
  
  # pull out string names for family, link functions
  link <- ensure_scalar_character(family$link)
  family <- ensure_scalar_character(family$family)
  
  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)
  
  glr <- invoke_new(
    sc,
    "org.apache.spark.ml.regression.GeneralizedLinearRegression"
  )
  
  model <- glr %>%
    invoke("setMaxIter", max.iter) %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setFitIntercept", intercept) %>%
    invoke("setFamily", family) %>%
    invoke("setLink", link)
  
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
  
  aic <- invoke(summary, "aic")
  dof <- invoke(summary, "degreesOfFreedom")
  deviance <- invoke(summary, "deviance")
  dispersion <- invoke(summary, "dispersion")
  null.deviance <- invoke(summary, "nullDeviance")
  rank <- invoke(summary, "rank")
  residual.dof <- invoke(summary, "residualDegreeOfFreedom")
  residual.dof.null <- invoke(summary, "residualDegreeOfFreedomNull")
  
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
  
  ml_model("generalized_linear_regression", fit,
    features = features,
    response = response,
    family = family,
    link = link,
    coefficients = coefficients,
    standard.errors = errors,
    t.values = tvalues,
    p.values = pvalues,
    rank = rank,
    deviance = deviance,
    null.deviance = null.deviance,
    dof = dof,
    dispersion = dispersion,
    residual.dof = residual.dof,
    residual.dof.null = residual.dof.null,
    aic = aic,
    model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_generalized_linear_regression <-
  function(x, digits = max(3L, getOption("digits") - 3L), ...)
{
  ml_model_print_call(x)
  print_newline()
  ml_model_print_coefficients(x)
  print_newline()
  
  cat(
    sprintf("Degress of Freedom:  %s Total (i.e. Null);  %s Residual",
            x$residual.dof.null,
            x$residual.dof),
    sep = "\n"
  )
  cat(sprintf("Null Deviance:       %s", signif(x$null.deviance, digits)), sep = "\n")
  cat(sprintf("Residual Deviance:   %s\tAIC: %s",
              signif(x$deviance, digits),
              signif(x$aic, digits)), sep = "\n")
}

#' @export
summary.ml_model_generalized_linear_regression <-
  function(object, digits = max(3L, getOption("digits") - 3L), ...)
{
  columns <- c("coefficients", "standard.errors", "t.values", "p.values")
  values <- as.list(object[columns])
  matrix <- do.call(base::cbind, values)
  colnames(matrix) <- c("Estimate", "Std. Error", "t value", "Pr(>|t|)")
  
  ml_model_print_call(object)
  print_newline()
  ml_model_print_residuals(object, residuals.header = "Deviance Residuals")
  print_newline()
  
  cat("Coefficients:", sep = "\n")
  stats::printCoefmat(matrix)
  print_newline()
  
  printf("(Dispersion paramter for %s family taken to be %s)\n\n",
         object$family,
         signif(object$dispersion, digits + 3))
  
  printf("   Null  deviance: %s on %s degress of freedom\n",
         signif(object$null.deviance, digits + 2),
         signif(object$residual.dof.null, digits))
  
  printf("Residual deviance: %s on %s degrees of freedom\n",
         signif(object$deviance, digits + 2),
         signif(object$dof, digits))
  printf("AIC: %s\n", signif(object$aic, digits + 1))
  
  invisible(object)
}
