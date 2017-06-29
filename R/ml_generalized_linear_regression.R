#' Spark ML -- Generalized Linear Regression
#'
#' Perform generalized linear regression on a Spark DataFrame.
#'
#' In contrast to \code{\link{ml_linear_regression}()} and
#' \code{\link{ml_logistic_regression}()}, these routines do not allow you to
#' tweak the loss function (e.g. for elastic net regression); however, the model
#' fits returned by this routine are generally richer in regards to information
#' provided for assessing the quality of fit.
#'
#' @template roxlate-ml-x
#' @template roxlate-ml-response
#' @template roxlate-ml-features
#' @template roxlate-ml-intercept
#' @param family The family / link function to use; analogous to those normally
#'   passed in to calls to \R's own \code{\link{glm}}.
#' @template roxlate-ml-weights-column
#' @template roxlate-ml-iter-max
#' @template roxlate-ml-options
#' @template roxlate-ml-dots
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
           weights.column = NULL,
           iter.max = 100L,
           ml.options = ml_options(),
           ...)
{
  ml_backwards_compatibility_api()

  df <- spark_dataframe(x)
  sc <- spark_connection(df)

  spark_require_version(sc, "2.0.0")

  categorical.transformations <- new.env(parent = emptyenv())
  df <- ml_prepare_response_features_intercept(
    x = df,
    response = response,
    features = features,
    intercept = intercept,
    envir = environment(),
    categorical.transformations = categorical.transformations,
    ml.options = ml.options
  )

  iter.max <- ensure_scalar_integer(iter.max)
  weights.column <- ensure_scalar_character(weights.column, allow.null = TRUE)
  only.model <- ensure_scalar_boolean(ml.options$only.model)

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

  envir$id <- ml.options$id.column
  df <- df %>%
    sdf_with_unique_id(envir$id) %>%
    spark_dataframe()

  tdf <- ml_prepare_dataframe(df, features, response, ml.options = ml.options, envir = envir)

  envir$model <- "org.apache.spark.ml.regression.GeneralizedLinearRegression"
  glr <- invoke_new(sc, envir$model)

  model <- glr %>%
    invoke("setMaxIter", iter.max) %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setFitIntercept", intercept) %>%
    invoke("setFamily", family) %>%
    invoke("setLink", link)

  if (!is.null(weights.column)) {
    model <- model %>%
      invoke("setWeightCol", weights.column)
  }


  if (is.function(ml.options$model.transform))
    model <- ml.options$model.transform(model)

  if (only.model)
    return(model)

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
    intercept = intercept,
    family = family,
    weights.column = weights.column,
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
    data = df,
    ml.options = ml.options,
    categorical.transformations = categorical.transformations,
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
  ml_model_print_call(object)
  print_newline()
  ml_model_print_residuals(object, residuals.header = "Deviance Residuals")
  print_newline()
  ml_model_print_coefficients_detailed(object)
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

#' @export
residuals.ml_model_generalized_linear_regression <- function(
  object,
  type = c("deviance", "pearson", "working", "response"),
  ...) {

  type <- rlang::arg_match(type)
  ensure_scalar_character(type)

  residuals <- object$.model %>%
    invoke("summary") %>%
    invoke("residuals", type)

  sdf_read_column(residuals, paste0(type, "Residuals"))
}

#' @rdname sdf_residuals
#' @param type type of residuals which should be returned.
#' @export
sdf_residuals.ml_model_generalized_linear_regression <- function(
  object,
  type = c("deviance", "pearson", "working", "response"),
  ...) {

  type <- rlang::arg_match(type)
  ensure_scalar_character(type)

  residuals <- object$.model %>%
    invoke("summary") %>%
    invoke("residuals", type) %>%
    sdf_register() %>%
    dplyr::rename(residuals = !!! rlang::sym(paste0(type, "Residuals")))

  ml_model_data(object) %>%
    select(- !!! rlang::sym(object$model.parameters$id)) %>%
    sdf_fast_bind_cols(residuals)
}
