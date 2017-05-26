#' Tidying methods for Spark ML linear models
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_glm_tidiers
NULL

#' @rdname ml_glm_tidiers
#' @param exponentiate For GLM, whether to exponentiate the coefficient estimates (typical for logistic regression.)
#'
#' @importFrom dplyr matches everything mutate
#' @importFrom rlang sym syms quo
#' @export
tidy.ml_model_generalized_linear_regression <- function(x, exponentiate = FALSE,
                                                        ...) {

  ensure_scalar_boolean(exponentiate)

  stat_names <- c("coefficients", "standard.errors", "t.values", "p.values")

  new_names <- c("estimate", "std.error", "statistic", "p.value")

  if (exponentiate) {
    if (! x$link %in% c("log", "logit")) {
      warning(paste("Exponentiating coefficients, but model did not use",
                    "a log or logit link function"))
    }
    trans <- exp
    # drop standard errors because they're not valid after exponentiating
    vars <- dplyr::select_vars(c("term", new_names), -matches("std.error"))
  } else {
    trans <- identity
    vars <- dplyr::select_vars(c("term", new_names), everything())
  }

  extract_estimate_statistics(x, stat_names, new_names) %>%
    mutate(estimate = trans(!!sym("estimate"))) %>%
    select(!!!syms(vars))

}

#' @rdname ml_glm_tidiers
#' @export
tidy.ml_model_linear_regression <- function(x,
                                            ...) {
  stat_names <- c("coefficients", "standard.errors", "t.values", "p.values")
  new_names <- c("estimate", "std.error", "statistic", "p.value")
  vars <- dplyr::select_vars(c("term", new_names), everything())

  extract_estimate_statistics(x, stat_names, new_names) %>%
    select(!!!rlang::syms(vars))
}

#' @rdname ml_glm_tidiers
#' @param newdata a tbl_spark of new data to use for prediction.
#' @param type.residuals type of residuals, defaults to \code{"working"}. Must be set to
#'   \code{"working"} when \code{newdata} is supplied.
#'
#' @details The residuals attached by \code{augment} are of type "working" by default,
#'   which is different from the default of "deviance" for \code{residuals()} or \code{sdf_residuals()}.
#' @importFrom rlang := sym
#' @importFrom dplyr rename mutate
#' @export
augment.ml_model_generalized_linear_regression <-
  function(x, newdata = NULL,
           type.residuals = c("working", "deviance", "pearson", "response"),
           ...) {
    type.residuals <- rlang::arg_match(type.residuals) %>%
      ensure_scalar_character()

    if (!is.null(newdata) && !identical(type.residuals, "working"))
      stop("'type.residuals' must be set to 'working' when 'newdata' is supplied")

    newdata <- newdata %||% (ml_model_data(x) %>%
                               select(- !!! sym(x$model.parameters$id)))

    # We calculate working residuals on training data via SparkSQL directly
    # instead of calling the MLlib API.
    if (type.residuals == "working") {
      predictions <- sdf_predict(x, newdata) %>%
        rename(fitted = !!"prediction")
      return(predictions %>%
               mutate(resid = `-`(!!sym(x$response), !!sym("fitted")))
      )
    }

    # If the code reaches here, user didn't supply 'newdata' so we're dealing with
    # training data. We call 'sdf_residuals()' first and then 'sdf_predict()' in
    # order to guarantee row order presevation.
    residuals <- sdf_residuals(x, type = type.residuals)
    sdf_predict(x, newdata = residuals) %>%
      # Two calls to 'rename': https://github.com/rstudio/sparklyr/issues/678
      rename(fitted = !!"prediction") %>%
      rename(resid = !!"residuals")
  }

#' @rdname ml_glm_tidiers
#' @export
augment.ml_model_linear_regression <- augment.ml_model_generalized_linear_regression

#' @rdname ml_glm_tidiers
#' @export
glance.ml_model_generalized_linear_regression <- function(x, ...) {
  metric_names <- c("null.deviance", "residual.dof.null", "aic", "deviance",
                    "residual.dof")
  new_names <- c("null.deviance", "df.null", "AIC", "deviance", "df.residual")
  extract_model_metrics(x, metric_names, new_names)
}

#' @rdname ml_glm_tidiers
#' @export
glance.ml_model_linear_regression <- function(x, ...) {
  metric_names <- c("explained.variance", "mean.absolute.error", "mean.squared.error",
                    "r.squared", "root.mean.squared.error")
  new_names <- metric_names
  extract_model_metrics(x, metric_names, new_names)
}
