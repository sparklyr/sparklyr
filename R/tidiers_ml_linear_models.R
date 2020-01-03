#' Tidying methods for Spark ML linear models
#'
#' These methods summarize the results of Spark ML models into tidy forms.
#'
#' @param x a Spark ML model.
#' @param ... extra arguments (not used.)
#' @name ml_glm_tidiers
NULL

get_stats <- function(stats, model) {
  stats %>%
    purrr::map(~ ml_summary(model, .x, allow_null = TRUE)) %>%
    purrr::map_if(is.function, ~ .x())
}

#' @rdname ml_glm_tidiers
#' @param exponentiate For GLM, whether to exponentiate the coefficient estimates (typical for logistic regression.)
#'
#' @importFrom dplyr matches everything mutate
#' @importFrom rlang sym syms quo
#' @export
tidy.ml_model_generalized_linear_regression <- function(x, exponentiate = FALSE,
                                                        ...) {
  model <- x$model
  exponentiate <- cast_scalar_logical(exponentiate)

  stats <- c("coefficient_standard_errors", "t_values", "p_values")
  new_names <- c("estimate", "std.error", "statistic", "p.value")

  if (exponentiate) {
    exp_warning <- paste("Exponentiating coefficients, but model did not use",
                         "a log or logit link function")

    if (rlang::is_null(ml_param(model, "link", allow_null = TRUE))) {
      if (!ml_param(model, "family") %in% c("binomial", "poisson"))
        warning(exp_warning)
    } else {
      if (!ml_param(model, "link") %in% c("log", "logit"))
        warning(exp_warning)
    }
    trans <- exp
    # drop standard errors because they're not valid after exponentiating
    vars <- dplyr::select_vars(c("term", new_names), -matches("std.error"))
  } else {
    trans <- identity
    vars <- dplyr::select_vars(c("term", new_names), everything())
  }


  coefficients <- list(x$coefficients)
  statistics <- stats %>%
    get_stats(model)
  c(coefficients, statistics) %>%
    as.data.frame() %>%
    fix_data_frame(newnames = new_names) %>%
    dplyr::mutate(estimate = trans(!!sym("estimate"))) %>%
    dplyr::select(!!!syms(vars))

}

#' @rdname ml_glm_tidiers
#' @export
tidy.ml_model_linear_regression <- function(x, ...) {
  model <- x$model
  stats <- c("coefficient_standard_errors", "t_values", "p_values")
  new_names <- c("estimate", "std.error", "statistic", "p.value")
  vars <- dplyr::select_vars(c("term", new_names), everything())

  coefficients <- list(x$coefficients)
  statistics <- stats %>%
    get_stats(model)
  c(coefficients, statistics) %>%
    as.data.frame() %>%
    fix_data_frame(newnames = new_names) %>%
    select(!!!syms(vars))
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
augment.ml_model_generalized_linear_regression <- function(x, newdata = NULL,
                                                           type.residuals = c("working", "deviance", "pearson", "response"),
                                                           ...) {
  type.residuals <- rlang::arg_match(type.residuals) %>%
    cast_string()

  if (!is.null(newdata) && !(type.residuals == "working"))
    stop("'type.residuals' must be set to 'working' when 'newdata' is supplied")

  newdata <- newdata %||% ml_model_data(x)

  # We calculate working residuals on training data via SparkSQL directly
  # instead of calling the MLlib API.
  if (type.residuals == "working") {
    predictions <- ml_predict(x, newdata) %>%
      rename(fitted = !!rlang::sym("prediction"))
    return(predictions %>%
             mutate(resid = `-`(!!sym(x$response), !!sym("fitted")))
    )
  }

  # If the code reaches here, user didn't supply 'newdata' so we're dealing with
  # training data. We call 'sdf_residuals()' first and then 'ml_predict()' in
  # order to guarantee row order presevation.
  residuals <- sdf_residuals(x, type = type.residuals)
  ml_predict(x, newdata = residuals) %>%
    # Two calls to 'rename': https://github.com/sparklyr/sparklyr/issues/678
    rename(fitted = !!"prediction") %>%
    rename(resid = !!"residuals")
}

#' @rdname ml_glm_tidiers
#' @export
augment.ml_model_linear_regression <- augment.ml_model_generalized_linear_regression

#' @rdname ml_glm_tidiers
#' @export
glance.ml_model_generalized_linear_regression <- function(x, ...) {
  metric_names <- c("null_deviance", "residual_degree_of_freedom_null", "aic", "deviance",
                    "residual_degree_of_freedom")
  new_names <- c("null.deviance", "df.null", "AIC", "deviance", "df.residual")
  extract_model_metrics(x, metric_names, new_names)
}

#' @rdname ml_glm_tidiers
#' @export
glance.ml_model_linear_regression <- function(x, ...) {
  metric_names <- c("explained_variance", "mean_absolute_error",
                    "mean_squared_error",
                    "r2", "root_mean_squared_error")
  new_names <- c("explained.variance", "mean.absolute.error",
                 "mean.squared.error", "r.squared", "root.mean.squared.error")
  extract_model_metrics(x, metric_names, new_names)
}
