new_ml_model_linear_regression <- function(pipeline, pipeline_model, model,
                                           dataset, formula, feature_names, call) {
  jobj <- spark_jobj(model)
  sc <- spark_connection(model)

  coefficients <- model$coefficients
  names(coefficients) <- feature_names

  coefficients <- if (ml_param(model, "fit_intercept"))
    rlang::set_names(
      c(invoke(jobj, "intercept"), model$coefficients),
      c("(Intercept)", feature_names))

  summary <- model$summary

  new_ml_model_regression(
    pipeline, pipeline_model, model, dataset, formula,
    coefficients = coefficients,
    summary = summary,
    subclass = "ml_model_linear_regression",
    .features = feature_names
  )
}

# Generic implementations

#' @export
print.ml_model_linear_regression <- function(x, ...) {
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}

#' @export
summary.ml_model_linear_regression <- function(object, ...) {
  ml_model_print_residuals(object, residuals.header = "Deviance Residuals")
  print_newline()
  ml_model_print_coefficients_detailed(object)
  print_newline()

  cat(paste("R-Squared:", signif(object$summary$r2, 4)), sep = "\n")
  cat(paste("Root Mean Squared Error:",
            signif(object$summary$root_mean_squared_error, 4)), sep = "\n")
}

#' @export
residuals.ml_model_linear_regression <- function(object, ...) {
  residuals <- object$summary$residuals
  sdf_read_column(residuals, "residuals")
}

#' @export
#' @rdname sdf_residuals
sdf_residuals.ml_model_linear_regression <- function(object, ...) {
  residuals <- object$summary$residuals

  ml_model_data(object) %>%
    sdf_fast_bind_cols(residuals)
}
