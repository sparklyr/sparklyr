new_ml_model_linear_regression <- function(pipeline_model, formula, dataset, label_col,
                                           features_col) {
  m <- new_ml_model_regression(
    pipeline_model, formula,
    dataset = dataset,
    label_col = label_col, features_col = features_col,
    class = "ml_model_linear_regression"
  )

  model <- m$model
  jobj <- spark_jobj(model)

  coefficients <- model$coefficients
  names(coefficients) <- m$feature_names

  m$coefficients <- if (ml_param(model, "fit_intercept")) {
    rlang::set_names(
      c(invoke(jobj, "intercept"), model$coefficients),
      c("(Intercept)", m$feature_names)
    )
  } else {
    coefficients
  }

  m$summary <- model$summary

  m
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
  cat(paste(
    "Root Mean Squared Error:",
    signif(object$summary$root_mean_squared_error, 4)
  ), sep = "\n")
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
