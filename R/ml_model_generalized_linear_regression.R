new_ml_model_generalized_linear_regression <- function(pipeline, pipeline_model, model,
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
    subclass = "ml_model_generalized_linear_regression",
    .features = feature_names
  )
}

#' @export
print.ml_model_generalized_linear_regression <-
  function(x, digits = max(3L, getOption("digits") - 3L), ...)
  {
    ml_model_print_coefficients(x)
    print_newline()

    cat(
      sprintf("Degress of Freedom:  %s Total (i.e. Null);  %s Residual",
              x$summary$residual_degree_of_freedom_null(),
              x$summary$residual_degree_of_freedom()),
      sep = "\n"
    )
    cat(sprintf("Null Deviance:       %s", signif(x$summary$null_deviance(), digits)), sep = "\n")
    cat(sprintf("Residual Deviance:   %s\tAIC: %s",
                signif(x$summary$deviance(), digits),
                signif(x$summary$aic(), digits)), sep = "\n")
  }

#' @export
summary.ml_model_generalized_linear_regression <- function(object,
                                                           digits = max(3L, getOption("digits") - 3L),
                                                           ...) {
  ml_model_print_residuals(object, residuals.header = "Deviance Residuals")
  print_newline()
  ml_model_print_coefficients_detailed(object)
  print_newline()

  printf("(Dispersion paramter for %s family taken to be %s)\n\n",
         ml_param(ml_stage(object$pipeline_model, 2), "family"),
         signif(object$summary$dispersion(), digits + 3))

  printf("   Null  deviance: %s on %s degress of freedom\n",
         signif(object$summary$null_deviance(), digits + 2),
         signif(object$summary$residual_degree_of_freedom_null(), digits))

  printf("Residual deviance: %s on %s degrees of freedom\n",
         signif(object$summary$deviance(), digits + 2),
         signif(object$summary$degrees_of_freedom(), digits))
  printf("AIC: %s\n", signif(object$summary$aic(), digits + 1))

  invisible(object)
}

#' @export
residuals.ml_model_generalized_linear_regression <- function(
  object,
  type = c("deviance", "pearson", "working", "response"),
  ...) {

  type <- rlang::arg_match(type) %>%
    cast_string()

  residuals <- object %>%
    `[[`("summary") %>%
    `[[`("residuals") %>%
    do.call(list(type = type))

  sdf_read_column(residuals, paste0(type, "Residuals"))
}

#' @rdname sdf_residuals
#' @param type type of residuals which should be returned.
#' @export
sdf_residuals.ml_model_generalized_linear_regression <- function(object,
                                                                 type = c("deviance", "pearson", "working", "response"),
                                                                 ...) {
  type <- rlang::arg_match(type) %>%
    cast_string()

  residuals <- object %>%
    `[[`("summary") %>%
    `[[`("residuals") %>%
    do.call(list(type = type)) %>%
    dplyr::rename(residuals = !!rlang::sym(paste0(type, "Residuals")))

  ml_model_data(object) %>%
    sdf_fast_bind_cols(residuals)
}
