new_ml_model_logistic_regression <- function(
  pipeline, pipeline_model, model, dataset, formula, feature_names, index_labels,
  call) {

  jobj <- spark_jobj(model)
  sc <- spark_connection(model)

  # multinomial vs. binomial models have separate APIs for
  # retrieving results
  is_multinomial <- invoke(jobj, "numClasses") > 2

  # extract coefficients (can be either a vector or matrix, depending
  # on binomial vs. multinomial)
  coefficients <- if (is_multinomial) {
    spark_require_version(sc, "2.1.0", "Multinomial regression")

    # multinomial
    coefficients <- model$coefficient_matrix
    colnames(coefficients) <- feature_names
    rownames(coefficients) <- index_labels

    if (ml_param(model, "fit_intercept")) {
      intercept <- model$intercept_vector
      coefficients <- cbind(intercept, coefficients)
      colnames(coefficients) <- c("(Intercept)", feature_names)
    }
    coefficients
  } else {
    # binomial

    coefficients <- if (ml_param(model, "fit_intercept"))
      rlang::set_names(
        c(invoke(jobj, "intercept"), model$coefficients),
        c("(Intercept)", feature_names)
      )
    else
      rlang::set_names(model$coefficients, feature_names)
    coefficients
  }

  summary <- model$summary

  new_ml_model_classification(
    pipeline, pipeline_model,
    model, dataset, formula,
    coefficients = coefficients,
    summary = summary,
    subclass = "ml_model_logistic_regression",
    .features = feature_names,
    .index_labels = index_labels
  )
}


# Generic implementations

#' @export
print.ml_model_logistic_regression <- function(x, ...) {
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}

#' @export
summary.ml_model_logistic_regression <- function(object, ...) {
  ml_model_print_coefficients(object)
  print_newline()
}
