new_ml_model_logistic_regression <- function(pipeline_model, formula, dataset, label_col,
                                             features_col, predicted_label_col) {
  m <- new_ml_model_classification(
    pipeline_model, formula, dataset = dataset,
    label_col = label_col, features_col = features_col,
    predicted_label_col = predicted_label_col,
    class = "ml_model_logistic_regression"
  )

  model <- m$model
  jobj <- spark_jobj(model)
  sc <- spark_connection(model)

  # multinomial vs. binomial models have separate APIs for
  # retrieving results
  is_multinomial <- invoke(jobj, "numClasses") > 2

  # extract coefficients (can be either a vector or matrix, depending
  # on binomial vs. multinomial)
  m$coefficients <- if (is_multinomial) {
    spark_require_version(sc, "2.1.0", "Multinomial regression")

    # multinomial
    coefficients <- model$coefficient_matrix
    colnames(coefficients) <- m$feature_names
    rownames(coefficients) <- m$index_labels

    if (ml_param(model, "fit_intercept")) {
      intercept <- model$intercept_vector
      coefficients <- cbind(intercept, coefficients)
      colnames(coefficients) <- c("(Intercept)", m$feature_names)
    }
    coefficients
  } else {
    # binomial

    if (ml_param(model, "fit_intercept")) {
      rlang::set_names(
        c(invoke(jobj, "intercept"), model$coefficients),
        c("(Intercept)", m$feature_names)
      )
    } else {
      rlang::set_names(model$coefficients, m$feature_names)
    }
  }

  m$summary <- model$summary

  m
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
