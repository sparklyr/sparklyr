new_ml_model_linear_svc <- function(pipeline, pipeline_model, model,
                                    dataset, formula, feature_names,
                                    index_labels, call) {
  jobj <- spark_jobj(model)
  sc <- spark_connection(model)

  coefficients <- model$coefficients
  names(coefficients) <- feature_names

  coefficients <- if (ml_param(model, "fit_intercept"))
    rlang::set_names(
      c(invoke(jobj, "intercept"), model$coefficients),
      c("(Intercept)", feature_names))

  new_ml_model_classification(
    pipeline, pipeline_model, model, dataset, formula,
    coefficients = coefficients,
    subclass = "ml_model_linear_svc",
    .features = feature_names,
    .index_labels = index_labels
  )
}

#' @export
print.ml_model_linear_svc <- function(x, ...) {
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}
