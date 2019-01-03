new_ml_model_linear_svc <- function(pipeline_model, formula, dataset, label_col,
                                    features_col, predicted_label_col) {
  m <- new_ml_model_classification(
    pipeline_model, formula, dataset = dataset,
    label_col = label_col, features_col = features_col,
    predicted_label_col = predicted_label_col,
    class = "ml_model_linear_svc"
  )

  model <- m$model
  jobj <- spark_jobj(model)

  coefficients <- model$coefficients
  names(coefficients) <- m$feature_names

  m$coefficients <- if (ml_param(model, "fit_intercept"))
    rlang::set_names(
      c(invoke(jobj, "intercept"), model$coefficients),
      c("(Intercept)", m$feature_names)
    )

  m
}

#' @export
print.ml_model_linear_svc <- function(x, ...) {
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}
