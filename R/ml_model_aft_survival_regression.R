new_ml_model_aft_survival_regression <- function(pipeline_model, formula, dataset, label_col,
                                                 features_col) {
  m <- new_ml_model_regression(
    pipeline_model, formula = formula, dataset = dataset,
    label_col = label_col, features_col = features_col,
    class = "ml_model_aft_survival_regression"
  )

  coefficients <- m$model$coefficients
  names(coefficients) <- m$feature_names

  coefficients <- if (ml_param(m$model, "fit_intercept"))
    rlang::set_names(
      c(invoke(jobj, "intercept"), m$model$coefficients),
      c("(Intercept)", m$feature_names))

  m$coefficients <- coefficients

  m
}

#' @export
print.ml_model_aft_survival_regression <- function(x, ...) {
  cat("Formula: ", x$formula, "\n\n", sep = "")
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}
